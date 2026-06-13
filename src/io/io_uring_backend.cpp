/// @file   io/io_uring_backend.cpp
/// @brief  Implementación del backend io_uring sobre el uapi del kernel (sin liburing).
/// @ingroup io

#include "io/io_uring_backend.hpp"

#include <linux/io_uring.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <system_error>
#include <unordered_map>
#include <utility>

namespace nexus {

namespace {

// `mmap` señala el fallo con `MAP_FAILED` (`(void*)-1`); se normaliza a `nullptr` aquí —en el único
// punto donde aparece la conversión entero→puntero del SO— para trabajar con punteros nulos.
void* normalize_map(void* result) {
    return result == MAP_FAILED ? nullptr : result;  // NOLINT(performance-no-int-to-ptr)
}

// --- Envoltura fina de los syscalls de io_uring (glibc no los expone). ---
long io_uring_setup(unsigned entries, io_uring_params* params) {
    return syscall(__NR_io_uring_setup, entries, params);
}
long io_uring_enter(int ring_fd, unsigned to_submit, unsigned min_complete, unsigned flags) {
    return syscall(__NR_io_uring_enter, ring_fd, to_submit, min_complete, flags, nullptr, 0);
}

/// Devuelve `base + offset` reinterpretado como `T*` (los anillos se publican por desplazamientos).
template <class T>
T* at_offset(void* base, std::uint32_t offset) {
    auto* bytes = static_cast<std::byte*>(base) + offset;
    return reinterpret_cast<T*>(bytes);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

std::uint64_t address_of(const void* pointer) {
    return reinterpret_cast<std::uintptr_t>(pointer);  // NOLINT(*-reinterpret-cast)
}

// Barreras sobre los índices compartidos con el kernel (load-acquire / store-release). Usamos los
// builtins atómicos del compilador, no `std::atomic_ref` (ausente en la libc++ 18 del CI); es lo
// idiomático para memoria compartida con el kernel (estilo `smp_load_acquire` de liburing).
std::uint32_t load_acquire(const std::uint32_t* slot) {
    return __atomic_load_n(slot, __ATOMIC_ACQUIRE);
}
// NOLINTNEXTLINE(readability-non-const-parameter): el builtin escribe en `slot` (tidy no lo ve).
void store_release(std::uint32_t* slot, std::uint32_t value) {
    __atomic_store_n(slot, value, __ATOMIC_RELEASE);
}

}  // namespace

/// Anillo io_uring + tabla de ops en vuelo. Toda la gestión cruda confinada aquí (RAII).
struct IoUringBackend::Ring {
    /// Estado de una operación en vuelo: su completion y, para temporizadores, el `timespec` (que
    /// el kernel lee de forma asíncrona y debe vivir hasta que la operación termine).
    struct OpState {
        Proactor::Completion on_done;
        bool is_timer = false;
        __kernel_timespec timespec{};
    };

    int fd = -1;
    void* ring_map = nullptr;  // SQ y CQ comparten un único mmap (se exige SINGLE_MMAP)
    std::size_t ring_size = 0;
    void* sqes_map = nullptr;
    std::size_t sqes_size = 0;

    std::uint32_t* sq_head = nullptr;
    std::uint32_t* sq_tail = nullptr;
    std::uint32_t* sq_ring_mask = nullptr;
    std::uint32_t* sq_array = nullptr;
    std::uint32_t* cq_head = nullptr;
    std::uint32_t* cq_tail = nullptr;
    std::uint32_t* cq_ring_mask = nullptr;
    io_uring_cqe* cqes = nullptr;
    io_uring_sqe* sqe_array = nullptr;
    unsigned entries = 0;

    std::unordered_map<std::uint64_t, OpState> inflight;
    std::uint64_t next_id = 0;

    explicit Ring(unsigned requested);
    ~Ring() { release(); }
    Ring(const Ring&) = delete;
    Ring& operator=(const Ring&) = delete;
    Ring(Ring&&) = delete;
    Ring& operator=(Ring&&) = delete;

    /// Libera mmap y fd, dejándolos nulos (idempotente; sirve también para limpiar tras un fallo
    /// parcial en el constructor, donde el destructor no llegaría a ejecutarse).
    void release() noexcept {
        if (sqes_map != nullptr) {
            ::munmap(sqes_map, sqes_size);
            sqes_map = nullptr;
        }
        if (ring_map != nullptr) {
            ::munmap(ring_map, ring_size);
            ring_map = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    /// Reserva la siguiente SQE libre (ya puesta a cero), o `nullptr` si el anillo de envío llena.
    io_uring_sqe* acquire_sqe(std::uint32_t* out_index) const {
        const std::uint32_t tail = *sq_tail;  // único productor: lectura simple
        const std::uint32_t head = load_acquire(sq_head);
        if (tail - head >= entries) {
            return nullptr;
        }
        const std::uint32_t index = tail & *sq_ring_mask;
        *out_index = index;
        io_uring_sqe* sqe = &sqe_array[index];
        *sqe = io_uring_sqe{};
        return sqe;
    }

    /// Avanza la cola del anillo de envío (con el dato ya escrito) y entrega la SQE al kernel.
    void submit(std::uint32_t index) const {
        sq_array[index] = index;
        store_release(sq_tail, *sq_tail + 1);
        io_uring_enter(fd, 1, 0, 0);
    }

    /// Prepara y envía una operación genérica (read/write/recv/send/accept/fsync).
    void submit_op(int opcode, int fd_arg, std::uint64_t addr, std::uint32_t len,
                   std::uint64_t offset, std::uint32_t op_flags, Proactor::Completion on_done) {
        std::uint32_t index = 0;
        io_uring_sqe* sqe = acquire_sqe(&index);
        if (sqe == nullptr) {
            on_done(-EAGAIN);  // anillo lleno (backpressure); no ocurre con envío inmediato
            return;
        }
        const std::uint64_t user_data = next_id++;
        sqe->opcode = static_cast<std::uint8_t>(opcode);
        sqe->fd = fd_arg;
        sqe->off = offset;
        sqe->addr = addr;
        sqe->len = len;
        sqe->fsync_flags = op_flags;  // unión: comparte hueco con accept_flags/msg_flags/etc.
        sqe->user_data = user_data;
        inflight.emplace(user_data, OpState{.on_done = std::move(on_done)});
        submit(index);
    }
};

IoUringBackend::Ring::Ring(unsigned requested) {
    io_uring_params params{};
    const long setup = io_uring_setup(requested, &params);
    if (setup < 0) {
        throw std::system_error(errno, std::generic_category(), "io_uring_setup");
    }
    fd = static_cast<int>(setup);
    entries = params.sq_entries;

    if ((params.features & IORING_FEAT_SINGLE_MMAP) == 0) {
        release();
        throw std::system_error(ENOTSUP, std::generic_category(), "io_uring sin SINGLE_MMAP");
    }

    const std::size_t sring = params.sq_off.array + (params.sq_entries * sizeof(std::uint32_t));
    const std::size_t cring = params.cq_off.cqes + (params.cq_entries * sizeof(io_uring_cqe));
    ring_size = std::max(sring, cring);
    ring_map = normalize_map(::mmap(nullptr, ring_size, PROT_READ | PROT_WRITE,
                                    MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQ_RING));
    sqes_size = params.sq_entries * sizeof(io_uring_sqe);
    sqes_map = normalize_map(::mmap(nullptr, sqes_size, PROT_READ | PROT_WRITE,
                                    MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQES));
    if (ring_map == nullptr || sqes_map == nullptr) {
        const int err = errno;
        release();
        throw std::system_error(err, std::generic_category(), "io_uring mmap");
    }

    sq_head = at_offset<std::uint32_t>(ring_map, params.sq_off.head);
    sq_tail = at_offset<std::uint32_t>(ring_map, params.sq_off.tail);
    sq_ring_mask = at_offset<std::uint32_t>(ring_map, params.sq_off.ring_mask);
    sq_array = at_offset<std::uint32_t>(ring_map, params.sq_off.array);
    cq_head = at_offset<std::uint32_t>(ring_map, params.cq_off.head);
    cq_tail = at_offset<std::uint32_t>(ring_map, params.cq_off.tail);
    cq_ring_mask = at_offset<std::uint32_t>(ring_map, params.cq_off.ring_mask);
    cqes = at_offset<io_uring_cqe>(ring_map, params.cq_off.cqes);
    sqe_array = static_cast<io_uring_sqe*>(sqes_map);
}

IoUringBackend::IoUringBackend(unsigned entries) : ring_(std::make_unique<Ring>(entries)) {}
IoUringBackend::~IoUringBackend() = default;

void IoUringBackend::submit_read(int fd, MutByteSpan buffer, std::uint64_t offset,
                                 Completion on_done) {
    ring_->submit_op(IORING_OP_READ, fd, address_of(buffer.data()),
                     static_cast<std::uint32_t>(buffer.size()), offset, 0, std::move(on_done));
}

void IoUringBackend::submit_write(int fd, ByteSpan data, std::uint64_t offset, Completion on_done) {
    ring_->submit_op(IORING_OP_WRITE, fd, address_of(data.data()),
                     static_cast<std::uint32_t>(data.size()), offset, 0, std::move(on_done));
}

void IoUringBackend::submit_fsync(int fd, bool datasync, Completion on_done) {
    const std::uint32_t flags = datasync ? IORING_FSYNC_DATASYNC : 0U;
    ring_->submit_op(IORING_OP_FSYNC, fd, 0, 0, 0, flags, std::move(on_done));
}

void IoUringBackend::submit_accept(int listen_fd, Completion on_done) {
    ring_->submit_op(IORING_OP_ACCEPT, listen_fd, 0, 0, 0, 0, std::move(on_done));
}

void IoUringBackend::submit_recv(int fd, MutByteSpan buffer, Completion on_done) {
    ring_->submit_op(IORING_OP_RECV, fd, address_of(buffer.data()),
                     static_cast<std::uint32_t>(buffer.size()), 0, 0, std::move(on_done));
}

void IoUringBackend::submit_send(int fd, ByteSpan data, Completion on_done) {
    ring_->submit_op(IORING_OP_SEND, fd, address_of(data.data()),
                     static_cast<std::uint32_t>(data.size()), 0, 0, std::move(on_done));
}

void IoUringBackend::submit_timer(MonoTime deadline, Completion on_done) {
    Ring& ring = *ring_;
    std::uint32_t index = 0;
    io_uring_sqe* sqe = ring.acquire_sqe(&index);
    if (sqe == nullptr) {
        on_done(-EAGAIN);
        return;
    }
    const auto nanos =
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch()).count();
    Ring::OpState state{.on_done = std::move(on_done), .is_timer = true, .timespec = {}};
    state.timespec.tv_sec = nanos / 1'000'000'000;
    state.timespec.tv_nsec = nanos % 1'000'000'000;

    const std::uint64_t user_data = ring.next_id++;
    // El kernel lee el timespec del nodo del mapa (dirección estable): se inserta antes de
    // apuntarlo.
    auto it = ring.inflight.emplace(user_data, std::move(state)).first;
    sqe->opcode = static_cast<std::uint8_t>(IORING_OP_TIMEOUT);
    sqe->fd = -1;
    sqe->addr = address_of(&it->second.timespec);
    sqe->len = 1;
    sqe->off = 0;
    sqe->timeout_flags = IORING_TIMEOUT_ABS;
    sqe->user_data = user_data;
    ring.submit(index);
}

int IoUringBackend::run_completions(int max) {
    Ring& ring = *ring_;
    // Drenado **no bloqueante**: procesa las completions ya disponibles en la CQ y vuelve. La
    // espera bloqueante (con timeout) y la interrupción por `wake` son del bucle del reactor (R6).
    std::uint32_t head = *ring.cq_head;  // único consumidor: lectura simple
    const std::uint32_t tail = load_acquire(ring.cq_tail);

    int processed = 0;
    while (head != tail && processed < max) {
        const io_uring_cqe& cqe = ring.cqes[head & *ring.cq_ring_mask];
        const std::uint64_t user_data = cqe.user_data;
        std::int32_t result = cqe.res;
        ++head;

        auto found = ring.inflight.find(user_data);
        if (found != ring.inflight.end()) {
            auto state = std::move(found->second);
            ring.inflight.erase(found);
            if (state.is_timer && result == -ETIME) {
                result = 0;  // el temporizador venció con normalidad: éxito, no error
            }
            state.on_done(result);  // puede reentrar (encolar nuevas ops): es seguro
        }
        ++processed;
    }
    store_release(ring.cq_head, head);
    return processed;
}

void IoUringBackend::wake() {
    // Sin efecto por ahora: la integración con el reactor (eventfd registrado en el anillo para
    // interrumpir `io_uring_enter` desde otro hilo) llega en R6 (CrossCoreMailbox).
}

}  // namespace nexus
