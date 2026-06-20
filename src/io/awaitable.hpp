/// @file   io/awaitable.hpp
/// @brief  Awaitables que suspenden una corrutina y la reanudan en la completion del Proactor.
/// @ingroup io

#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <string>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "io/proactor.hpp"

namespace nexus {

namespace detail {

/// Traduce un resultado estilo io_uring (`< 0` ⇒ errno negado) a un `Error{IoError}`.
[[nodiscard]] inline Error io_error_from(Proactor::IoResult result) {
    return Error{ErrorCode::IoError, "E/S asíncrona falló (errno " + std::to_string(-result) + ")"};
}

[[nodiscard]] inline expected<std::size_t> result_to_size(Proactor::IoResult result) {
    if (result < 0) {
        return std::unexpected(io_error_from(result));
    }
    return static_cast<std::size_t>(result);
}

[[nodiscard]] inline expected<NativeHandle> result_to_fd(Proactor::IoResult result) {
    if (result < 0) {
        return std::unexpected(io_error_from(result));
    }
    return static_cast<NativeHandle>(result);
}

[[nodiscard]] inline expected<void> result_to_void(Proactor::IoResult result) {
    if (result < 0) {
        return std::unexpected(io_error_from(result));
    }
    return {};
}

/// @brief Base CRTP de los awaitables de E/S: gestiona la suspensión y la reanudación.
/// @details `await_suspend` delega en `Derived::submit` el encolado de la operación, pasando una
///   completion que guarda el resultado y reanuda la corrutina. El awaitable vive en el *frame* de
///   la corrutina mientras está suspendida, así que `this` sigue válido cuando llega la completion.
/// @note El @p buffer de la operación debe sobrevivir hasta la completion: no usar `co_await` sobre
///   temporales que envuelvan búferes locales del que llama.
template <class Derived>
class IoAwaitable {
public:
    // `const` (no estático) a propósito: el lenguaje lo invoca sobre la instancia del awaitable;
    // `const` evita `readability-static-accessed-through-instance` en cada `co_await`.
    [[nodiscard]] bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> awaiting) {
        static_cast<Derived*>(this)->submit([this, awaiting](Proactor::IoResult result) {
            result_ = result;
            awaiting.resume();
        });
    }

protected:
    // Vista no propietaria; el awaitable es efímero (vive dentro del co_await).
    Proactor& proactor_;             // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Proactor::IoResult result_ = 0;  // lo fija la completion antes de reanudar

private:
    // Constructor privado + `friend Derived`: solo el propio derivado puede instanciar la base CRTP
    // (impide heredarla como plantilla regular con un `Derived` ajeno).
    friend Derived;
    explicit IoAwaitable(Proactor& proactor) noexcept : proactor_(proactor) {}
};

}  // namespace detail

/// `co_await` sobre una lectura posicional; produce los bytes leídos (`0` = EOF).
class ReadAwaitable : public detail::IoAwaitable<ReadAwaitable> {
public:
    ReadAwaitable(Proactor& proactor, NativeHandle fd, MutByteSpan buffer,
                  std::uint64_t offset) noexcept
        : IoAwaitable(proactor), fd_(fd), buffer_(buffer), offset_(offset) {}

    void submit(Proactor::Completion on_done) {
        proactor_.submit_read(fd_, buffer_, offset_, std::move(on_done));
    }
    [[nodiscard]] expected<std::size_t> await_resume() const {
        return detail::result_to_size(result_);
    }

private:
    NativeHandle fd_;
    MutByteSpan buffer_;
    std::uint64_t offset_;
};

/// `co_await` sobre una escritura posicional; produce los bytes escritos.
class WriteAwaitable : public detail::IoAwaitable<WriteAwaitable> {
public:
    WriteAwaitable(Proactor& proactor, NativeHandle fd, ByteSpan data,
                   std::uint64_t offset) noexcept
        : IoAwaitable(proactor), fd_(fd), data_(data), offset_(offset) {}

    void submit(Proactor::Completion on_done) {
        proactor_.submit_write(fd_, data_, offset_, std::move(on_done));
    }
    [[nodiscard]] expected<std::size_t> await_resume() const {
        return detail::result_to_size(result_);
    }

private:
    NativeHandle fd_;
    ByteSpan data_;
    std::uint64_t offset_;
};

/// `co_await` sobre un `fsync`/`fdatasync`.
class FsyncAwaitable : public detail::IoAwaitable<FsyncAwaitable> {
public:
    FsyncAwaitable(Proactor& proactor, NativeHandle fd, bool datasync) noexcept
        : IoAwaitable(proactor), fd_(fd), datasync_(datasync) {}

    void submit(Proactor::Completion on_done) {
        proactor_.submit_fsync(fd_, datasync_, std::move(on_done));
    }
    [[nodiscard]] expected<void> await_resume() const { return detail::result_to_void(result_); }

private:
    NativeHandle fd_;
    bool datasync_;
};

/// `co_await` sobre un `accept`; produce el descriptor del socket aceptado.
class AcceptAwaitable : public detail::IoAwaitable<AcceptAwaitable> {
public:
    AcceptAwaitable(Proactor& proactor, NativeHandle listen_fd) noexcept
        : IoAwaitable(proactor), listen_fd_(listen_fd) {}

    void submit(Proactor::Completion on_done) {
        proactor_.submit_accept(listen_fd_, std::move(on_done));
    }
    [[nodiscard]] expected<NativeHandle> await_resume() const {
        return detail::result_to_fd(result_);
    }

private:
    NativeHandle listen_fd_;
};

/// `co_await` sobre un `recv`; produce los bytes recibidos (`0` = conexión cerrada).
class RecvAwaitable : public detail::IoAwaitable<RecvAwaitable> {
public:
    RecvAwaitable(Proactor& proactor, NativeHandle fd, MutByteSpan buffer) noexcept
        : IoAwaitable(proactor), fd_(fd), buffer_(buffer) {}

    void submit(Proactor::Completion on_done) {
        proactor_.submit_recv(fd_, buffer_, std::move(on_done));
    }
    [[nodiscard]] expected<std::size_t> await_resume() const {
        return detail::result_to_size(result_);
    }

private:
    NativeHandle fd_;
    MutByteSpan buffer_;
};

/// `co_await` sobre un `send`; produce los bytes enviados.
class SendAwaitable : public detail::IoAwaitable<SendAwaitable> {
public:
    SendAwaitable(Proactor& proactor, NativeHandle fd, ByteSpan data) noexcept
        : IoAwaitable(proactor), fd_(fd), data_(data) {}

    void submit(Proactor::Completion on_done) {
        proactor_.submit_send(fd_, data_, std::move(on_done));
    }
    [[nodiscard]] expected<std::size_t> await_resume() const {
        return detail::result_to_size(result_);
    }

private:
    NativeHandle fd_;
    ByteSpan data_;
};

/// `co_await` que se reanuda cuando el reloj monótono alcanza un instante.
class TimerAwaitable : public detail::IoAwaitable<TimerAwaitable> {
public:
    TimerAwaitable(Proactor& proactor, MonoTime deadline) noexcept
        : IoAwaitable(proactor), deadline_(deadline) {}

    void submit(Proactor::Completion on_done) {
        proactor_.submit_timer(deadline_, std::move(on_done));
    }
    [[nodiscard]] expected<void> await_resume() const { return detail::result_to_void(result_); }

private:
    MonoTime deadline_;
};

// --- Azúcar de construcción (el tipo del awaitable queda implícito en el co_await). ---

[[nodiscard]] inline ReadAwaitable async_read(Proactor& proactor, NativeHandle fd,
                                              MutByteSpan buffer, std::uint64_t offset) noexcept {
    return ReadAwaitable{proactor, fd, buffer, offset};
}
[[nodiscard]] inline WriteAwaitable async_write(Proactor& proactor, NativeHandle fd, ByteSpan data,
                                                std::uint64_t offset) noexcept {
    return WriteAwaitable{proactor, fd, data, offset};
}
[[nodiscard]] inline FsyncAwaitable async_fsync(Proactor& proactor, NativeHandle fd,
                                                bool datasync = false) noexcept {
    return FsyncAwaitable{proactor, fd, datasync};
}
[[nodiscard]] inline AcceptAwaitable async_accept(Proactor& proactor,
                                                  NativeHandle listen_fd) noexcept {
    return AcceptAwaitable{proactor, listen_fd};
}
[[nodiscard]] inline RecvAwaitable async_recv(Proactor& proactor, NativeHandle fd,
                                              MutByteSpan buffer) noexcept {
    return RecvAwaitable{proactor, fd, buffer};
}
[[nodiscard]] inline SendAwaitable async_send(Proactor& proactor, NativeHandle fd,
                                              ByteSpan data) noexcept {
    return SendAwaitable{proactor, fd, data};
}
[[nodiscard]] inline TimerAwaitable async_timer(Proactor& proactor, MonoTime deadline) noexcept {
    return TimerAwaitable{proactor, deadline};
}

}  // namespace nexus
