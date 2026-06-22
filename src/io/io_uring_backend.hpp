/// @file   io/io_uring_backend.hpp
/// @brief  IoUringBackend: backend del Proactor sobre io_uring (Linux), vía el uapi del kernel.
/// @ingroup io

#pragma once

#include <cstdint>
#include <memory>

#include "common/bytes.hpp"
#include "common/types.hpp"
#include "io/proactor.hpp"

namespace nexus {

/// @brief Backend del `Proactor` sobre **io_uring** (Linux). Afinidad: REACTOR-LOCAL.
/// @details Habla directamente con el uapi del kernel (`io_uring_setup`/`io_uring_enter` vía
///   `syscall`, anillos mapeados con `mmap`), **sin liburing** (ADR-0012): cero dependencias
///   externas, mismo binario en local y CI con solo las cabeceras del kernel. Un anillo por
///   instancia (un reactor por núcleo, ADR-0005). Toda la gestión cruda (fd, mmap, anillos, ops en
///   vuelo) queda confinada en un *pimpl* RAII, sin filtrar `<linux/io_uring.h>` al resto del
///   árbol.
/// @invariant Tras construir, el anillo está listo; el destructor libera los mmap y cierra el fd.
class IoUringBackend final : public Proactor {
public:
    /// @brief Crea el anillo con al menos @p entries entradas (el kernel la redondea).
    /// @throws std::system_error si io_uring no está disponible (kernel viejo, seccomp…). La
    ///   creación es **plano de control** (arranque), donde ADR-0009 permite excepciones.
    explicit IoUringBackend(unsigned entries);
    ~IoUringBackend() override;

    IoUringBackend(const IoUringBackend&) = delete;
    IoUringBackend& operator=(const IoUringBackend&) = delete;
    IoUringBackend(IoUringBackend&&) = delete;
    IoUringBackend& operator=(IoUringBackend&&) = delete;

    void submit_read(int fd, MutByteSpan buffer, std::uint64_t offset, Completion on_done) override;
    void submit_write(int fd, ByteSpan data, std::uint64_t offset, Completion on_done) override;
    void submit_fsync(int fd, bool datasync, Completion on_done) override;
    void submit_accept(int listen_fd, Completion on_done) override;
    void submit_connect(int fd, ByteSpan addr, Completion on_done) override;
    void submit_recv(int fd, MutByteSpan buffer, Completion on_done) override;
    void submit_send(int fd, ByteSpan data, Completion on_done) override;
    void submit_timer(MonoTime deadline, Completion on_done) override;
    int run_completions(int max) override;
    int wait_completions(int max, MonoTime deadline) override;
    void wake() override;

private:
    struct Ring;  // detalles de io_uring, definidos en el .cpp (oculta el uapi del kernel)
    std::unique_ptr<Ring> ring_;
};

}  // namespace nexus
