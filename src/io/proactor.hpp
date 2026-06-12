/// @file   io/proactor.hpp
/// @brief  Proactor: puerto abstracto de E/S asíncrona por *completions* (un anillo por reactor).
/// @ingroup io

#pragma once

#include <cstdint>

#include "common/bytes.hpp"
#include "common/move_only_function.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Puerto de E/S asíncrona orientada a *completions* (proactor). Afinidad: REACTOR-LOCAL.
/// @details Cada reactor posee su propia instancia (un anillo io_uring por núcleo, ADR-0005). Las
///   operaciones se **encolan** (`submit_*`) con una *completion* que se ejecuta cuando terminan;
///   el reactor las drena con `run_completions`. Es el **único** punto de contacto con la E/S del
///   SO: el resto del sistema depende solo de esta abstracción (DIP), con backend io_uring en Linux
///   y un doble de test (`FakeProactor`) para pruebas deterministas.
/// @note La completion **no** debe ejecutarse de forma síncrona dentro de `submit_*` (reentraría en
///   una corrutina aún suspendiéndose): siempre se difiere a `run_completions`.
class Proactor {
public:
    /// Resultado estilo io_uring: `>= 0` éxito (bytes/fd); `< 0` error con `-result` = errno.
    using Completion = MoveOnlyFunction<void(std::int32_t result)>;

    Proactor(const Proactor&) = delete;
    Proactor& operator=(const Proactor&) = delete;
    Proactor(Proactor&&) = delete;
    Proactor& operator=(Proactor&&) = delete;
    virtual ~Proactor() = default;

    /// Lee de @p fd en @p buffer desde @p offset.
    virtual void submit_read(int fd, MutByteSpan buffer, std::uint64_t offset,
                             Completion on_done) = 0;
    /// Escribe @p data en @p fd en @p offset.
    virtual void submit_write(int fd, ByteSpan data, std::uint64_t offset, Completion on_done) = 0;
    /// Fuerza durabilidad de @p fd (`fsync`/`fdatasync` según @p datasync).
    virtual void submit_fsync(int fd, bool datasync, Completion on_done) = 0;
    /// Acepta una conexión entrante en @p listen_fd (result = fd del socket aceptado).
    virtual void submit_accept(int listen_fd, Completion on_done) = 0;
    /// Recibe de un socket @p fd en @p buffer.
    virtual void submit_recv(int fd, MutByteSpan buffer, Completion on_done) = 0;
    /// Envía @p data por el socket @p fd.
    virtual void submit_send(int fd, ByteSpan data, Completion on_done) = 0;
    /// Dispara una completion cuando el reloj monótono alcance @p deadline.
    virtual void submit_timer(MonoTime deadline, Completion on_done) = 0;

    /// @brief Drena hasta @p max completions terminadas, ejecutando su callback.
    /// @return Cuántas se procesaron.
    virtual int run_completions(int max) = 0;

    /// Despierta el reactor si está bloqueado esperando completions (desde otro hilo).
    virtual void wake() = 0;

protected:
    Proactor() = default;
};

}  // namespace nexus
