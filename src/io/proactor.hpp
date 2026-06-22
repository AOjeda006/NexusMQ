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
    /// @brief Resultado de una operación, estilo io_uring: `>= 0` éxito (bytes o handle aceptado);
    ///   `< 0` error con `-result` = errno.
    /// @details Ancho de **puntero** (`intptr_t`) y no `int32`: además de cuentas y errores, debe
    ///   alojar un `NativeHandle` (un `SOCKET`/`HANDLE` de Windows no cabe en 32 bits). En Linux
    ///   `intptr_t` aloja el `int` del `cqe->res` sin cambio de comportamiento.
    using IoResult = std::intptr_t;
    using Completion = MoveOnlyFunction<void(IoResult result)>;

    Proactor(const Proactor&) = delete;
    Proactor& operator=(const Proactor&) = delete;
    Proactor(Proactor&&) = delete;
    Proactor& operator=(Proactor&&) = delete;
    virtual ~Proactor() = default;

    /// Lee de @p fd en @p buffer desde @p offset.
    virtual void submit_read(NativeHandle fd, MutByteSpan buffer, std::uint64_t offset,
                             Completion on_done) = 0;
    /// Escribe @p data en @p fd en @p offset.
    virtual void submit_write(NativeHandle fd, ByteSpan data, std::uint64_t offset,
                              Completion on_done) = 0;
    /// Fuerza durabilidad de @p fd (`fsync`/`fdatasync` según @p datasync).
    virtual void submit_fsync(NativeHandle fd, bool datasync, Completion on_done) = 0;
    /// Acepta una conexión entrante en @p listen_fd (result = handle del socket aceptado).
    virtual void submit_accept(NativeHandle listen_fd, Completion on_done) = 0;
    /// @brief Conecta @p fd (socket TCP creado, sin conectar) a la dirección @p addr (bytes de un
    ///   `sockaddr`); result = 0 en éxito, `< 0` errno en fallo.
    /// @details @p addr debe **sobrevivir hasta la completion** (el SO la lee de forma asíncrona):
    ///   pásala desde un almacenamiento que viva en el *frame* de la corrutina que hace `co_await`.
    virtual void submit_connect(NativeHandle fd, ByteSpan addr, Completion on_done) = 0;
    /// Recibe de un socket @p fd en @p buffer.
    virtual void submit_recv(NativeHandle fd, MutByteSpan buffer, Completion on_done) = 0;
    /// Envía @p data por el socket @p fd.
    virtual void submit_send(NativeHandle fd, ByteSpan data, Completion on_done) = 0;
    /// Dispara una completion cuando el reloj monótono alcance @p deadline.
    virtual void submit_timer(MonoTime deadline, Completion on_done) = 0;

    /// @brief Drena hasta @p max completions terminadas **sin bloquear**, ejecutando su callback.
    /// @return Cuántas se procesaron.
    virtual int run_completions(int max) = 0;

    /// @brief Bloquea hasta que haya alguna completion lista, llegue un `wake()` o se alcance
    ///   @p deadline; luego drena hasta @p max (como `run_completions`).
    /// @return Cuántas completions de usuario se ejecutaron.
    /// @details Es el modo de espera eficiente del bucle del reactor: cede la CPU mientras no haya
    ///   nada que hacer. El doble de test no bloquea (drena lo que haya, para tests deterministas).
    virtual int wait_completions(int max, MonoTime deadline) = 0;

    /// Despierta el reactor si está bloqueado en `wait_completions` (seguro desde otro hilo).
    virtual void wake() = 0;

protected:
    Proactor() = default;
};

}  // namespace nexus
