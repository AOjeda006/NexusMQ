/// @file   io/iocp_backend.hpp
/// @brief  IocpBackend: backend del Proactor sobre IOCP (Windows) — **diseño, F10** (ADR-0021).
/// @ingroup io
///
/// @details **Artefacto de diseño, no construido.** NexusMQ es Linux-nativo (io_uring, `O_DIRECT`,
///   `eventfd`, sockets POSIX): un backend Windows real exige además portar `File`/`Socket` a la
///   API Win32, lo que excede una funcionalidad *stretch* y **no es verificable en este entorno**
///   (sin *toolchain* MSVC/SDK; el CI es solo Linux). Aquí se fija **cómo** mapearía IOCP al puerto
///   `Proactor` (declaración + diseño); la implementación queda **diferida** y documentada en
///   ADR-0021. Todo el contenido va bajo `#ifdef _WIN32`, así que en Linux este encabezado es vacío
///   y no afecta a la build ni a la puerta de calidad.

#pragma once

#ifdef _WIN32

#include <cstdint>
#include <memory>

#include "common/bytes.hpp"
#include "common/types.hpp"
#include "io/proactor.hpp"

namespace nexus {

/// @brief Backend del `Proactor` sobre **I/O Completion Ports** (Windows). Afinidad: REACTOR-LOCAL.
/// @details IOCP es, como io_uring, un modelo **proactor** (se inicia la operación y el SO notifica
///   al **completarse**), de modo que encaja en el mismo puerto sin cambiar el bucle del reactor.
///   Mapeo de cada operación a Win32 (cada una lleva una estructura `OVERLAPPED` propia que porta
///   el `offset` y enlaza con su `Completion`):
///   - **construcción:** `CreateIoCompletionPort(INVALID_HANDLE_VALUE, ...)` crea el puerto; cada
///     `HANDLE`/`SOCKET` se **asocia** al puerto antes de su primera operación. El `int fd` del
///     puerto `Proactor` se trata como el `HANDLE`/`SOCKET` nativo (responsabilidad de `File`/
///     `Socket` al portarse).
///   - `submit_read`/`submit_write`: `ReadFile`/`WriteFile` con `OVERLAPPED` (campos `Offset`/
///     `OffsetHigh` = @p offset). `submit_fsync`: `FlushFileBuffers` (síncrono; se difiere la
///     *completion* vía `PostQueuedCompletionStatus`).
///   - `submit_accept`: `AcceptEx` (precarga un socket aceptado). `submit_recv`/`submit_send`:
///     `WSARecv`/`WSASend` con `WSAOVERLAPPED`.
///   - `submit_timer`: un *waitable timer* asociado, o el *timeout* de
///   `GetQueuedCompletionStatusEx`.
///   - `run_completions`/`wait_completions`: `GetQueuedCompletionStatusEx` drena un **lote** de
///     *completions* (sin bloquear con `dwMilliseconds = 0`; bloqueando hasta @p deadline en la
///     espera). El `result` estilo io_uring (`>=0` bytes / `<0` `-errno`) se traduce desde
///     `dwNumberOfBytesTransferred` / `GetLastError`.
///   - `wake`: `PostQueuedCompletionStatus` encola una *completion* centinela que desbloquea la
///     espera (seguro desde otro hilo).
///   Toda la maquinaria Win32 (puerto, `OVERLAPPED` en vuelo, punteros a funciones de extensión
///   como `AcceptEx`) se confina en un *pimpl* RAII, sin filtrar `<windows.h>`/`<winsock2.h>` al
///   árbol.
/// @note **No implementado** (sin `.cpp`): declaración de diseño. Ver ADR-0021.
class IocpBackend final : public Proactor {
public:
    /// @brief Crea el puerto de *completions* dimensionado para @p entries operaciones en vuelo.
    /// @throws std::system_error si IOCP/Winsock no se pueden inicializar (plano de control,
    ///   ADR-0009 permite excepciones en arranque).
    explicit IocpBackend(unsigned entries);
    ~IocpBackend() override;

    IocpBackend(const IocpBackend&) = delete;
    IocpBackend& operator=(const IocpBackend&) = delete;
    IocpBackend(IocpBackend&&) = delete;
    IocpBackend& operator=(IocpBackend&&) = delete;

    void submit_read(int fd, MutByteSpan buffer, std::uint64_t offset, Completion on_done) override;
    void submit_write(int fd, ByteSpan data, std::uint64_t offset, Completion on_done) override;
    void submit_fsync(int fd, bool datasync, Completion on_done) override;
    void submit_accept(int listen_fd, Completion on_done) override;
    void submit_recv(int fd, MutByteSpan buffer, Completion on_done) override;
    void submit_send(int fd, ByteSpan data, Completion on_done) override;
    void submit_timer(MonoTime deadline, Completion on_done) override;
    int run_completions(int max) override;
    int wait_completions(int max, MonoTime deadline) override;
    void wake() override;

private:
    struct Port;  // detalles de IOCP/Winsock, definidos en el .cpp (oculta <windows.h>)
    std::unique_ptr<Port> port_;
};

}  // namespace nexus

#endif  // _WIN32
