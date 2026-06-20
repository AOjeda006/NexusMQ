/// @file   io/iocp_backend.hpp
/// @brief  IocpBackend: backend del Proactor sobre IOCP (Windows) — **implementado** (ADR-0022).
/// @ingroup io
///
/// @details Espejo Windows del backend io_uring: ambos son **proactores** (se inicia la operación y
///   el SO notifica al completarse), de modo que comparten el puerto `Proactor` sin tocar el bucle
///   del reactor. La implementación (`iocp_backend.cpp`) está **compile-verificada con MinGW-w64**
///   (headers Win32 reales) y, además, **verificada en runtime sobre Windows con MSVC** (arnés
///   `tools/wincheck`; ver ADR-0023, que reemplaza al ADR-0022). Enlaza contra `ws2_32`/`mswsock`.
///   Todo el contenido va bajo `#ifdef _WIN32`: en Linux este encabezado es vacío y no afecta a la
///   build ni a la puerta de calidad. La maquinaria Win32/Winsock se confina en el *pimpl* `Port`
///   (no filtra
///   `<windows.h>`).

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
///     `HANDLE`/`SOCKET` se **asocia** al puerto antes de su primera operación. El `NativeHandle`
///     del puerto `Proactor` porta el `HANDLE`/`SOCKET` nativo (lo abren `File`/`Socket`).
///   - `submit_read`/`submit_write`: `ReadFile`/`WriteFile` con `OVERLAPPED` (campos `Offset`/
///     `OffsetHigh` = @p offset). `submit_fsync`: `FlushFileBuffers` (síncrono; se difiere la
///     *completion* vía `PostQueuedCompletionStatus`).
///   - `submit_accept`: `AcceptEx` (precarga un socket aceptado). `submit_recv`/`submit_send`:
///     `WSARecv`/`WSASend` con `WSAOVERLAPPED`.
///   - `submit_timer`: los temporizadores se guardan en un `multimap` por *deadline*; vencen cuando
///     la espera de `GetQueuedCompletionStatusEx` agota su *timeout* (calculado al próximo).
///   - `run_completions`/`wait_completions`: `GetQueuedCompletionStatusEx` drena un **lote** de
///     *completions* (sin bloquear con `dwMilliseconds = 0`; bloqueando hasta @p deadline en la
///     espera). El `result` estilo io_uring (`>=0` bytes/handle / `<0` `-error`) se obtiene de
///     `GetOverlappedResult` (o del handle aceptado para `accept`).
///   - `wake`: `PostQueuedCompletionStatus` encola una *completion* centinela que desbloquea la
///     espera (seguro desde otro hilo).
///   Toda la maquinaria Win32 (puerto, `OVERLAPPED` en vuelo, punteros a funciones de extensión
///   como `AcceptEx`) se confina en un *pimpl* RAII, sin filtrar `<windows.h>`/`<winsock2.h>` al
///   árbol.
/// @note Implementado en `iocp_backend.cpp` (compile-verificado con MinGW y verificado en runtime
///   sobre Windows con MSVC vía `tools/wincheck`). Ver ADR-0023.
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

    void submit_read(NativeHandle fd, MutByteSpan buffer, std::uint64_t offset,
                     Completion on_done) override;
    void submit_write(NativeHandle fd, ByteSpan data, std::uint64_t offset,
                      Completion on_done) override;
    void submit_fsync(NativeHandle fd, bool datasync, Completion on_done) override;
    void submit_accept(NativeHandle listen_fd, Completion on_done) override;
    void submit_recv(NativeHandle fd, MutByteSpan buffer, Completion on_done) override;
    void submit_send(NativeHandle fd, ByteSpan data, Completion on_done) override;
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
