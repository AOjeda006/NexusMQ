/// @file   io/iocp_backend.cpp
/// @brief  Implementación del backend IOCP (Windows) del puerto Proactor (ADR-0021/0022).
/// @ingroup io
///
/// @details Espejo Windows del backend io_uring: ambos son **proactores** (se inicia la operación y
///   el SO notifica al completarse), así que comparten el puerto `Proactor` y el bucle del reactor.
///   Toda la maquinaria Win32/Winsock (puerto, `OVERLAPPED` en vuelo, `AcceptEx`) se confina en el
///   *pimpl* `Port` (RAII), sin filtrar `<windows.h>` al resto del árbol. **Compile-verificado con
///   MinGW-w64** y **verificado en runtime sobre Windows con MSVC** (arnés `tools/wincheck`,
///   ADR-0023).

#include "io/iocp_backend.hpp"

#ifdef _WIN32

#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0A00  // Windows 10: GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY
#endif

// clang-format off
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#include <windows.h>
// clang-format on

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace nexus {

namespace {

// Traduce un error de Win32/Winsock a un resultado negativo estilo io_uring (el borde lo mapea a
// `Error{IoError}`). Nunca devuelve 0 para un fallo (evita confundirlo con éxito de 0 bytes).
Proactor::IoResult neg_error(unsigned long code) {
    const auto value = static_cast<Proactor::IoResult>(code == 0 ? 1UL : code);
    return -value;
}

// NOLINTNEXTLINE(*-reinterpret-cast): el NativeHandle alberga el HANDLE de Win32 (ADR-0021/0022).
HANDLE as_handle(NativeHandle fd) noexcept {
    return reinterpret_cast<HANDLE>(fd);
}
SOCKET as_socket(NativeHandle fd) noexcept {
    return static_cast<SOCKET>(fd);
}
// NOLINTNEXTLINE(*-reinterpret-cast): un SOCKET se asocia al puerto como cualquier HANDLE.
HANDLE socket_as_handle(SOCKET sock) noexcept {
    return reinterpret_cast<HANDLE>(sock);
}

}  // namespace

/// Puerto IOCP + sockets/handles asociados + operaciones en vuelo. Gestión cruda confinada (RAII).
struct IocpBackend::Port {
    /// Una operación en vuelo: su `OVERLAPPED` (lo que devuelve el puerto), su completion y los
    /// recursos que deben vivir hasta la notificación (el handle para `GetOverlappedResult`, y para
    /// `accept` el socket precreado y el búfer de direcciones de `AcceptEx`).
    struct Op {
        OVERLAPPED ov{};
        Proactor::Completion on_done;
        HANDLE handle = INVALID_HANDLE_VALUE;
        bool is_accept = false;
        SOCKET accept_sock = INVALID_SOCKET;
        SOCKET listen_sock = INVALID_SOCKET;
        // AcceptEx escribe las direcciones local/remota: cada una necesita sizeof(sockaddr)+16.
        std::array<std::byte, 2 * (sizeof(sockaddr_in) + 16)> accept_buf{};
        bool forced = false;  // resultado predeterminado (fsync/errores inmediatos)
        Proactor::IoResult forced_result = 0;
    };

    HANDLE iocp = nullptr;
    LPFN_ACCEPTEX accept_ex = nullptr;
    std::unordered_set<HANDLE> associated;
    std::unordered_map<OVERLAPPED*, std::unique_ptr<Op>> inflight;
    std::multimap<MonoTime, Proactor::Completion> timers;

    explicit Port(unsigned entries) {
        WSADATA wsa = {};
        if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
            throw std::system_error(::WSAGetLastError(), std::system_category(), "WSAStartup");
        }
        iocp = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0);
        if (iocp == nullptr) {
            const auto err = ::GetLastError();
            ::WSACleanup();
            throw std::system_error(static_cast<int>(err), std::system_category(),
                                    "CreateIoCompletionPort");
        }
        inflight.reserve(entries);
    }

    ~Port() {
        if (iocp != nullptr) {
            ::CloseHandle(iocp);
        }
        ::WSACleanup();
    }

    Port(const Port&) = delete;
    Port& operator=(const Port&) = delete;
    Port(Port&&) = delete;
    Port& operator=(Port&&) = delete;

    /// Asocia un handle/socket al puerto antes de su primera operación (idempotente).
    void associate(HANDLE handle) {
        if (associated.insert(handle).second) {
            ::CreateIoCompletionPort(handle, iocp, 0, 0);
        }
    }

    /// Registra una operación (el mapa la posee hasta su completion) y devuelve un puntero estable.
    Op* track(std::unique_ptr<Op> op) {
        Op* raw = op.get();
        inflight.emplace(&raw->ov, std::move(op));
        return raw;
    }

    /// Encola una completion sintética para @p op con @p result (fsync síncrono o error inmediato):
    /// la entrega `run_completions`, nunca el hilo que hace `submit` (no reentra la corrutina).
    void post_forced(Op* op, Proactor::IoResult result) {
        op->forced = true;
        op->forced_result = result;
        ::PostQueuedCompletionStatus(iocp, 0, 0, &op->ov);
    }

    /// Carga el puntero a `AcceptEx` (función de extensión) la primera vez, vía `WSAIoctl`.
    void ensure_accept_ex(SOCKET sock) {
        if (accept_ex != nullptr) {
            return;
        }
        GUID guid = WSAID_ACCEPTEX;
        DWORD bytes = 0;
        ::WSAIoctl(sock, SIO_GET_EXTENSION_FUNCTION_POINTER, &guid, sizeof(guid), &accept_ex,
                   sizeof(accept_ex), &bytes, nullptr, nullptr);
    }

    /// Calcula el timeout (ms) de la espera: el menor entre @p deadline y el temporizador más
    /// próximo; `0` si ya venció. Acotado para no pasar `INFINITE` por accidente.
    DWORD timeout_until(MonoTime deadline) const {
        MonoTime target = deadline;
        if (!timers.empty() && timers.begin()->first < target) {
            target = timers.begin()->first;
        }
        const auto now = std::chrono::steady_clock::now();
        if (target <= now) {
            return 0;
        }
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(target - now).count();
        constexpr long long kMaxWaitMs = MAXDWORD - 1;
        return static_cast<DWORD>(ms > kMaxWaitMs ? kMaxWaitMs : ms);
    }

    /// Dispara los temporizadores vencidos (hasta agotar @p budget), con resultado 0 (éxito).
    int fire_expired_timers(int budget) {
        int fired = 0;
        const auto now = std::chrono::steady_clock::now();
        while (fired < budget && !timers.empty() && timers.begin()->first <= now) {
            auto node = timers.begin();
            Proactor::Completion on_done = std::move(node->second);
            timers.erase(node);
            on_done(0);
            ++fired;
        }
        return fired;
    }

    /// Resuelve el resultado de una entrada de completion: forzado, handle aceptado o bytes/errno.
    Proactor::IoResult resolve(Op& op) {
        if (op.forced) {
            return op.forced_result;
        }
        if (op.is_accept) {
            // SO_UPDATE_ACCEPT_CONTEXT hereda las propiedades del listener en el socket aceptado.
            // NOLINTNEXTLINE(*-reinterpret-cast): setsockopt toma const char* en Winsock.
            ::setsockopt(op.accept_sock, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
                         reinterpret_cast<const char*>(&op.listen_sock), sizeof(op.listen_sock));
            return static_cast<Proactor::IoResult>(op.accept_sock);
        }
        DWORD bytes = 0;
        if (::GetOverlappedResult(op.handle, &op.ov, &bytes, FALSE) == FALSE) {
            return neg_error(::GetLastError());
        }
        return static_cast<Proactor::IoResult>(bytes);
    }

    /// Drena hasta @p max completions, esperando como mucho @p timeout_ms en el puerto.
    int drain(int max, DWORD timeout_ms) {
        const ULONG capacity = static_cast<ULONG>(max < 1 ? 1 : max);
        std::vector<OVERLAPPED_ENTRY> entries(capacity);
        ULONG removed = 0;
        const BOOL ok = ::GetQueuedCompletionStatusEx(iocp, entries.data(), capacity, &removed,
                                                      timeout_ms, FALSE);
        int processed = 0;
        if (ok == TRUE) {
            for (ULONG i = 0; i < removed; ++i) {
                OVERLAPPED* ov = entries[i].lpOverlapped;
                if (ov == nullptr) {
                    continue;  // paquete de wake() (PostQueuedCompletionStatus sin OVERLAPPED)
                }
                auto found = inflight.find(ov);
                if (found == inflight.end()) {
                    continue;
                }
                std::unique_ptr<Op> op = std::move(found->second);
                inflight.erase(found);
                op->on_done(resolve(*op));  // puede reentrar (encolar nuevas ops): es seguro
                ++processed;
            }
        }
        processed += fire_expired_timers(max - processed);
        return processed;
    }
};

IocpBackend::IocpBackend(unsigned entries) : port_(std::make_unique<Port>(entries)) {}
IocpBackend::~IocpBackend() = default;

void IocpBackend::submit_read(NativeHandle fd, MutByteSpan buffer, std::uint64_t offset,
                              Completion on_done) {
    HANDLE handle = as_handle(fd);
    port_->associate(handle);
    auto op = std::make_unique<Port::Op>();
    op->handle = handle;
    op->on_done = std::move(on_done);
    op->ov.Offset = static_cast<DWORD>(offset & 0xFFFFFFFFULL);
    op->ov.OffsetHigh = static_cast<DWORD>(offset >> 32U);
    Port::Op* raw = port_->track(std::move(op));
    if (::ReadFile(handle, buffer.data(), static_cast<DWORD>(buffer.size()), nullptr, &raw->ov) ==
        FALSE) {
        const DWORD err = ::GetLastError();
        if (err == ERROR_HANDLE_EOF) {
            port_->post_forced(raw, 0);  // EOF: 0 bytes, no es error
        } else if (err != ERROR_IO_PENDING) {
            port_->post_forced(raw, neg_error(err));
        }
    }
}

void IocpBackend::submit_write(NativeHandle fd, ByteSpan data, std::uint64_t offset,
                               Completion on_done) {
    HANDLE handle = as_handle(fd);
    port_->associate(handle);
    auto op = std::make_unique<Port::Op>();
    op->handle = handle;
    op->on_done = std::move(on_done);
    op->ov.Offset = static_cast<DWORD>(offset & 0xFFFFFFFFULL);
    op->ov.OffsetHigh = static_cast<DWORD>(offset >> 32U);
    Port::Op* raw = port_->track(std::move(op));
    // NOLINTNEXTLINE(*-reinterpret-cast): byte→void* para la API de Win32.
    if (::WriteFile(handle, data.data(), static_cast<DWORD>(data.size()), nullptr, &raw->ov) ==
        FALSE) {
        const DWORD err = ::GetLastError();
        if (err != ERROR_IO_PENDING) {
            port_->post_forced(raw, neg_error(err));
        }
    }
}

void IocpBackend::submit_fsync(NativeHandle fd, bool /*datasync*/, Completion on_done) {
    // Windows no distingue fsync/fdatasync: FlushFileBuffers es síncrono; su resultado se entrega
    // de forma diferida (post_forced) para no reentrar la corrutina dentro de submit.
    HANDLE handle = as_handle(fd);
    port_->associate(handle);
    auto op = std::make_unique<Port::Op>();
    op->handle = handle;
    op->on_done = std::move(on_done);
    Port::Op* raw = port_->track(std::move(op));
    const BOOL ok = ::FlushFileBuffers(handle);
    port_->post_forced(raw, ok == TRUE ? 0 : neg_error(::GetLastError()));
}

void IocpBackend::submit_accept(NativeHandle listen_fd, Completion on_done) {
    SOCKET listen_sock = as_socket(listen_fd);
    port_->associate(socket_as_handle(listen_sock));
    port_->ensure_accept_ex(listen_sock);

    const SOCKET accept_sock =
        ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
    auto op = std::make_unique<Port::Op>();
    op->on_done = std::move(on_done);
    if (accept_sock == INVALID_SOCKET) {
        Port::Op* raw = port_->track(std::move(op));
        port_->post_forced(raw, neg_error(::WSAGetLastError()));
        return;
    }
    op->is_accept = true;
    op->accept_sock = accept_sock;
    op->listen_sock = listen_sock;
    op->handle = socket_as_handle(listen_sock);
    Port::Op* raw = port_->track(std::move(op));

    DWORD received = 0;
    const DWORD addr_len = sizeof(sockaddr_in) + 16;
    if (port_->accept_ex(listen_sock, accept_sock, raw->accept_buf.data(), 0, addr_len, addr_len,
                         &received, &raw->ov) == FALSE) {
        const DWORD err = ::WSAGetLastError();
        if (err != ERROR_IO_PENDING) {
            ::closesocket(accept_sock);
            raw->accept_sock = INVALID_SOCKET;
            port_->post_forced(raw, neg_error(err));
        }
    }
}

void IocpBackend::submit_recv(NativeHandle fd, MutByteSpan buffer, Completion on_done) {
    SOCKET sock = as_socket(fd);
    port_->associate(socket_as_handle(sock));
    auto op = std::make_unique<Port::Op>();
    op->handle = socket_as_handle(sock);
    op->on_done = std::move(on_done);
    Port::Op* raw = port_->track(std::move(op));
    WSABUF wsabuf = {};
    wsabuf.len = static_cast<ULONG>(buffer.size());
    // NOLINTNEXTLINE(*-reinterpret-cast): byte→CHAR* para la API de Winsock.
    wsabuf.buf = reinterpret_cast<CHAR*>(buffer.data());
    DWORD flags = 0;
    if (::WSARecv(sock, &wsabuf, 1, nullptr, &flags, &raw->ov, nullptr) == SOCKET_ERROR) {
        const DWORD err = ::WSAGetLastError();
        if (err != WSA_IO_PENDING) {
            port_->post_forced(raw, neg_error(err));
        }
    }
}

void IocpBackend::submit_send(NativeHandle fd, ByteSpan data, Completion on_done) {
    SOCKET sock = as_socket(fd);
    port_->associate(socket_as_handle(sock));
    auto op = std::make_unique<Port::Op>();
    op->handle = socket_as_handle(sock);
    op->on_done = std::move(on_done);
    Port::Op* raw = port_->track(std::move(op));
    WSABUF wsabuf = {};
    wsabuf.len = static_cast<ULONG>(data.size());
    // NOLINTNEXTLINE(*-reinterpret-cast): byte→CHAR* (const) para la API de Winsock.
    wsabuf.buf = const_cast<CHAR*>(reinterpret_cast<const CHAR*>(data.data()));
    if (::WSASend(sock, &wsabuf, 1, nullptr, 0, &raw->ov, nullptr) == SOCKET_ERROR) {
        const DWORD err = ::WSAGetLastError();
        if (err != WSA_IO_PENDING) {
            port_->post_forced(raw, neg_error(err));
        }
    }
}

void IocpBackend::submit_timer(MonoTime deadline, Completion on_done) {
    port_->timers.emplace(deadline, std::move(on_done));
}

int IocpBackend::run_completions(int max) {
    return port_->drain(max, 0);  // no bloqueante: solo lo ya listo + temporizadores vencidos
}

int IocpBackend::wait_completions(int max, MonoTime deadline) {
    return port_->drain(max, port_->timeout_until(deadline));
}

void IocpBackend::wake() {
    // Paquete centinela sin OVERLAPPED: interrumpe GetQueuedCompletionStatusEx; seguro entre hilos.
    ::PostQueuedCompletionStatus(port_->iocp, 0, 0, nullptr);
}

}  // namespace nexus

#endif  // _WIN32
