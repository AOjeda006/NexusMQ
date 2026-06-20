/// @file   io/socket.cpp
/// @brief  Implementación de Socket/Listener (RAII + E/S asíncrona sobre el Proactor).
/// @ingroup io
///
/// @details Los métodos asíncronos (`async_recv`/`async_send`/`async_accept`) son **portables**:
///   delegan en el `Proactor` (io_uring en Linux, IOCP en Windows). El plano de control
///   (`connect`/`bind`/`close`/`set_nodelay`/`local_port`) tiene implementación por plataforma:
///   sockets POSIX o Winsock.

#include "io/socket.hpp"

#include <string>
#include <utility>

#include "io/awaitable.hpp"

#if defined(_WIN32)
// clang-format off
#include <winsock2.h>
#include <ws2tcpip.h>
// clang-format on
#include <system_error>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <system_error>
#endif

namespace nexus {

// --- Miembros especiales y E/S asíncrona: portables (la liberación concreta vive en `close`). ---

Socket::~Socket() {
    close();
}

Socket::Socket(Socket&& other) noexcept : fd_(std::exchange(other.fd_, kInvalidHandle)) {}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = std::exchange(other.fd_, kInvalidHandle);
    }
    return *this;
}

task<expected<std::size_t>> Socket::async_recv(Proactor& proactor, MutByteSpan buffer) const {
    co_return co_await RecvAwaitable{proactor, fd_, buffer};
}

task<expected<std::size_t>> Socket::async_send(Proactor& proactor, ByteSpan data) const {
    co_return co_await SendAwaitable{proactor, fd_, data};
}

Listener::~Listener() {
    close();
}

Listener::Listener(Listener&& other) noexcept : fd_(std::exchange(other.fd_, kInvalidHandle)) {}

Listener& Listener::operator=(Listener&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = std::exchange(other.fd_, kInvalidHandle);
    }
    return *this;
}

task<expected<Socket>> Listener::async_accept(Proactor& proactor) const {
    const expected<NativeHandle> accepted = co_await AcceptAwaitable{proactor, fd_};
    if (!accepted) {
        co_return std::unexpected(accepted.error());
    }
    co_return Socket{*accepted};
}

#if defined(_WIN32)

namespace {

// Inicializa Winsock una vez por proceso (RAII): WSAStartup al primer uso, WSACleanup al salir.
void ensure_winsock() {
    struct WsaInit {
        WsaInit() {
            WSADATA data = {};
            ::WSAStartup(MAKEWORD(2, 2), &data);
        }
        ~WsaInit() { ::WSACleanup(); }
        WsaInit(const WsaInit&) = delete;
        WsaInit& operator=(const WsaInit&) = delete;
        WsaInit(WsaInit&&) = delete;
        WsaInit& operator=(WsaInit&&) = delete;
    };
    static const WsaInit kInit;  // construcción thread-safe garantizada por el lenguaje
}

// Traduce el último error de Winsock a un Error del núcleo, con la operación como contexto.
std::unexpected<Error> io_error(const char* op) {
    return make_error(ErrorCode::IoError,
                      std::string{op} + ": " + std::system_category().message(::WSAGetLastError()));
}

// NOLINTNEXTLINE(*-reinterpret-cast): el NativeHandle alberga el SOCKET de Winsock (ADR-0021/0022).
SOCKET to_socket(NativeHandle fd) noexcept {
    return static_cast<SOCKET>(fd);
}

}  // namespace

void Socket::close() noexcept {
    if (fd_ != kInvalidHandle) {
        ::closesocket(to_socket(fd_));
        fd_ = kInvalidHandle;
    }
}

expected<Socket> Socket::connect(std::string_view host, std::uint16_t port) {
    ensure_winsock();
    const SOCKET sock = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCKET) {
        return io_error("socket");
    }
    Socket socket{static_cast<NativeHandle>(sock)};  // RAII: cierra si algo falla a partir de aquí

    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    const std::string host_z{host};  // inet_pton necesita cadena terminada en NUL
    if (::inet_pton(AF_INET, host_z.c_str(), &addr.sin_addr) != 1) {
        return make_error(ErrorCode::InvalidArgument, "host IPv4 inválido: " + host_z);
    }
    // NOLINTNEXTLINE(*-reinterpret-cast): API de sockets (sockaddr).
    if (::connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
        return io_error("connect");
    }
    return socket;
}

void Socket::set_nodelay(bool enabled) const {
    const BOOL value = enabled ? TRUE : FALSE;
    // Mejor esfuerzo: TCP_NODELAY es una optimización de latencia, no es crítico si falla.
    // NOLINTNEXTLINE(*-reinterpret-cast): setsockopt toma const char* en Winsock.
    static_cast<void>(::setsockopt(to_socket(fd_), IPPROTO_TCP, TCP_NODELAY,
                                   reinterpret_cast<const char*>(&value), sizeof(value)));
}

void Listener::close() noexcept {
    if (fd_ != kInvalidHandle) {
        ::closesocket(to_socket(fd_));
        fd_ = kInvalidHandle;
    }
}

expected<Listener> Listener::bind(std::string_view host, std::uint16_t port, int backlog) {
    ensure_winsock();
    const SOCKET sock = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCKET) {
        return io_error("socket");
    }
    Listener listener{static_cast<NativeHandle>(sock)};  // RAII: cierra si algo falla

    const BOOL reuse = TRUE;
    // NOLINTNEXTLINE(*-reinterpret-cast): setsockopt toma const char* en Winsock.
    static_cast<void>(::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
                                   reinterpret_cast<const char*>(&reuse), sizeof(reuse)));

    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host.empty() || host == "0.0.0.0") {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else {
        const std::string host_z{host};  // inet_pton necesita cadena terminada en NUL
        if (::inet_pton(AF_INET, host_z.c_str(), &addr.sin_addr) != 1) {
            return make_error(ErrorCode::InvalidArgument, "host IPv4 inválido: " + host_z);
        }
    }

    // NOLINTNEXTLINE(*-reinterpret-cast): API de sockets (sockaddr).
    if (::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
        return io_error("bind");
    }
    if (::listen(sock, backlog) == SOCKET_ERROR) {
        return io_error("listen");
    }
    return listener;
}

std::uint16_t Listener::local_port() const {
    sockaddr_in addr = {};
    int len = sizeof(addr);
    // NOLINTNEXTLINE(*-reinterpret-cast): API de sockets (sockaddr).
    if (::getsockname(to_socket(fd_), reinterpret_cast<sockaddr*>(&addr), &len) == SOCKET_ERROR) {
        return 0;
    }
    return ntohs(addr.sin_port);
}

#else  // POSIX

namespace {

// Traduce el errno actual a un Error del núcleo, con la operación como contexto.
std::unexpected<Error> io_error(const char* op) {
    return make_error(ErrorCode::IoError,
                      std::string{op} + ": " + std::generic_category().message(errno));
}

}  // namespace

void Socket::close() noexcept {
    if (fd_ != kInvalidHandle) {
        ::close(fd_);
        fd_ = kInvalidHandle;
    }
}

expected<Socket> Socket::connect(std::string_view host, std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return io_error("socket");
    }
    Socket socket{fd};  // toma posesión: cierra el fd si algo falla a partir de aquí (RAII)

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    const std::string host_z{host};  // inet_pton necesita cadena terminada en NUL
    if (::inet_pton(AF_INET, host_z.c_str(), &addr.sin_addr) != 1) {
        return make_error(ErrorCode::InvalidArgument, "host IPv4 inválido: " + host_z);
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): API de sockets POSIX (sockaddr).
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        return io_error("connect");
    }
    return socket;
}

void Socket::set_nodelay(bool enabled) const {
    const int value = enabled ? 1 : 0;
    // Mejor esfuerzo: TCP_NODELAY es una optimización de latencia, no es crítico si falla.
    static_cast<void>(::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)));
}

void Listener::close() noexcept {
    if (fd_ != kInvalidHandle) {
        ::close(fd_);
        fd_ = kInvalidHandle;
    }
}

expected<Listener> Listener::bind(std::string_view host, std::uint16_t port, int backlog) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return io_error("socket");
    }
    Listener listener{fd};  // toma posesión: cierra el fd si algo falla a partir de aquí (RAII)

    const int reuse = 1;
    static_cast<void>(::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host.empty() || host == "0.0.0.0") {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else {
        const std::string host_z{host};  // inet_pton necesita cadena terminada en NUL
        if (::inet_pton(AF_INET, host_z.c_str(), &addr.sin_addr) != 1) {
            return make_error(ErrorCode::InvalidArgument, "host IPv4 inválido: " + host_z);
        }
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): API de sockets POSIX (sockaddr).
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        return io_error("bind");
    }
    if (::listen(fd, backlog) < 0) {
        return io_error("listen");
    }
    return listener;
}

std::uint16_t Listener::local_port() const {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): API de sockets POSIX (sockaddr).
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
        return 0;
    }
    return ntohs(addr.sin_port);
}

#endif  // _WIN32

}  // namespace nexus
