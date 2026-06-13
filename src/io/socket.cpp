/// @file   io/socket.cpp
/// @brief  Implementación de Socket/Listener (RAII + E/S asíncrona sobre el Proactor).
/// @ingroup io

#include "io/socket.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <string>
#include <system_error>
#include <utility>

#include "io/awaitable.hpp"

namespace nexus {

namespace {

// Traduce el errno actual a un Error del núcleo, con la operación como contexto.
std::unexpected<Error> io_error(const char* op) {
    return make_error(ErrorCode::IoError,
                      std::string{op} + ": " + std::generic_category().message(errno));
}

}  // namespace

Socket::~Socket() {
    close();
}

Socket::Socket(Socket&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

void Socket::close() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void Socket::set_nodelay(bool enabled) const {
    const int value = enabled ? 1 : 0;
    // Mejor esfuerzo: TCP_NODELAY es una optimización de latencia, no es crítico si falla.
    static_cast<void>(::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)));
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

Listener::Listener(Listener&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

Listener& Listener::operator=(Listener&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

void Listener::close() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
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

task<expected<Socket>> Listener::async_accept(Proactor& proactor) const {
    const expected<int> accepted = co_await AcceptAwaitable{proactor, fd_};
    if (!accepted) {
        co_return std::unexpected(accepted.error());
    }
    co_return Socket{*accepted};
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

}  // namespace nexus
