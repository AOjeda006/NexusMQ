// Pruebas de Socket/Listener. Las del envoltorio async usan FakeProactor (deterministas); bind y
// local_port usan sockets reales (sin proactor). Si hay io_uring, un e2e por loopback valida la
// composición accept→recv→send (eco) sobre el backend real.
#include "io/socket.hpp"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <thread>

#include "common/error.hpp"
#include "common/task.hpp"
#include "support/fake_proactor.hpp"

#ifdef NEXUS_HAVE_IOURING
#include "io/io_uring_backend.hpp"
#endif

namespace {

// Arranca @p work y guarda su resultado en @p out (corrutina conductora para los tests).
template <class T>
nexus::task<void> collect(nexus::task<T> work, std::optional<T>& out) {
    out = co_await std::move(work);
}

TEST(Socket, AsyncRecv_Completa_TraduceBytesAExpected) {
    const int raw = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(raw, 0);
    nexus::FakeProactor fake;
    nexus::Socket sock{raw};

    std::array<std::byte, 16> buffer{};
    std::optional<nexus::expected<std::size_t>> result;
    auto driver = collect(sock.async_recv(fake, buffer), result);
    driver.handle().resume();  // arranca → submit_recv (queda pendiente)

    ASSERT_EQ(fake.pending(), 1U);
    EXPECT_EQ(fake.peek(0).kind, nexus::FakeProactor::OpKind::Recv);
    EXPECT_EQ(fake.peek(0).fd, raw);

    fake.arm_front(10);  // recibió 10 bytes
    fake.run_completions(8);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ(**result, 10U);
}

TEST(Socket, AsyncSend_Error_DevuelveIoError) {
    const int raw = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(raw, 0);
    nexus::FakeProactor fake;
    nexus::Socket sock{raw};

    const std::string payload = "hola";
    nexus::ByteSpan data{reinterpret_cast<const std::byte*>(payload.data()), payload.size()};
    std::optional<nexus::expected<std::size_t>> result;
    auto driver = collect(sock.async_send(fake, data), result);
    driver.handle().resume();

    ASSERT_EQ(fake.pending(), 1U);
    EXPECT_EQ(fake.peek(0).kind, nexus::FakeProactor::OpKind::Send);

    fake.arm_front(-ECONNRESET);  // error: errno negado
    fake.run_completions(8);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
    EXPECT_EQ(result->error().code(), nexus::ErrorCode::IoError);
}

TEST(Listener, Bind_PuertoEfimero_AsignaPuertoValido) {
    auto listener = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener.has_value());
    EXPECT_TRUE(listener->is_open());
    EXPECT_GT(listener->local_port(), 0);
}

TEST(Listener, Bind_HostInvalido_DevuelveInvalidArgument) {
    auto listener = nexus::Listener::bind("no-es-una-ip", 0);
    ASSERT_FALSE(listener.has_value());
    EXPECT_EQ(listener.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Socket, Connect_AlListener_Conecta) {
    auto listener = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener.has_value());
    auto sock = nexus::Socket::connect("127.0.0.1", listener->local_port());
    ASSERT_TRUE(sock.has_value());
    EXPECT_TRUE(sock->is_open());
}

TEST(Socket, Connect_HostInvalido_DevuelveInvalidArgument) {
    auto sock = nexus::Socket::connect("no-es-una-ip", 12345);
    ASSERT_FALSE(sock.has_value());
    EXPECT_EQ(sock.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Socket, AsyncConnect_HostInvalido_DevuelveInvalidArgument) {
    nexus::FakeProactor fake;
    std::optional<nexus::expected<nexus::Socket>> result;
    auto driver = collect(nexus::Socket::async_connect(fake, "no-es-una-ip", 1234), result);
    driver.handle().resume();  // falla en inet_pton, antes de suspender: no llega a submit_connect

    EXPECT_EQ(fake.pending(), 0U);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
    EXPECT_EQ(result->error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Socket, AsyncConnect_Completa_DevuelveSocketConectado) {
    nexus::FakeProactor fake;
    std::optional<nexus::expected<nexus::Socket>> result;
    auto driver = collect(nexus::Socket::async_connect(fake, "127.0.0.1", 1234), result);
    driver.handle().resume();  // crea el fd y emite submit_connect (queda pendiente)

    ASSERT_EQ(fake.pending(), 1U);
    EXPECT_EQ(fake.peek(0).kind, nexus::FakeProactor::OpKind::Connect);
    EXPECT_EQ(fake.peek(0).write_buffer.size(), sizeof(sockaddr_in));  // bytes del sockaddr destino

    fake.arm_front(0);  // éxito (result = 0 estilo io_uring)
    fake.run_completions(1);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    EXPECT_TRUE((*result)->is_open());
}

TEST(Socket, AsyncConnect_Error_DevuelveIoError) {
    nexus::FakeProactor fake;
    std::optional<nexus::expected<nexus::Socket>> result;
    auto driver = collect(nexus::Socket::async_connect(fake, "127.0.0.1", 1234), result);
    driver.handle().resume();

    ASSERT_EQ(fake.pending(), 1U);
    fake.arm_front(-ECONNREFUSED);  // error: errno negado
    fake.run_completions(1);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
    EXPECT_EQ(result->error().code(), nexus::ErrorCode::IoError);
}

#ifdef NEXUS_HAVE_IOURING

// Corrutina servidora: acepta una conexión, lee un mensaje y lo devuelve (eco); produce lo leído.
nexus::task<nexus::expected<std::string>> serve_echo(nexus::Proactor& proactor,
                                                     nexus::Listener& listener) {
    nexus::expected<nexus::Socket> client = co_await listener.async_accept(proactor);
    if (!client) {
        co_return std::unexpected(client.error());
    }
    std::array<std::byte, 64> buffer{};
    const nexus::expected<std::size_t> received = co_await client->async_recv(proactor, buffer);
    if (!received) {
        co_return std::unexpected(received.error());
    }
    const nexus::expected<std::size_t> sent =
        co_await client->async_send(proactor, nexus::ByteSpan{buffer.data(), *received});
    if (!sent) {
        co_return std::unexpected(sent.error());
    }
    co_return std::string{reinterpret_cast<const char*>(buffer.data()), *received};
}

TEST(SocketIoUring, AcceptRecvSend_EcoPorLoopback) {
    try {
        (void)nexus::IoUringBackend{8};
    } catch (const std::system_error&) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    auto listener = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener.has_value());
    const std::uint16_t port = listener->local_port();
    ASSERT_GT(port, 0);

    nexus::IoUringBackend proactor{32};
    auto server = serve_echo(proactor, *listener);
    server.handle().resume();  // arranca → accept pendiente

    // Cliente en otro hilo: conecta, envía "hola NexusMQ" y espera el eco (con timeout
    // anti-cuelgue).
    const std::string message = "hola NexusMQ";
    std::string echoed;
    std::thread client([&] {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return;
        }
        timeval timeout{.tv_sec = 5, .tv_usec = 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
            ::send(fd, message.data(), message.size(), 0);
            std::array<char, 64> buf{};
            const ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
            if (n > 0) {
                echoed.assign(buf.data(), static_cast<std::size_t>(n));
            }
        }
        ::close(fd);
    });

    // Bombea las completions del proactor hasta que el servidor termine (con tope de tiempo).
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!server.done() && std::chrono::steady_clock::now() < deadline) {
        proactor.run_completions(16);
    }
    client.join();

    ASSERT_TRUE(server.done());
    const nexus::expected<std::string> result = server.handle().promise().result();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, message);
    EXPECT_EQ(echoed, message);
}

// Cliente: conecta con Socket::async_connect, envía @p message y devuelve el eco del servidor.
nexus::task<nexus::expected<std::string>> connect_and_echo(nexus::Proactor& proactor,
                                                           std::uint16_t port,
                                                           std::string message) {
    nexus::expected<nexus::Socket> sock =
        co_await nexus::Socket::async_connect(proactor, "127.0.0.1", port);
    if (!sock) {
        co_return std::unexpected(sock.error());
    }
    const nexus::ByteSpan data{reinterpret_cast<const std::byte*>(message.data()), message.size()};
    const nexus::expected<std::size_t> sent = co_await sock->async_send(proactor, data);
    if (!sent) {
        co_return std::unexpected(sent.error());
    }
    std::array<std::byte, 64> buffer{};
    const nexus::expected<std::size_t> received = co_await sock->async_recv(proactor, buffer);
    if (!received) {
        co_return std::unexpected(received.error());
    }
    co_return std::string{reinterpret_cast<const char*>(buffer.data()), *received};
}

TEST(SocketIoUring, AsyncConnect_RoundTripPorLoopback) {
    try {
        (void)nexus::IoUringBackend{8};
    } catch (const std::system_error&) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    auto listener = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener.has_value());
    const std::uint16_t port = listener->local_port();
    ASSERT_GT(port, 0);

    // Servidor (eco) y cliente (async_connect) sobre el MISMO proactor/hilo: prueba determinista de
    // la composición connect→send→recv sin segundo hilo.
    nexus::IoUringBackend proactor{32};
    auto server = serve_echo(proactor, *listener);
    server.handle().resume();  // accept pendiente
    const std::string message = "ping inter-nodo";
    auto client = connect_and_echo(proactor, port, message);
    client.handle().resume();  // connect pendiente

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while ((!server.done() || !client.done()) && std::chrono::steady_clock::now() < deadline) {
        proactor.run_completions(16);
    }

    ASSERT_TRUE(server.done());
    ASSERT_TRUE(client.done());
    const nexus::expected<std::string> server_result = server.handle().promise().result();
    const nexus::expected<std::string> client_result = client.handle().promise().result();
    ASSERT_TRUE(server_result.has_value());
    ASSERT_TRUE(client_result.has_value());
    EXPECT_EQ(*server_result, message);
    EXPECT_EQ(*client_result, message);
}

#endif  // NEXUS_HAVE_IOURING

}  // namespace
