// E2E de TLS por loopback: dos TlsConnection (servidor y cliente) sobre el mismo reactor io_uring
// completan el handshake, intercambian datos cifrados y, en mTLS, el servidor lee el principal del
// cliente. Cierra el lazo OpenSSL↔BIO de memoria↔Proactor que las unitarias de TlsContext no
// cubren.
#include <gtest/gtest.h>

#include "ingress/tls.hpp"

#if defined(NEXUS_HAVE_OPENSSL) && defined(NEXUS_HAVE_IOURING)

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "common/error.hpp"
#include "common/task.hpp"
#include "io/io_uring_backend.hpp"
#include "io/socket.hpp"
#include "support/test_certs.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_tls_e2e_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] std::filesystem::path file(const char* name) const { return path_ / name; }

private:
    std::filesystem::path path_;
};

// Corrutina conductora: ejecuta @p work y guarda su resultado en @p out.
template <class T>
nexus::task<void> collect(nexus::task<T> work, std::optional<T>& out) {
    out = co_await std::move(work);
}

// Arranca cada conductor y bombea el reactor hasta que todos terminen (o venza el plazo).
bool drive(nexus::IoUringBackend& proactor, std::vector<nexus::task<void>*> drivers) {
    for (auto* driver : drivers) {
        driver->handle().resume();
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        bool all_done = true;
        for (auto* driver : drivers) {
            if (!driver->done()) {
                all_done = false;
            }
        }
        if (all_done) {
            return true;
        }
        proactor.run_completions(16);
    }
    return false;
}

nexus::ByteSpan as_bytes(std::string_view text) {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

// Acepta por loopback la conexión ya iniciada con Socket::connect, sobre @p proactor.
std::optional<nexus::Socket> accept_one(nexus::IoUringBackend& proactor,
                                        const nexus::Listener& listener) {
    std::optional<nexus::expected<nexus::Socket>> accepted;
    auto driver = collect(listener.async_accept(proactor), accepted);
    if (!drive(proactor, {&driver}) || !accepted.has_value() || !accepted->has_value()) {
        return std::nullopt;
    }
    return std::move(accepted->value());
}

// Establece un par de sockets conectados por loopback: devuelve {servidor, cliente} o nullopt.
struct SocketPair {
    nexus::Socket server;
    nexus::Socket client;
};

std::optional<SocketPair> loopback_pair(nexus::IoUringBackend& proactor,
                                        const nexus::Listener& listener) {
    auto client = nexus::Socket::connect("127.0.0.1", listener.local_port());
    if (!client) {
        return std::nullopt;
    }
    auto server = accept_one(proactor, listener);
    if (!server) {
        return std::nullopt;
    }
    return SocketPair{.server = std::move(*server), .client = std::move(*client)};
}

bool iouring_available() {
    try {
        (void)nexus::IoUringBackend{8};
        return true;
    } catch (const std::system_error&) {
        return false;
    }
}

TEST(TlsE2E, Handshake_UnaVia_IntercambiaDatosCifrados) {
    if (!iouring_available()) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    TempDir dir{"oneway"};
    const auto cert = dir.file("cert.pem");
    const auto key = dir.file("key.pem");
    ASSERT_TRUE(nexus::testing::write_self_signed(cert, key, "localhost"));

    auto listener = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener.has_value());

    nexus::IoUringBackend proactor{64};
    auto pair = loopback_pair(proactor, *listener);
    ASSERT_TRUE(pair.has_value());

    const auto srv_ctx = nexus::TlsContext::server(cert, key);
    ASSERT_TRUE(srv_ctx.has_value()) << srv_ctx.error().message();
    const auto cli_ctx = nexus::TlsContext::client();  // sin CA: acepta el cert autofirmado
    ASSERT_TRUE(cli_ctx.has_value()) << cli_ctx.error().message();

    auto srv_conn = srv_ctx->accept(std::move(pair->server));
    ASSERT_TRUE(srv_conn.has_value());
    auto cli_conn = cli_ctx->connect(std::move(pair->client));
    ASSERT_TRUE(cli_conn.has_value());

    // Handshake simultáneo en el mismo reactor.
    std::optional<nexus::expected<void>> srv_hs;
    std::optional<nexus::expected<void>> cli_hs;
    auto t_srv = collect(srv_conn->handshake(proactor), srv_hs);
    auto t_cli = collect(cli_conn->handshake(proactor), cli_hs);
    ASSERT_TRUE(drive(proactor, {&t_srv, &t_cli}));
    ASSERT_TRUE(srv_hs.has_value() && srv_hs->has_value()) << srv_hs->error().message();
    ASSERT_TRUE(cli_hs.has_value() && cli_hs->has_value()) << cli_hs->error().message();

    // cliente → servidor.
    {
        std::array<std::byte, 64> buffer{};
        std::optional<nexus::expected<std::size_t>> sent;
        std::optional<nexus::expected<std::size_t>> received;
        auto t1 = collect(cli_conn->async_send(proactor, as_bytes("ping")), sent);
        auto t2 = collect(srv_conn->async_recv(proactor, buffer), received);
        ASSERT_TRUE(drive(proactor, {&t1, &t2}));
        ASSERT_TRUE(received.has_value() && received->has_value());
        EXPECT_EQ(**received, 4U);
        EXPECT_EQ((std::string_view{reinterpret_cast<const char*>(buffer.data()), 4}), "ping");
    }

    // servidor → cliente.
    {
        std::array<std::byte, 64> buffer{};
        std::optional<nexus::expected<std::size_t>> sent;
        std::optional<nexus::expected<std::size_t>> received;
        auto t1 = collect(srv_conn->async_send(proactor, as_bytes("pong")), sent);
        auto t2 = collect(cli_conn->async_recv(proactor, buffer), received);
        ASSERT_TRUE(drive(proactor, {&t1, &t2}));
        ASSERT_TRUE(received.has_value() && received->has_value());
        EXPECT_EQ(**received, 4U);
        EXPECT_EQ((std::string_view{reinterpret_cast<const char*>(buffer.data()), 4}), "pong");
    }
}

TEST(TlsE2E, MutualTls_ServidorVePrincipalDelCliente) {
    if (!iouring_available()) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    TempDir dir{"mtls"};
    const auto cert = dir.file("cert.pem");
    const auto key = dir.file("key.pem");
    ASSERT_TRUE(nexus::testing::write_self_signed(cert, key, "nexus-client"));

    auto listener = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener.has_value());

    nexus::IoUringBackend proactor{64};
    auto pair = loopback_pair(proactor, *listener);
    ASSERT_TRUE(pair.has_value());

    // El mismo par cert/clave actúa de identidad de ambos extremos y de CA mutua.
    const auto srv_ctx = nexus::TlsContext::server(cert, key, cert);
    ASSERT_TRUE(srv_ctx.has_value()) << srv_ctx.error().message();
    EXPECT_TRUE(srv_ctx->require_client_cert());
    const auto cli_ctx = nexus::TlsContext::client(cert, cert, key);
    ASSERT_TRUE(cli_ctx.has_value()) << cli_ctx.error().message();

    auto srv_conn = srv_ctx->accept(std::move(pair->server));
    ASSERT_TRUE(srv_conn.has_value());
    auto cli_conn = cli_ctx->connect(std::move(pair->client));
    ASSERT_TRUE(cli_conn.has_value());

    std::optional<nexus::expected<void>> srv_hs;
    std::optional<nexus::expected<void>> cli_hs;
    auto t_srv = collect(srv_conn->handshake(proactor), srv_hs);
    auto t_cli = collect(cli_conn->handshake(proactor), cli_hs);
    ASSERT_TRUE(drive(proactor, {&t_srv, &t_cli}));
    ASSERT_TRUE(srv_hs.has_value() && srv_hs->has_value()) << srv_hs->error().message();
    ASSERT_TRUE(cli_hs.has_value() && cli_hs->has_value()) << cli_hs->error().message();

    const std::optional<std::string> principal = srv_conn->peer_principal();
    ASSERT_TRUE(principal.has_value());
    EXPECT_EQ(*principal, "nexus-client");
}

TEST(TlsE2E, Verificacion_CaQueNoCasa_HandshakeFalla) {
    if (!iouring_available()) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    TempDir dir{"verify"};
    const auto server_cert = dir.file("server.pem");
    const auto server_key = dir.file("server.key");
    const auto other_cert = dir.file("other.pem");
    const auto other_key = dir.file("other.key");
    ASSERT_TRUE(nexus::testing::write_self_signed(server_cert, server_key, "localhost"));
    ASSERT_TRUE(nexus::testing::write_self_signed(other_cert, other_key, "impostor"));

    auto listener = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener.has_value());

    nexus::IoUringBackend proactor{64};
    auto pair = loopback_pair(proactor, *listener);
    ASSERT_TRUE(pair.has_value());

    const auto srv_ctx = nexus::TlsContext::server(server_cert, server_key);
    ASSERT_TRUE(srv_ctx.has_value());
    // El cliente exige verificar contra una CA ajena: el cert del servidor no validará.
    const auto cli_ctx = nexus::TlsContext::client(other_cert);
    ASSERT_TRUE(cli_ctx.has_value());

    auto srv_conn = srv_ctx->accept(std::move(pair->server));
    ASSERT_TRUE(srv_conn.has_value());
    auto cli_conn = cli_ctx->connect(std::move(pair->client));
    ASSERT_TRUE(cli_conn.has_value());

    std::optional<nexus::expected<void>> srv_hs;
    std::optional<nexus::expected<void>> cli_hs;
    auto t_srv = collect(srv_conn->handshake(proactor), srv_hs);
    auto t_cli = collect(cli_conn->handshake(proactor), cli_hs);
    ASSERT_TRUE(drive(proactor, {&t_srv, &t_cli}));
    ASSERT_TRUE(cli_hs.has_value());
    EXPECT_FALSE(cli_hs->has_value());  // el cliente aborta por fallo de verificación
}

}  // namespace

#else

TEST(TlsE2E, OmitidoSinOpenSSLoIoUring) {
    GTEST_SKIP() << "compilado sin OpenSSL o sin io_uring";
}

#endif  // NEXUS_HAVE_OPENSSL && NEXUS_HAVE_IOURING
