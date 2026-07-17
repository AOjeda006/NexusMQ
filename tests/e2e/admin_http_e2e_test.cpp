// E2E del puerto de operación: un Server real (reactor io_uring) sirviendo el plano de admin sobre
// HTTP/1.1. Un cliente crudo (Socket::connect + ::send/::recv) ejerce /healthz, /readyz, /metrics y
// el REST admin (/api/v1/topics), cerrando el lazo HTTP↔red↔dominio que las pruebas unitarias del
// AdminRouter no cubren.
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/time.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

#include "io/socket.hpp"
#include "server/server.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_admin_e2e_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

bool send_all(int fd, std::string_view data) {
    const char* ptr = data.data();
    std::size_t left = data.size();
    while (left > 0) {
        const ssize_t sent = ::send(fd, ptr, left, MSG_NOSIGNAL);
        if (sent <= 0) {
            return false;
        }
        ptr += sent;
        left -= static_cast<std::size_t>(sent);
    }
    return true;
}

// Hace una petición HTTP cruda al puerto de admin y devuelve la respuesta completa (hasta el cierre
// del par, que responde con `Connection: close`). Vacío si la conexión falla.
std::string http_request(std::uint16_t port, std::string_view method, std::string_view target,
                         std::string_view body = {}) {
    nexus::expected<nexus::Socket> client = nexus::Socket::connect("127.0.0.1", port);
    if (!client) {
        return {};
    }
    std::string request{method};
    request += " ";
    request += target;
    request += " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n";
    if (!body.empty()) {
        request += "Content-Type: application/json\r\nContent-Length: ";
        request += std::to_string(body.size());
        request += "\r\n";
    }
    request += "\r\n";
    request += body;
    if (!send_all(client->fd(), request)) {
        return {};
    }

    std::string response;
    std::array<char, 4096> chunk{};
    while (true) {
        const ssize_t got = ::recv(client->fd(), chunk.data(), chunk.size(), 0);
        if (got <= 0) {
            break;
        }
        response.append(chunk.data(), static_cast<std::size_t>(got));
    }
    return response;
}

// Abre el stream SSE y lee hasta capturar cabeceras + el primer frame (que se emite de inmediato,
// antes de la primera espera de cadencia). Cierra al salir (RAII) → el server termina ese stream.
std::string sse_probe(std::uint16_t port) {
    nexus::expected<nexus::Socket> client = nexus::Socket::connect("127.0.0.1", port);
    if (!client) {
        return {};
    }
    const timeval tv{.tv_sec = 3, .tv_usec = 0};  // no bloquear indefinidamente si algo va mal.
    ::setsockopt(client->fd(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    if (!send_all(client->fd(), "GET /api/v1/stream HTTP/1.1\r\nHost: localhost\r\n\r\n")) {
        return {};
    }
    std::string response;
    std::array<char, 4096> chunk{};
    for (int i = 0; i < 64; ++i) {
        const ssize_t got = ::recv(client->fd(), chunk.data(), chunk.size(), 0);
        if (got <= 0) {
            break;
        }
        response.append(chunk.data(), static_cast<std::size_t>(got));
        const std::size_t data_pos = response.find("data: ");
        if (data_pos != std::string::npos && response.find("\n\n", data_pos) != std::string::npos) {
            break;  // ya tenemos un frame completo.
        }
    }
    return response;
}

TEST(AdminHttpE2E, PuertoDeOperacion_HealthMetricsYRest) {
    TempDir dir{"ops"};
    nexus::Server::Config config;
    config.host = "127.0.0.1";
    config.port = 0;
    config.admin_port = 0;  // efímero
    config.data_dir = dir.path();

    std::optional<nexus::Server> server;
    try {
        server.emplace(std::move(config));
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }

    ASSERT_TRUE(server->create_topic("orders", 2).has_value());
    ASSERT_TRUE(server->bind().has_value());
    const std::uint16_t admin = server->admin_port();
    ASSERT_NE(admin, 0);

    std::thread server_thread{[&server] { server->run(); }};

    // --- liveness ---
    const std::string healthz = http_request(admin, "GET", "/healthz");
    EXPECT_NE(healthz.find("200"), std::string::npos);
    EXPECT_NE(healthz.find(R"("status":"ok")"), std::string::npos);

    // --- readiness (arrancado, sin probes) ---
    const std::string readyz = http_request(admin, "GET", "/readyz");
    EXPECT_NE(readyz.find("200"), std::string::npos);
    EXPECT_NE(readyz.find(R"("status":"ready")"), std::string::npos);

    // --- métricas Prometheus ---
    const std::string metrics = http_request(admin, "GET", "/metrics");
    EXPECT_NE(metrics.find("200"), std::string::npos);
    EXPECT_NE(metrics.find("version=0.0.4"), std::string::npos);

    // --- REST admin: el topic creado aparece en el listado ---
    const std::string topics = http_request(admin, "GET", "/api/v1/topics");
    EXPECT_NE(topics.find("200"), std::string::npos);
    EXPECT_NE(topics.find(R"("name":"orders")"), std::string::npos);

    // --- REST admin: crear un topic vía POST y verlo después ---
    const std::string created =
        http_request(admin, "POST", "/api/v1/topics", R"({"name":"events","partitionCount":1})");
    EXPECT_NE(created.find("201"), std::string::npos);
    const std::string topics2 = http_request(admin, "GET", "/api/v1/topics");
    EXPECT_NE(topics2.find(R"("name":"events")"), std::string::npos);

    // --- ruta desconocida → 404 ---
    const std::string unknown = http_request(admin, "GET", "/api/v1/desconocido");
    EXPECT_NE(unknown.find("404"), std::string::npos);

    // --- stream SSE: cabeceras text/event-stream (sin Content-Length) + un frame de métricas ---
    const std::string stream = sse_probe(admin);
    EXPECT_NE(stream.find("200"), std::string::npos);
    EXPECT_NE(stream.find("text/event-stream"), std::string::npos);
    EXPECT_EQ(stream.find("Content-Length"), std::string::npos);  // stream: sin Content-Length.
    EXPECT_NE(stream.find("event: metrics"), std::string::npos);
    EXPECT_NE(stream.find("data: {"), std::string::npos);  // payload JSON del snapshot.

    server->stop();
    server_thread.join();
}

TEST(AdminHttpE2E, ConexionesActivas_ReflejaConexionNativaAbierta) {
    TempDir dir{"conns"};
    nexus::Server::Config config;
    config.host = "127.0.0.1";
    config.port = 0;        // plano de datos nativo, efímero.
    config.admin_port = 0;  // plano de operación, efímero.
    config.data_dir = dir.path();

    std::optional<nexus::Server> server;
    try {
        server.emplace(std::move(config));
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }
    ASSERT_TRUE(server->bind().has_value());
    const std::uint16_t data_port = server->port();
    const std::uint16_t admin = server->admin_port();
    ASSERT_NE(data_port, 0);
    ASSERT_NE(admin, 0);
    std::thread server_thread{[&server] { server->run(); }};

    // Abre una conexión al plano de datos nativo y la mantiene viva (sin enviar nada: el servicio
    // se suspende leyendo la trama). El `GaugeGuard` de la conexión sube el gauge del plano
    // `native`.
    nexus::expected<nexus::Socket> conn = nexus::Socket::connect("127.0.0.1", data_port);
    ASSERT_TRUE(conn.has_value());
    std::optional<nexus::Socket> native_conn{std::move(*conn)};  // mantiene el socket vivo (RAII).

    // El *accept* + *spawn* del servicio es asíncrono: sondeamos /metrics hasta ver la conexión
    // nativa reflejada (o agotar el plazo). Determinista en la práctica: una sola conexión → 1.
    bool native_seen = false;
    for (int attempt = 0; attempt < 100 && !native_seen; ++attempt) {
        const std::string metrics = http_request(admin, "GET", "/metrics");
        if (metrics.find(R"(nexus_broker_connections_active{plane="native"} 1)") !=
            std::string::npos) {
            native_seen = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(native_seen);  // la conexión nativa abierta se cuenta en el plano `native`.

    // El propio scrape de /metrics es una conexión del plano admin: su gauge existe y es >= 1.
    const std::string metrics = http_request(admin, "GET", "/metrics");
    EXPECT_NE(metrics.find(R"(# TYPE nexus_broker_connections_active gauge)"), std::string::npos);
    EXPECT_NE(metrics.find(R"(nexus_broker_connections_active{plane="admin"})"), std::string::npos);

    native_conn.reset();  // cierra la conexión nativa (RAII); el servicio termina y el gauge baja.
    server->stop();
    server_thread.join();
}

}  // namespace
