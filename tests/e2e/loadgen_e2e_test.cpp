// E2E del generador de carga: nexus-loadgen (LoadGenerator + ProduceRunner) contra un Server real
// en su propio hilo. Comprueba que la campaña open-loop completa sin errores y que las peticiones
// llegan de verdad al broker por la red (el high-watermark avanza al total publicado).
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "client/client.hpp"
#include "client/consumer.hpp"
#include "client/endpoint.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "load_generator.hpp"
#include "loadgen_config.hpp"
#include "loadgen_report.hpp"
#include "produce_runner.hpp"
#include "request_runner.hpp"
#include "server/server.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_loadgen_e2e_" + std::string{tag} + "_" +
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

// Levanta un Server en su propio hilo; RAII para bind/run/stop/join. GTEST_SKIP si no hay io_uring.
class ServerFixture {
public:
    explicit ServerFixture(const std::filesystem::path& data_dir) {
        nexus::Server::Config config;
        config.host = "127.0.0.1";
        config.port = 0;  // efímero
        config.data_dir = data_dir;
        config.advertised_host = "127.0.0.1";
        server_.emplace(std::move(config));
    }
    ~ServerFixture() {
        if (running_) {
            server_->stop();
            thread_.join();
        }
    }
    ServerFixture(const ServerFixture&) = delete;
    ServerFixture& operator=(const ServerFixture&) = delete;

    [[nodiscard]] bool start() {
        if (!server_->bind()) {
            return false;
        }
        port_ = server_->port();
        thread_ = std::thread{[this] { server_->run(); }};
        running_ = true;
        return port_ != 0;
    }

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

private:
    std::optional<nexus::Server> server_;
    std::thread thread_;
    std::uint16_t port_ = 0;
    bool running_ = false;
};

// Cuenta los records de la partición 0 leyendo hasta agotar (verifica que la carga llegó al log).
std::int64_t count_records(nexus::Client& client, const std::string& topic) {
    nexus::Consumer consumer = client.consumer(topic, 0);
    std::int64_t total = 0;
    for (;;) {
        nexus::expected<std::vector<nexus::ConsumedRecord>> batch = consumer.poll();
        if (!batch || batch->empty()) {
            break;
        }
        total += static_cast<std::int64_t>(batch->size());
    }
    return total;
}

TEST(LoadGenE2E, ProduceOpenLoop_CompletaSinErroresYEscribeEnElBroker) {
    TempDir dir{"produce"};
    std::optional<ServerFixture> fixture;
    try {
        fixture.emplace(dir.path());
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }
    ASSERT_TRUE(fixture->start());

    const nexus::Endpoint endpoint{.host = "127.0.0.1", .port = fixture->port()};
    const std::string topic = "loadgen-e2e";

    // Crea el topic antes de la campaña.
    nexus::expected<nexus::Client> admin = nexus::Client::connect(endpoint);
    ASSERT_TRUE(admin.has_value());
    ASSERT_TRUE(admin->create_topic(topic, 1).has_value());

    // Campaña pequeña, sin ritmo (rápida y determinista en recuento), 2 conexiones.
    nexus::loadgen::LoadGenConfig config;
    config.op_count = 200;
    config.warmup_ops = 20;
    config.connections = 2;
    config.target_rate = 0.0;
    config.payload_size = 64;

    nexus::loadgen::RunnerFactory factory =
        [endpoint, topic, payload = config.payload_size](
            int /*worker_id*/) -> nexus::expected<std::unique_ptr<nexus::loadgen::RequestRunner>> {
        nexus::expected<std::unique_ptr<nexus::loadgen::ProduceRunner>> runner =
            nexus::loadgen::ProduceRunner::create(endpoint, topic, 0, payload);
        if (!runner) {
            return std::unexpected<nexus::Error>(runner.error());
        }
        return std::unique_ptr<nexus::loadgen::RequestRunner>{std::move(*runner)};
    };

    nexus::loadgen::LoadGenerator generator{config, std::move(factory)};
    nexus::expected<nexus::loadgen::LoadGenReport> report = generator.run();
    ASSERT_TRUE(report.has_value());
    EXPECT_EQ(report->recorded, 200U);
    EXPECT_EQ(report->errors, 0U);
    EXPECT_GT(report->p50_ns, 0U);  // hubo latencia real medida end-to-end

    // El total publicado (calentamiento + medidas) debe estar en el log del broker.
    const std::int64_t written = count_records(*admin, topic);
    EXPECT_EQ(written, static_cast<std::int64_t>(config.op_count + config.warmup_ops));
    admin->close();
}

}  // namespace
