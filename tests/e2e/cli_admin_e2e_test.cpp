// E2E del CLI: el HttpAdminClient real (sockets bloqueantes) contra un Server real con puerto de
// operación. Cierra el lazo CLI→HTTP→REST admin→broker que el doble de AdminClient no cubre.
#include <gtest/gtest.h>

#include <filesystem>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include "cli/http_admin_client.hpp"
#include "cli/topic_commands.hpp"
#include "server/server.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_cli_e2e_" + std::string{tag} + "_" +
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

TEST(CliAdminE2E, HttpClientContraServerReal) {
    TempDir dir{"http"};
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

    nexus::cli::HttpAdminClient client{nexus::cli::HttpAdminClient::Options{
        .host = "127.0.0.1", .port = admin, .bearer_token = ""}};

    // Transporte directo: /healthz responde 200.
    const auto health = client.get("/healthz");
    ASSERT_TRUE(health.has_value());
    EXPECT_EQ(health->status, 200);

    // `topic list` ve el topic creado.
    {
        std::ostringstream out;
        std::ostringstream err;
        const std::vector<std::string_view> args{"list"};
        EXPECT_EQ(nexus::cli::run_topic(client, args, out, err), 0);
        EXPECT_NE(out.str().find("orders"), std::string::npos);
    }

    // `topic create` y luego aparece en el listado.
    {
        std::ostringstream out;
        std::ostringstream err;
        const std::vector<std::string_view> args{"create", "events", "-p", "1"};
        EXPECT_EQ(nexus::cli::run_topic(client, args, out, err), 0) << err.str();
    }
    {
        std::ostringstream out;
        std::ostringstream err;
        const std::vector<std::string_view> args{"list"};
        EXPECT_EQ(nexus::cli::run_topic(client, args, out, err), 0);
        EXPECT_NE(out.str().find("events"), std::string::npos);
    }

    server->stop();
    server_thread.join();
}

}  // namespace
