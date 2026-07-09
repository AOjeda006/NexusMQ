// parse_daemon_args: parseo de los argumentos de línea de comandos de `nexusd` (ver
// `server/daemon_args.hpp`). Cubre los flags de TLS del plano de datos (--tls-cert/--tls-key,
// P5a), una muestra de los flags existentes (caracterización) y el rechazo de entradas inválidas.
#include "server/daemon_args.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <initializer_list>
#include <span>
#include <string>
#include <vector>

#include "server/server.hpp"

namespace {

// Construye un argv mutable (char*) a partir de literales, como el que recibe `main`.
class Argv {
public:
    Argv(std::initializer_list<std::string> args) : storage_(args) {
        pointers_.reserve(storage_.size());
        for (std::string& arg : storage_) {
            pointers_.push_back(arg.data());
        }
    }
    [[nodiscard]] std::span<char* const> span() const noexcept { return pointers_; }

private:
    std::vector<std::string> storage_;
    std::vector<char*> pointers_;
};

TEST(DaemonArgs, TlsCertYKey_HabilitanTlsDelPlanoDeDatos) {
    const Argv argv{"nexusd", "--tls-cert", "/certs/server.pem", "--tls-key", "/certs/server.key"};
    nexus::Server::Config config;
    std::vector<nexus::DaemonTopicSpec> topics;

    ASSERT_TRUE(nexus::parse_daemon_args(argv.span(), config, topics));
    EXPECT_EQ(config.tls.cert_chain, "/certs/server.pem");
    EXPECT_EQ(config.tls.private_key, "/certs/server.key");
    EXPECT_TRUE(
        config.tls.enabled());  // con cert y clave, el plano de datos termina TLS (ADR-0019).
}

TEST(DaemonArgs, SinFlagsTls_NoHabilitaTls) {
    const Argv argv{"nexusd", "--port", "9092"};
    nexus::Server::Config config;
    std::vector<nexus::DaemonTopicSpec> topics;

    ASSERT_TRUE(nexus::parse_daemon_args(argv.span(), config, topics));
    EXPECT_FALSE(config.tls.enabled());
}

TEST(DaemonArgs, FlagsExistentes_SeParsean) {
    const Argv argv{"nexusd",   "--port",    "7000", "--admin-port", "7644",     "--host",
                    "10.0.0.1", "--node-id", "3",    "--topic",      "eventos:4"};
    nexus::Server::Config config;
    std::vector<nexus::DaemonTopicSpec> topics;

    ASSERT_TRUE(nexus::parse_daemon_args(argv.span(), config, topics));
    EXPECT_EQ(config.port, 7000);
    EXPECT_EQ(config.admin_port, 7644);
    EXPECT_EQ(config.host, "10.0.0.1");
    EXPECT_EQ(config.advertised_host, "10.0.0.1");
    EXPECT_EQ(config.node_id, 3);
    ASSERT_EQ(topics.size(), 1U);
    EXPECT_EQ(topics.front().first, "eventos");
    EXPECT_EQ(topics.front().second, 4);
}

TEST(DaemonArgs, ArgumentoDesconocido_Rechaza) {
    const Argv argv{"nexusd", "--no-existe"};
    nexus::Server::Config config;
    std::vector<nexus::DaemonTopicSpec> topics;

    EXPECT_FALSE(nexus::parse_daemon_args(argv.span(), config, topics));
}

TEST(DaemonArgs, FlagSinValor_Rechaza) {
    const Argv argv{"nexusd", "--tls-cert"};  // falta el valor.
    nexus::Server::Config config;
    std::vector<nexus::DaemonTopicSpec> topics;

    EXPECT_FALSE(nexus::parse_daemon_args(argv.span(), config, topics));
}

}  // namespace
