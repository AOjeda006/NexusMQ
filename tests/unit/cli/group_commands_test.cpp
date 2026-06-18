// Subcomandos `group`/`partitions`/`metrics`/`diagnostics` del CLI con un doble de AdminClient.
#include "cli/group_commands.hpp"

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "cli/admin_client.hpp"
#include "cli/admin_commands.hpp"
#include "cli/partition_commands.hpp"
#include "common/error.hpp"

namespace {

// Doble de AdminClient con respuestas configurables por ruta.
class FakeClient : public nexus::cli::AdminClient {
public:
    std::string last_path;
    nexus::cli::ClientResponse on_groups{.status = 200, .body = R"({"items":[]})"};
    nexus::cli::ClientResponse on_topic{.status = 200, .body = "{}"};
    nexus::cli::ClientResponse on_metrics{.status = 200, .body = "nexus_x 1\n"};
    nexus::cli::ClientResponse on_healthz{.status = 200, .body = R"({"status":"ok"})"};
    nexus::cli::ClientResponse on_readyz{.status = 200, .body = R"({"status":"ready"})"};

    nexus::expected<nexus::cli::ClientResponse> get(std::string_view path) override {
        last_path = std::string{path};
        if (path == "/api/v1/groups") {
            return on_groups;
        }
        if (path == "/metrics") {
            return on_metrics;
        }
        if (path == "/healthz") {
            return on_healthz;
        }
        if (path == "/readyz") {
            return on_readyz;
        }
        return on_topic;  // /api/v1/topics/<name>
    }
    nexus::expected<nexus::cli::ClientResponse> post(std::string_view, std::string_view) override {
        return nexus::make_error(nexus::ErrorCode::Unsupported, "no");
    }
    nexus::expected<nexus::cli::ClientResponse> del(std::string_view) override {
        return nexus::make_error(nexus::ErrorCode::Unsupported, "no");
    }
};

std::vector<std::string_view> args_of(std::initializer_list<std::string_view> items) {
    return {items};
}

TEST(GroupCommands, List_ImprimeTabla) {
    FakeClient client;
    client.on_groups = {
        .status = 200,
        .body = R"({"items":[{"groupId":"g1","state":"Stable","generation":2,"memberCount":3}]})"};
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(nexus::cli::run_group(client, args_of({"list"}), out, err), 0);
    EXPECT_EQ(client.last_path, "/api/v1/groups");
    EXPECT_NE(out.str().find("g1"), std::string::npos);
    EXPECT_NE(out.str().find("Stable"), std::string::npos);
}

TEST(GroupCommands, Describe_Encontrado) {
    FakeClient client;
    client.on_groups = {
        .status = 200,
        .body = R"({"items":[{"groupId":"g1","state":"Stable","generation":2,"memberCount":3}]})"};
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(nexus::cli::run_group(client, args_of({"describe", "g1"}), out, err), 0);
    EXPECT_NE(out.str().find("members:"), std::string::npos);
}

TEST(GroupCommands, Describe_NoEncontrado_Exit1) {
    FakeClient client;
    client.on_groups = {.status = 200, .body = R"({"items":[]})"};
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(nexus::cli::run_group(client, args_of({"describe", "fantasma"}), out, err), 1);
    EXPECT_NE(err.str().find("no encontrado"), std::string::npos);
}

TEST(PartitionCommands, ListaParticionesDeTopic) {
    FakeClient client;
    client.on_topic = {
        .status = 200,
        .body =
            R"({"name":"orders","partitions":[{"id":0,"leader":1,"highWatermark":5,"leaderEpoch":0}]})"};
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(nexus::cli::run_partitions(client, args_of({"orders"}), out, err), 0);
    EXPECT_EQ(client.last_path, "/api/v1/topics/orders");
    EXPECT_NE(out.str().find("HIGH-WATERMARK"), std::string::npos);
}

TEST(PartitionCommands, SinTopic_Exit1) {
    FakeClient client;
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(nexus::cli::run_partitions(client, args_of({}), out, err), 1);
}

TEST(AdminCommands, Metrics_VuelcaBody) {
    FakeClient client;
    client.on_metrics = {.status = 200, .body = "nexus_requests_total 42\n"};
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(nexus::cli::run_metrics(client, out, err), 0);
    EXPECT_NE(out.str().find("nexus_requests_total 42"), std::string::npos);
}

TEST(AdminCommands, Diagnostics_VivoYListo_Exit0) {
    FakeClient client;
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(nexus::cli::run_diagnostics(client, out, err), 0);
    EXPECT_NE(out.str().find("liveness:"), std::string::npos);
    EXPECT_NE(out.str().find("readiness:"), std::string::npos);
}

TEST(AdminCommands, Diagnostics_NoListo_Exit1) {
    FakeClient client;
    client.on_readyz = {.status = 503, .body = R"({"status":"not_ready"})"};
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(nexus::cli::run_diagnostics(client, out, err), 1);
}

}  // namespace
