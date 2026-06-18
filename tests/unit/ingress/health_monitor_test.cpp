// HealthMonitor (§7.6): liveness (/healthz) y readiness (/readyz) con probes inyectados.
#include "ingress/health_monitor.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <limits>
#include <string>

namespace {

TEST(HealthMonitor, Liveness_Vivo_200Ok) {
    const nexus::HealthMonitor monitor;
    const auto response = monitor.liveness();
    EXPECT_EQ(response.status, 200);
    EXPECT_NE(response.body.find(R"("status":"ok")"), std::string::npos);
}

TEST(HealthMonitor, Liveness_Drenando_503) {
    nexus::HealthMonitor monitor;
    monitor.set_live(false);
    const auto response = monitor.liveness();
    EXPECT_EQ(response.status, 503);
    EXPECT_NE(response.body.find(R"("status":"draining")"), std::string::npos);
}

TEST(HealthMonitor, Readiness_AntesDeArrancar_503) {
    const nexus::HealthMonitor monitor;
    const auto response = monitor.readiness();
    EXPECT_EQ(response.status, 503);
    EXPECT_NE(response.body.find(R"("status":"not_ready")"), std::string::npos);
    EXPECT_NE(response.body.find(R"("name":"startup")"), std::string::npos);
}

TEST(HealthMonitor, Readiness_ArrancadoSinProbes_200Ready) {
    nexus::HealthMonitor monitor;
    monitor.set_started(true);
    const auto response = monitor.readiness();
    EXPECT_EQ(response.status, 200);
    EXPECT_NE(response.body.find(R"("status":"ready")"), std::string::npos);
}

TEST(HealthMonitor, Readiness_ProbeSano_200Ready) {
    nexus::HealthMonitor monitor;
    monitor.set_started(true);
    monitor.register_readiness("raft", []() {
        return nexus::HealthCheckResult{.name = "raft", .healthy = true, .detail = ""};
    });
    const auto response = monitor.readiness();
    EXPECT_EQ(response.status, 200);
    EXPECT_NE(response.body.find(R"("name":"raft")"), std::string::npos);
    EXPECT_NE(response.body.find(R"("healthy":true)"), std::string::npos);
}

TEST(HealthMonitor, Readiness_ProbeNoSano_503ConDetalle) {
    nexus::HealthMonitor monitor;
    monitor.set_started(true);
    monitor.register_readiness("raft", []() {
        return nexus::HealthCheckResult{.name = "raft", .healthy = false, .detail = "sin líder"};
    });
    const auto response = monitor.readiness();
    EXPECT_EQ(response.status, 503);
    EXPECT_NE(response.body.find(R"("status":"not_ready")"), std::string::npos);
    EXPECT_NE(response.body.find(R"("detail":"sin l)"), std::string::npos);
}

TEST(HealthMonitor, DiskSpaceProbe_ConEspacio_Sano) {
    const auto probe = nexus::disk_space_probe(std::filesystem::temp_directory_path(), 0);
    const nexus::HealthCheckResult result = probe();
    EXPECT_TRUE(result.healthy);
    EXPECT_EQ(result.name, "disk");
}

TEST(HealthMonitor, DiskSpaceProbe_SinEspacio_NoSano) {
    const auto probe = nexus::disk_space_probe(std::filesystem::temp_directory_path(),
                                               std::numeric_limits<std::uintmax_t>::max());
    const nexus::HealthCheckResult result = probe();
    EXPECT_FALSE(result.healthy);
    EXPECT_FALSE(result.detail.empty());
}

}  // namespace
