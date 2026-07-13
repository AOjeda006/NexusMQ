// AdminRouter (§7.6): multiplexa el puerto de operación — REST admin, /metrics (Prometheus) y los
// health-checks /healthz//readyz, delegando el resto al RestGateway (auth + /api/v1).
#include "server/admin_router.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "common/error.hpp"
#include "common/task.hpp"
#include "ingress/admin_service.hpp"
#include "ingress/health_monitor.hpp"
#include "ingress/http.hpp"
#include "ingress/jwt.hpp"
#include "ingress/rest_gateway.hpp"
#include "telemetry/metrics.hpp"

namespace {

// Doble mínimo de AdminService: lista de topics configurable; el resto, no-op.
class FakeAdmin : public nexus::AdminService {
public:
    std::vector<nexus::TopicSummary> topics;

    nexus::task<nexus::expected<nexus::TopicSummary>> create_topic(
        const nexus::CreateTopicSpec& spec) override {
        co_return nexus::TopicSummary{.name = spec.name};
    }
    nexus::task<nexus::expected<void>> delete_topic(std::string_view /*name*/) override {
        co_return nexus::expected<void>{};
    }
    nexus::task<nexus::expected<nexus::TopicDescription>> describe_topic(
        std::string_view /*name*/) override {
        co_return nexus::make_error(nexus::ErrorCode::NotFound, "no");
    }
    std::vector<nexus::TopicSummary> list_topics(nexus::Page /*page*/) const override {
        return topics;
    }
    nexus::task<std::vector<nexus::GroupSummary>> list_groups(nexus::Page /*page*/) override {
        co_return std::vector<nexus::GroupSummary>{};
    }
    nexus::task<nexus::expected<nexus::GroupDescription>> describe_group(
        std::string_view /*group_id*/) override {
        co_return nexus::make_error(nexus::ErrorCode::NotFound, "no");
    }
    nexus::task<nexus::expected<nexus::ClusterInfo>> describe_cluster() override {
        co_return nexus::ClusterInfo{};
    }
};

nexus::HttpRequest make_request(nexus::HttpMethod method, std::string target) {
    nexus::HttpRequest request;
    request.method = method;
    request.target = std::move(target);
    request.version = "HTTP/1.1";
    return request;
}

// `handle` es ahora una corrutina; se conduce a término con `sync_wait` (sin E/S real en tests).
nexus::HttpResponse run_handle(const nexus::AdminRouter& router,
                               const nexus::HttpRequest& request) {
    return nexus::sync_wait(router.handle(request));
}

TEST(AdminRouter, Metrics_200ExposicionPrometheus) {
    FakeAdmin admin;
    nexus::RestGateway rest{admin, nullptr};
    nexus::HealthMonitor health;
    nexus::MetricsRegistry metrics;
    metrics.counter("nexus_requests_total").inc(7);
    const nexus::AdminRouter router{rest, health, metrics};

    const auto response = run_handle(router, make_request(nexus::HttpMethod::Get, "/metrics"));
    EXPECT_EQ(response.status, 200);
    EXPECT_NE(response.body.find("nexus_requests_total"), std::string::npos);
    bool prometheus_ct = false;
    for (const auto& [key, value] : response.headers) {
        if (key == "Content-Type" && value.find("version=0.0.4") != std::string::npos) {
            prometheus_ct = true;
        }
    }
    EXPECT_TRUE(prometheus_ct);
}

TEST(AdminRouter, Metrics_MetodoNoGet_405) {
    FakeAdmin admin;
    nexus::RestGateway rest{admin, nullptr};
    nexus::HealthMonitor health;
    nexus::MetricsRegistry metrics;
    const nexus::AdminRouter router{rest, health, metrics};

    const auto response = run_handle(router, make_request(nexus::HttpMethod::Post, "/metrics"));
    EXPECT_EQ(response.status, 405);
}

TEST(AdminRouter, MetricsSnapshot_200Json) {
    FakeAdmin admin;
    nexus::RestGateway rest{admin, nullptr};
    nexus::HealthMonitor health;
    nexus::MetricsRegistry metrics;
    metrics.counter("nexus_requests_total", {{"protocol", "native"}}).inc(3);
    const nexus::AdminRouter router{rest, health, metrics};

    const auto response =
        run_handle(router, make_request(nexus::HttpMethod::Get, "/api/v1/metrics/snapshot"));
    EXPECT_EQ(response.status, 200);
    bool json_ct = false;
    for (const auto& [key, value] : response.headers) {
        if (key == "Content-Type" && value == "application/json") {
            json_ct = true;
        }
    }
    EXPECT_TRUE(json_ct);
    EXPECT_NE(response.body.find(R"("name":"nexus_requests_total")"), std::string::npos);
    EXPECT_NE(response.body.find(R"("type":"counter")"), std::string::npos);
    EXPECT_NE(response.body.find(R"("protocol":"native")"), std::string::npos);
}

TEST(AdminRouter, MetricsSnapshot_MetodoNoGet_405) {
    FakeAdmin admin;
    nexus::RestGateway rest{admin, nullptr};
    nexus::HealthMonitor health;
    nexus::MetricsRegistry metrics;
    const nexus::AdminRouter router{rest, health, metrics};

    const auto response =
        run_handle(router, make_request(nexus::HttpMethod::Post, "/api/v1/metrics/snapshot"));
    EXPECT_EQ(response.status, 405);
}

TEST(AdminRouter, Healthz_200Liveness) {
    FakeAdmin admin;
    nexus::RestGateway rest{admin, nullptr};
    nexus::HealthMonitor health;
    nexus::MetricsRegistry metrics;
    const nexus::AdminRouter router{rest, health, metrics};

    const auto response = run_handle(router, make_request(nexus::HttpMethod::Get, "/healthz"));
    EXPECT_EQ(response.status, 200);
    EXPECT_NE(response.body.find(R"("status":"ok")"), std::string::npos);
}

TEST(AdminRouter, Healthz_MetodoNoGet_405) {
    FakeAdmin admin;
    nexus::RestGateway rest{admin, nullptr};
    nexus::HealthMonitor health;
    nexus::MetricsRegistry metrics;
    const nexus::AdminRouter router{rest, health, metrics};

    // P5b: consistente con /metrics — un método no-GET en un endpoint de solo lectura es 405.
    const auto response = run_handle(router, make_request(nexus::HttpMethod::Post, "/healthz"));
    EXPECT_EQ(response.status, 405);
}

TEST(AdminRouter, Readyz_MetodoNoGet_405) {
    FakeAdmin admin;
    nexus::RestGateway rest{admin, nullptr};
    nexus::HealthMonitor health;
    nexus::MetricsRegistry metrics;
    const nexus::AdminRouter router{rest, health, metrics};

    const auto response = run_handle(router, make_request(nexus::HttpMethod::Delete, "/readyz"));
    EXPECT_EQ(response.status, 405);
}

TEST(AdminRouter, Readyz_AntesDeArrancar_503) {
    FakeAdmin admin;
    nexus::RestGateway rest{admin, nullptr};
    nexus::HealthMonitor health;  // sin set_started: no listo.
    nexus::MetricsRegistry metrics;
    const nexus::AdminRouter router{rest, health, metrics};

    const auto response = run_handle(router, make_request(nexus::HttpMethod::Get, "/readyz"));
    EXPECT_EQ(response.status, 503);
    EXPECT_NE(response.body.find(R"("status":"not_ready")"), std::string::npos);
}

TEST(AdminRouter, Rest_DelegaEnGateway_200) {
    FakeAdmin admin;
    admin.topics = {nexus::TopicSummary{.name = "orders", .partition_count = 3}};
    nexus::RestGateway rest{admin, nullptr};
    nexus::HealthMonitor health;
    nexus::MetricsRegistry metrics;
    const nexus::AdminRouter router{rest, health, metrics};

    const auto response =
        run_handle(router, make_request(nexus::HttpMethod::Get, "/api/v1/topics"));
    EXPECT_EQ(response.status, 200);
    EXPECT_NE(response.body.find(R"("name":"orders")"), std::string::npos);
}

TEST(AdminRouter, RutaDesconocida_DelegaEnGateway_404) {
    FakeAdmin admin;
    nexus::RestGateway rest{admin, nullptr};
    nexus::HealthMonitor health;
    nexus::MetricsRegistry metrics;
    const nexus::AdminRouter router{rest, health, metrics};

    const auto response = run_handle(router, make_request(nexus::HttpMethod::Get, "/desconocido"));
    EXPECT_EQ(response.status, 404);
}

TEST(AdminRouter, Rest_RelojInyectado_AplicaAlJwt) {
    FakeAdmin admin;
    const nexus::JwtVerifier verifier{std::string{"secreto-de-operacion"}};
    nexus::RestGateway rest{admin, &verifier};
    nexus::HealthMonitor health;
    nexus::MetricsRegistry metrics;
    // El reloj inyectado fija «ahora» = 5000, posterior a cualquier exp del token de prueba.
    const nexus::AdminRouter router{rest, health, metrics, []() -> std::int64_t { return 5000; }};

    // Sin token: el gateway exige autenticación → 401 (la ruta REST sí pasa por auth).
    const auto response =
        run_handle(router, make_request(nexus::HttpMethod::Get, "/api/v1/topics"));
    EXPECT_EQ(response.status, 401);
}

}  // namespace
