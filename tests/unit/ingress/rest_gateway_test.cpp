// RestGateway (§7.6): enrutado /api/v1 sobre un doble de AdminService (valida el puerto ADR-0018),
// autenticación Bearer JWT, paginación, serialización JSON y errores RFC 7807.
#include "ingress/rest_gateway.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "common/base64.hpp"
#include "common/error.hpp"
#include "common/sha256.hpp"
#include "common/task.hpp"
#include "ingress/admin_service.hpp"
#include "ingress/http.hpp"
#include "ingress/jwt.hpp"

namespace {

// Doble de AdminService: registra la última creación y devuelve resultados configurables.
class FakeAdmin : public nexus::AdminService {
public:
    std::vector<nexus::TopicSummary> topics;
    std::vector<nexus::GroupSummary> groups;
    nexus::CreateTopicSpec last_spec;
    bool create_fails = false;
    bool describe_fails = false;
    bool delete_fails = false;

    nexus::task<nexus::expected<nexus::TopicSummary>> create_topic(
        const nexus::CreateTopicSpec& spec) override {
        last_spec = spec;
        if (create_fails) {
            co_return nexus::make_error(nexus::ErrorCode::InvalidArgument, "el topic ya existe");
        }
        co_return nexus::TopicSummary{.name = spec.name,
                                      .partition_count = spec.partition_count,
                                      .replication_factor = spec.replication_factor,
                                      .created_at_ms = 123};
    }

    nexus::task<nexus::expected<void>> delete_topic(std::string_view /*name*/) override {
        if (delete_fails) {
            co_return nexus::make_error(nexus::ErrorCode::NotFound, "topic inexistente");
        }
        co_return nexus::expected<void>{};
    }

    nexus::expected<nexus::TopicDescription> describe_topic(std::string_view name) const override {
        if (describe_fails) {
            return nexus::make_error(nexus::ErrorCode::NotFound, "topic inexistente");
        }
        nexus::TopicDescription description;
        description.summary = nexus::TopicSummary{.name = std::string{name}, .partition_count = 2};
        description.partitions = {nexus::PartitionInfo{.id = 0, .leader = 1, .high_watermark = 5},
                                  nexus::PartitionInfo{.id = 1, .leader = 1, .high_watermark = 9}};
        return description;
    }

    std::vector<nexus::TopicSummary> list_topics(nexus::Page /*page*/) const override {
        return topics;
    }
    std::vector<nexus::GroupSummary> list_groups(nexus::Page /*page*/) const override {
        return groups;
    }
};

nexus::ByteSpan bytes_of(std::string_view text) {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

nexus::HttpRequest make_request(nexus::HttpMethod method, std::string target,
                                std::string body = {}) {
    nexus::HttpRequest request;
    request.method = method;
    request.target = std::move(target);
    request.version = "HTTP/1.1";
    request.body = std::move(body);
    return request;
}

// `handle` es ahora una corrutina (`task<HttpResponse>`); en los tests no hace E/S y se conduce a
// término con `sync_wait`. Azúcar para no repetirlo en cada caso.
nexus::HttpResponse run_handle(const nexus::RestGateway& gateway, const nexus::HttpRequest& request,
                               std::int64_t now_unix_seconds) {
    return nexus::sync_wait(gateway.handle(request, now_unix_seconds));
}

constexpr std::string_view kSecret = "clave-del-gateway";

std::string make_token(std::string_view payload_json) {
    const std::string header = R"({"alg":"HS256","typ":"JWT"})";
    const std::string signing = nexus::base64url_encode(bytes_of(header)) + "." +
                                nexus::base64url_encode(bytes_of(payload_json));
    const nexus::Sha256Digest mac = nexus::hmac_sha256(bytes_of(kSecret), bytes_of(signing));
    return signing + "." + nexus::base64url_encode(mac);
}

TEST(RestGateway, ListTopics_200ConItems) {
    FakeAdmin admin;
    admin.topics = {nexus::TopicSummary{.name = "orders", .partition_count = 3}};
    const nexus::RestGateway gateway{admin, nullptr};

    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Get, "/api/v1/topics"), 0);
    EXPECT_EQ(response.status, 200);
    EXPECT_NE(response.body.find(R"("items":[)"), std::string::npos);
    EXPECT_NE(response.body.find(R"("name":"orders")"), std::string::npos);
    EXPECT_NE(response.body.find(R"("partitionCount":3)"), std::string::npos);
}

TEST(RestGateway, CreateTopic_201ConLocationYSpec) {
    FakeAdmin admin;
    const nexus::RestGateway gateway{admin, nullptr};

    const auto response =
        run_handle(gateway,
                   make_request(nexus::HttpMethod::Post, "/api/v1/topics",
                                R"({"name":"orders","partitionCount":4,"replicationFactor":1})"),
                   0);
    EXPECT_EQ(response.status, 201);
    EXPECT_EQ(admin.last_spec.name, "orders");
    EXPECT_EQ(admin.last_spec.partition_count, 4);
    bool has_location = false;
    for (const auto& [key, value] : response.headers) {
        if (key == "Location") {
            has_location = true;
            EXPECT_EQ(value, "/api/v1/topics/orders");
        }
    }
    EXPECT_TRUE(has_location);
}

TEST(RestGateway, CreateTopic_CuerpoNoJson_400) {
    FakeAdmin admin;
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Post, "/api/v1/topics", "no-json"), 0);
    EXPECT_EQ(response.status, 400);
}

TEST(RestGateway, CreateTopic_SinName_400) {
    FakeAdmin admin;
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response = run_handle(
        gateway, make_request(nexus::HttpMethod::Post, "/api/v1/topics", R"({"x":1})"), 0);
    EXPECT_EQ(response.status, 400);
}

TEST(RestGateway, CreateTopic_ErrorDelAdmin_SeTraduceRfc7807) {
    FakeAdmin admin;
    admin.create_fails = true;
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response = run_handle(
        gateway, make_request(nexus::HttpMethod::Post, "/api/v1/topics", R"({"name":"dup"})"), 0);
    EXPECT_EQ(response.status, 400);  // InvalidArgument → 400.
    bool problem_ct = false;
    for (const auto& [key, value] : response.headers) {
        if (key == "Content-Type" && value == "application/problem+json") {
            problem_ct = true;
        }
    }
    EXPECT_TRUE(problem_ct);
}

TEST(RestGateway, DescribeTopic_200ConParticiones) {
    FakeAdmin admin;
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Get, "/api/v1/topics/orders"), 0);
    EXPECT_EQ(response.status, 200);
    EXPECT_NE(response.body.find(R"("partitions":[)"), std::string::npos);
    EXPECT_NE(response.body.find(R"("highWatermark":9)"), std::string::npos);
}

TEST(RestGateway, DescribeTopic_NoExiste_404) {
    FakeAdmin admin;
    admin.describe_fails = true;
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Get, "/api/v1/topics/x"), 0);
    EXPECT_EQ(response.status, 404);
}

TEST(RestGateway, DeleteTopic_204) {
    FakeAdmin admin;
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Delete, "/api/v1/topics/orders"), 0);
    EXPECT_EQ(response.status, 204);
    EXPECT_TRUE(response.body.empty());
}

TEST(RestGateway, DeleteTopic_NoExiste_404) {
    FakeAdmin admin;
    admin.delete_fails = true;
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Delete, "/api/v1/topics/x"), 0);
    EXPECT_EQ(response.status, 404);
}

TEST(RestGateway, ListTopics_PaginacionInvalida_400) {
    FakeAdmin admin;
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Get, "/api/v1/topics?page=0"), 0);
    EXPECT_EQ(response.status, 400);
}

TEST(RestGateway, MetodoNoPermitido_405) {
    FakeAdmin admin;
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Put, "/api/v1/topics"), 0);
    EXPECT_EQ(response.status, 405);
}

TEST(RestGateway, RutaDesconocida_404) {
    FakeAdmin admin;
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Get, "/api/v1/desconocido"), 0);
    EXPECT_EQ(response.status, 404);
}

TEST(RestGateway, ListGroups_200) {
    FakeAdmin admin;
    admin.groups = {nexus::GroupSummary{
        .group_id = "g1", .state = "Stable", .generation = 2, .member_count = 3}};
    const nexus::RestGateway gateway{admin, nullptr};
    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Get, "/api/v1/groups"), 0);
    EXPECT_EQ(response.status, 200);
    EXPECT_NE(response.body.find(R"("groupId":"g1")"), std::string::npos);
    EXPECT_NE(response.body.find(R"("memberCount":3)"), std::string::npos);
}

TEST(RestGateway, Auth_SinToken_401) {
    FakeAdmin admin;
    const nexus::JwtVerifier verifier{std::string{kSecret}};
    const nexus::RestGateway gateway{admin, &verifier};
    const auto response =
        run_handle(gateway, make_request(nexus::HttpMethod::Get, "/api/v1/topics"), 0);
    EXPECT_EQ(response.status, 401);
}

TEST(RestGateway, Auth_TokenValido_200) {
    FakeAdmin admin;
    const nexus::JwtVerifier verifier{std::string{kSecret}};
    const nexus::RestGateway gateway{admin, &verifier};

    nexus::HttpRequest request = make_request(nexus::HttpMethod::Get, "/api/v1/topics");
    request.headers.emplace_back("Authorization",
                                 "Bearer " + make_token(R"({"sub":"admin","exp":2000})"));
    const auto response = run_handle(gateway, request, 1000);
    EXPECT_EQ(response.status, 200);
}

TEST(RestGateway, Auth_TokenExpirado_401) {
    FakeAdmin admin;
    const nexus::JwtVerifier verifier{std::string{kSecret}};
    const nexus::RestGateway gateway{admin, &verifier};

    nexus::HttpRequest request = make_request(nexus::HttpMethod::Get, "/api/v1/topics");
    request.headers.emplace_back("Authorization",
                                 "Bearer " + make_token(R"({"sub":"admin","exp":500})"));
    const auto response = run_handle(gateway, request, 1000);  // 1000 > 500 → expirado.
    EXPECT_EQ(response.status, 401);
}

}  // namespace
