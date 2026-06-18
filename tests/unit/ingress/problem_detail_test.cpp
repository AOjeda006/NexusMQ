// ProblemDetail (RFC 7807, §7.6/ADR-0009): mapeo ErrorCode→HTTP, cuerpo JSON y respuesta con
// Content-Type application/problem+json.
#include "ingress/problem_detail.hpp"

#include <gtest/gtest.h>

#include <string>

#include "common/error.hpp"

namespace {

TEST(ProblemDetail, MapeoErrorCodeAHttp) {
    EXPECT_EQ(nexus::http_status_for(nexus::ErrorCode::InvalidArgument), 400);
    EXPECT_EQ(nexus::http_status_for(nexus::ErrorCode::OutOfRange), 400);
    EXPECT_EQ(nexus::http_status_for(nexus::ErrorCode::NotFound), 404);
    EXPECT_EQ(nexus::http_status_for(nexus::ErrorCode::Unsupported), 501);
    EXPECT_EQ(nexus::http_status_for(nexus::ErrorCode::OutOfSpace), 507);
    EXPECT_EQ(nexus::http_status_for(nexus::ErrorCode::Shutdown), 503);
    EXPECT_EQ(nexus::http_status_for(nexus::ErrorCode::Corrupt), 500);
}

TEST(ProblemDetail, DesdeError_TituloYDetalle) {
    const nexus::Error err{nexus::ErrorCode::NotFound, "topic 'x' no existe"};
    const nexus::ProblemDetail problem = nexus::problem_from_error(err, "/api/v1/topics/x");
    EXPECT_EQ(problem.status, 404);
    EXPECT_EQ(problem.title, "Not Found");
    EXPECT_EQ(problem.detail, "topic 'x' no existe");
    EXPECT_EQ(problem.instance, "/api/v1/topics/x");
}

TEST(ProblemDetail, ToJson_RFC7807) {
    const nexus::Error err{nexus::ErrorCode::InvalidArgument, "size fuera de rango"};
    const std::string json = nexus::problem_from_error(err).to_json();
    EXPECT_EQ(
        json,
        R"({"type":"about:blank","title":"Bad Request","status":400,"detail":"size fuera de rango"})");
}

TEST(ProblemDetail, ToResponse_ContentTypeYStatus) {
    const nexus::Error err{nexus::ErrorCode::NotFound, "no existe"};
    const nexus::HttpResponse resp = nexus::problem_from_error(err).to_response();
    EXPECT_EQ(resp.status, 404);
    EXPECT_EQ(resp.reason, "Not Found");
    bool found_ct = false;
    for (const auto& [key, value] : resp.headers) {
        if (key == "Content-Type") {
            found_ct = true;
            EXPECT_EQ(value, "application/problem+json");
        }
    }
    EXPECT_TRUE(found_ct);
    EXPECT_NE(resp.body.find("\"status\":404"), std::string::npos);
}

}  // namespace
