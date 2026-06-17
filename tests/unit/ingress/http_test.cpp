// Parser/serializador HTTP/1.1 (§7.6): determinista y defensivo. Verifica el parseo de la línea de
// petición, método, target con path/query, cabeceras case-insensitive, cuerpo por Content-Length,
// rechazos por malformación y límites, y la serialización de respuesta con Content-Length.
#include "ingress/http.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

TEST(Http, ParseRequest_GetSimple) {
    const auto req = nexus::parse_request("GET /api/v1/topics HTTP/1.1\r\nHost: x\r\n\r\n");
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->method, nexus::HttpMethod::Get);
    EXPECT_EQ(req->target, "/api/v1/topics");
    EXPECT_EQ(req->version, "HTTP/1.1");
    EXPECT_EQ(req->path(), "/api/v1/topics");
    EXPECT_TRUE(req->query().empty());
    EXPECT_TRUE(req->body.empty());
}

TEST(Http, ParseRequest_PathYQuerySeSeparan) {
    const auto req = nexus::parse_request("GET /api/v1/topics?page=2&size=10 HTTP/1.1\r\n\r\n");
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->path(), "/api/v1/topics");
    EXPECT_EQ(req->query(), "page=2&size=10");
}

TEST(Http, ParseRequest_CabecerasCaseInsensitive) {
    const auto req =
        nexus::parse_request("GET / HTTP/1.1\r\nContent-Type: application/json\r\n\r\n");
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->header("content-type"), "application/json");
    EXPECT_EQ(req->header("CONTENT-TYPE"), "application/json");
    EXPECT_FALSE(req->header("authorization").has_value());
}

TEST(Http, ParseRequest_CuerpoPorContentLength) {
    const auto req = nexus::parse_request(
        "POST /api/v1/topics HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello world");
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->method, nexus::HttpMethod::Post);
    EXPECT_EQ(req->body, "hello");  // exactamente Content-Length bytes.
}

TEST(Http, ParseRequest_CuerpoIncompleto_Rechaza) {
    const auto req = nexus::parse_request("POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\nabc");
    ASSERT_FALSE(req.has_value());
    EXPECT_EQ(req.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Http, ParseRequest_SinFinDeCabeceras_Rechaza) {
    const auto req = nexus::parse_request("GET / HTTP/1.1\r\nHost: x\r\n");
    EXPECT_FALSE(req.has_value());
}

TEST(Http, ParseRequest_LineaMalformada_Rechaza) {
    EXPECT_FALSE(nexus::parse_request("GET /only-two-tokens\r\n\r\n").has_value());
    EXPECT_FALSE(nexus::parse_request("GET / HTTP/1.1").has_value());
    EXPECT_FALSE(nexus::parse_request("GET / FTP/1.1\r\n\r\n").has_value());
}

TEST(Http, ParseRequest_ContentLengthInvalido_Rechaza) {
    EXPECT_FALSE(
        nexus::parse_request("POST / HTTP/1.1\r\nContent-Length: abc\r\n\r\n").has_value());
}

TEST(Http, ParseRequest_CuerpoExcedeLimite_Rechaza) {
    nexus::HttpParseLimits limits;
    limits.max_body = 4;
    const auto req =
        nexus::parse_request("POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello", limits);
    EXPECT_FALSE(req.has_value());
}

TEST(Http, ParseRequest_MetodoDesconocido) {
    const auto req = nexus::parse_request("BREW / HTTP/1.1\r\n\r\n");
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->method, nexus::HttpMethod::Unknown);
    EXPECT_EQ(req->method_text, "BREW");
}

TEST(Http, Response_SerializaConContentLength) {
    nexus::HttpResponse resp;
    resp.status = 201;
    resp.reason = "Created";
    resp.set_header("Content-Type", "application/json");
    resp.body = "{\"ok\":true}";
    const std::string text = resp.serialize();
    EXPECT_TRUE(text.starts_with("HTTP/1.1 201 Created\r\n"));
    EXPECT_NE(text.find("Content-Type: application/json\r\n"), std::string::npos);
    EXPECT_NE(text.find("Content-Length: 11\r\n"), std::string::npos);
    EXPECT_TRUE(text.ends_with("\r\n\r\n{\"ok\":true}"));
}

TEST(Http, Response_SetHeader_ReemplazaCaseInsensitive) {
    nexus::HttpResponse resp;
    resp.set_header("X-Test", "a");
    resp.set_header("x-test", "b");  // reemplaza.
    int count = 0;
    for (const auto& [key, value] : resp.headers) {
        if (key == "X-Test") {
            ++count;
            EXPECT_EQ(value, "b");
        }
    }
    EXPECT_EQ(count, 1);
}

}  // namespace
