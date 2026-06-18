// Subcomandos `topic` del CLI: se prueban con un doble de AdminClient (sin red), verificando la
// ruta/método/cuerpo enviados, la salida formateada y el código de retorno.
#include "cli/topic_commands.hpp"

#include <gtest/gtest.h>

#include <array>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "cli/admin_client.hpp"
#include "common/error.hpp"

namespace {

// Doble de AdminClient: registra la última llamada y devuelve una respuesta configurable.
class FakeClient : public nexus::cli::AdminClient {
public:
    std::string last_method;
    std::string last_path;
    std::string last_body;
    nexus::cli::ClientResponse next{.status = 200, .body = "{}"};

    nexus::expected<nexus::cli::ClientResponse> get(std::string_view path) override {
        last_method = "GET";
        last_path = std::string{path};
        return next;
    }
    nexus::expected<nexus::cli::ClientResponse> post(std::string_view path,
                                                     std::string_view body) override {
        last_method = "POST";
        last_path = std::string{path};
        last_body = std::string{body};
        return next;
    }
    nexus::expected<nexus::cli::ClientResponse> del(std::string_view path) override {
        last_method = "DELETE";
        last_path = std::string{path};
        return next;
    }
};

std::vector<std::string_view> args_of(std::initializer_list<std::string_view> items) {
    return {items};
}

int run(nexus::cli::AdminClient& client, std::vector<std::string_view> args, std::ostream& out,
        std::ostream& err) {
    return nexus::cli::run_topic(client, args, out, err);
}

TEST(TopicCommands, List_ImprimeTablaConTopics) {
    FakeClient client;
    client.next = {
        .status = 200,
        .body = R"({"items":[{"name":"orders","partitionCount":3,"replicationFactor":1}]})"};
    std::ostringstream out;
    std::ostringstream err;

    const int code = run(client, args_of({"list"}), out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(client.last_method, "GET");
    EXPECT_EQ(client.last_path, "/api/v1/topics");
    EXPECT_NE(out.str().find("orders"), std::string::npos);
    EXPECT_NE(out.str().find("NAME"), std::string::npos);
}

TEST(TopicCommands, Create_EnviaPostConSpec) {
    FakeClient client;
    client.next = {.status = 201, .body = "{}"};
    std::ostringstream out;
    std::ostringstream err;

    const int code =
        run(client, args_of({"create", "events", "--partitions", "4", "-r", "1"}), out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(client.last_method, "POST");
    EXPECT_EQ(client.last_path, "/api/v1/topics");
    EXPECT_NE(client.last_body.find(R"("name":"events")"), std::string::npos);
    EXPECT_NE(client.last_body.find(R"("partitionCount":4)"), std::string::npos);
    EXPECT_NE(out.str().find("creado"), std::string::npos);
}

TEST(TopicCommands, Create_SinNombre_Falla) {
    FakeClient client;
    std::ostringstream out;
    std::ostringstream err;
    const int code = run(client, args_of({"create"}), out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("uso:"), std::string::npos);
}

TEST(TopicCommands, Describe_ImprimeParticiones) {
    FakeClient client;
    client.next = {
        .status = 200,
        .body =
            R"({"name":"orders","partitionCount":2,"partitions":[{"id":0,"leader":1,"highWatermark":5}]})"};
    std::ostringstream out;
    std::ostringstream err;

    const int code = run(client, args_of({"describe", "orders"}), out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(client.last_path, "/api/v1/topics/orders");
    EXPECT_NE(out.str().find("orders"), std::string::npos);
    EXPECT_NE(out.str().find("HIGH-WATERMARK"), std::string::npos);
}

TEST(TopicCommands, Delete_EnviaDelete204) {
    FakeClient client;
    client.next = {.status = 204, .body = ""};
    std::ostringstream out;
    std::ostringstream err;

    const int code = run(client, args_of({"delete", "orders"}), out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(client.last_method, "DELETE");
    EXPECT_EQ(client.last_path, "/api/v1/topics/orders");
    EXPECT_NE(out.str().find("borrado"), std::string::npos);
}

TEST(TopicCommands, ErrorHttp_SeTraduceAExit1) {
    FakeClient client;
    client.next = {.status = 404, .body = R"({"title":"Not Found"})"};
    std::ostringstream out;
    std::ostringstream err;

    const int code = run(client, args_of({"describe", "fantasma"}), out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("404"), std::string::npos);
}

TEST(TopicCommands, SubcomandoDesconocido_Falla) {
    FakeClient client;
    std::ostringstream out;
    std::ostringstream err;
    const int code = run(client, args_of({"frobnicate"}), out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("desconocido"), std::string::npos);
}

}  // namespace
