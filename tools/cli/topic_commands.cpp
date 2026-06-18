/// @file   cli/topic_commands.cpp
/// @brief  Implementación de los subcomandos `topic` del CLI.
/// @ingroup cli

#include "cli/topic_commands.hpp"

#include <iomanip>
#include <ios>
#include <string>

#include "ingress/json_value.hpp"

namespace nexus::cli {

namespace {

constexpr std::string_view kTopicsPath = "/api/v1/topics";

/// Lee un entero @p key de un objeto JSON (0 si falta o no es número).
std::int64_t json_int(const JsonValue& object, std::string_view key) {
    const JsonValue* value = object.find(key);
    return (value != nullptr && value->is_number()) ? value->as_int64() : 0;
}

/// Lee una cadena @p key de un objeto JSON (vacía si falta o no es cadena).
std::string json_str(const JsonValue& object, std::string_view key) {
    const JsonValue* value = object.find(key);
    return (value != nullptr && value->is_string()) ? value->as_string() : std::string{};
}

/// Imprime el cuerpo de error (problem+json o texto) y devuelve 1.
int fail(std::ostream& err, const ClientResponse& response) {
    err << "error (HTTP " << response.status << ")";
    if (!response.body.empty()) {
        err << ": " << response.body;
    }
    err << '\n';
    return 1;
}

/// `topic list`: GET /topics e imprime una tabla.
int list(AdminClient& client, std::ostream& out, std::ostream& err) {
    const expected<ClientResponse> response = client.get(kTopicsPath);
    if (!response) {
        err << response.error().message() << '\n';
        return 1;
    }
    if (response->status != 200) {
        return fail(err, *response);
    }
    const expected<JsonValue> json = parse_json(response->body);
    if (!json || !json->is_object()) {
        err << "respuesta JSON inesperada\n";
        return 1;
    }
    const JsonValue* items = json->find("items");
    out << std::left << std::setw(24) << "NAME" << std::setw(12) << "PARTITIONS" << "REPLICATION"
        << '\n';
    if (items != nullptr && items->is_array()) {
        for (const JsonValue& item : items->as_array()) {
            if (!item.is_object()) {
                continue;
            }
            out << std::left << std::setw(24) << json_str(item, "name") << std::setw(12)
                << json_int(item, "partitionCount") << json_int(item, "replicationFactor") << '\n';
        }
    }
    return 0;
}

/// `topic create <name> [--partitions N] [--replication R]`.
int create(AdminClient& client, std::span<const std::string_view> args, std::ostream& out,
           std::ostream& err) {
    if (args.empty()) {
        err << "uso: topic create <name> [--partitions N] [--replication R]\n";
        return 1;
    }
    const std::string_view name = args[0];
    std::int64_t partitions = 1;
    std::int64_t replication = 1;
    for (std::size_t i = 1; i < args.size(); ++i) {
        if ((args[i] == "--partitions" || args[i] == "-p") && i + 1 < args.size()) {
            partitions = std::stoll(std::string{args[++i]});
        } else if ((args[i] == "--replication" || args[i] == "-r") && i + 1 < args.size()) {
            replication = std::stoll(std::string{args[++i]});
        } else {
            err << "argumento desconocido: " << args[i] << '\n';
            return 1;
        }
    }
    std::string body = R"({"name":")";
    body += name;
    body += R"(","partitionCount":)";
    body += std::to_string(partitions);
    body += R"(,"replicationFactor":)";
    body += std::to_string(replication);
    body += "}";

    const expected<ClientResponse> response = client.post(kTopicsPath, body);
    if (!response) {
        err << response.error().message() << '\n';
        return 1;
    }
    if (response->status != 201) {
        return fail(err, *response);
    }
    out << "topic '" << name << "' creado\n";
    return 0;
}

/// `topic describe <name>`: GET /topics/<name> e imprime resumen + particiones.
int describe(AdminClient& client, std::span<const std::string_view> args, std::ostream& out,
             std::ostream& err) {
    if (args.empty()) {
        err << "uso: topic describe <name>\n";
        return 1;
    }
    const std::string path = std::string{kTopicsPath} + "/" + std::string{args[0]};
    const expected<ClientResponse> response = client.get(path);
    if (!response) {
        err << response.error().message() << '\n';
        return 1;
    }
    if (response->status != 200) {
        return fail(err, *response);
    }
    const expected<JsonValue> json = parse_json(response->body);
    if (!json || !json->is_object()) {
        err << "respuesta JSON inesperada\n";
        return 1;
    }
    out << "name:        " << json_str(*json, "name") << '\n';
    out << "partitions:  " << json_int(*json, "partitionCount") << '\n';
    out << "replication: " << json_int(*json, "replicationFactor") << '\n';
    if (const JsonValue* partitions = json->find("partitions");
        partitions != nullptr && partitions->is_array()) {
        out << std::left << std::setw(6) << "ID" << std::setw(8) << "LEADER" << "HIGH-WATERMARK"
            << '\n';
        for (const JsonValue& part : partitions->as_array()) {
            if (!part.is_object()) {
                continue;
            }
            out << std::left << std::setw(6) << json_int(part, "id") << std::setw(8)
                << json_int(part, "leader") << json_int(part, "highWatermark") << '\n';
        }
    }
    return 0;
}

/// `topic delete <name>`: DELETE /topics/<name>.
int remove(AdminClient& client, std::span<const std::string_view> args, std::ostream& out,
           std::ostream& err) {
    if (args.empty()) {
        err << "uso: topic delete <name>\n";
        return 1;
    }
    const std::string path = std::string{kTopicsPath} + "/" + std::string{args[0]};
    const expected<ClientResponse> response = client.del(path);
    if (!response) {
        err << response.error().message() << '\n';
        return 1;
    }
    if (response->status != 204) {
        return fail(err, *response);
    }
    out << "topic '" << args[0] << "' borrado\n";
    return 0;
}

}  // namespace

int run_topic(AdminClient& client, std::span<const std::string_view> args, std::ostream& out,
              std::ostream& err) {
    if (args.empty()) {
        err << "uso: topic <list|create|describe|delete> [args…]\n";
        return 1;
    }
    const std::string_view sub = args[0];
    const std::span<const std::string_view> rest = args.subspan(1);
    if (sub == "list") {
        return list(client, out, err);
    }
    if (sub == "create") {
        return create(client, rest, out, err);
    }
    if (sub == "describe") {
        return describe(client, rest, out, err);
    }
    if (sub == "delete") {
        return remove(client, rest, out, err);
    }
    err << "subcomando de topic desconocido: " << sub << '\n';
    return 1;
}

}  // namespace nexus::cli
