/// @file   cli/group_commands.cpp
/// @brief  Implementación de los subcomandos `group` del CLI.
/// @ingroup cli

#include "cli/group_commands.hpp"

#include <iomanip>
#include <ios>
#include <string>

#include "cli/render.hpp"
#include "ingress/json_value.hpp"

namespace nexus::cli {

namespace {

constexpr std::string_view kGroupsPath = "/api/v1/groups";

/// Recupera y parsea el array `items` del listado de grupos.
expected<JsonValue> fetch_groups(AdminClient& client, std::ostream& err) {
    expected<ClientResponse> response = client.get(kGroupsPath);
    if (!response) {
        err << response.error().message() << '\n';
        return std::unexpected{response.error()};
    }
    if (response->status != 200) {
        fail(err, *response);
        return make_error(ErrorCode::IoError, "respuesta de error");
    }
    expected<JsonValue> json = parse_json(response->body);
    if (!json || !json->is_object()) {
        err << "respuesta JSON inesperada\n";
        return make_error(ErrorCode::Corrupt, "json inesperado");
    }
    return json;
}

/// Imprime una fila de grupo (groupId, state, generation, memberCount).
void print_group_row(std::ostream& out, const JsonValue& group) {
    out << std::left << std::setw(24) << json_str(group, "groupId") << std::setw(22)
        << json_str(group, "state") << std::setw(12) << json_int(group, "generation")
        << json_int(group, "memberCount") << '\n';
}

int list(AdminClient& client, std::ostream& out, std::ostream& err) {
    const expected<JsonValue> json = fetch_groups(client, err);
    if (!json) {
        return 1;
    }
    out << std::left << std::setw(24) << "GROUP" << std::setw(22) << "STATE" << std::setw(12)
        << "GENERATION" << "MEMBERS" << '\n';
    if (const JsonValue* items = json->find("items"); items != nullptr && items->is_array()) {
        for (const JsonValue& group : items->as_array()) {
            if (group.is_object()) {
                print_group_row(out, group);
            }
        }
    }
    return 0;
}

int describe(AdminClient& client, std::span<const std::string_view> args, std::ostream& out,
             std::ostream& err) {
    if (args.empty()) {
        err << "uso: group describe <id>\n";
        return 1;
    }
    const expected<JsonValue> json = fetch_groups(client, err);
    if (!json) {
        return 1;
    }
    const std::string_view id = args[0];
    if (const JsonValue* items = json->find("items"); items != nullptr && items->is_array()) {
        for (const JsonValue& group : items->as_array()) {
            if (group.is_object() && json_str(group, "groupId") == id) {
                out << "group:      " << id << '\n';
                out << "state:      " << json_str(group, "state") << '\n';
                out << "generation: " << json_int(group, "generation") << '\n';
                out << "members:    " << json_int(group, "memberCount") << '\n';
                return 0;
            }
        }
    }
    err << "grupo no encontrado: " << id << '\n';
    return 1;
}

}  // namespace

int run_group(AdminClient& client, std::span<const std::string_view> args, std::ostream& out,
              std::ostream& err) {
    if (args.empty()) {
        err << "uso: group <list|describe> [args…]\n";
        return 1;
    }
    const std::string_view sub = args[0];
    if (sub == "list") {
        return list(client, out, err);
    }
    if (sub == "describe") {
        return describe(client, args.subspan(1), out, err);
    }
    err << "subcomando de group desconocido: " << sub << '\n';
    return 1;
}

}  // namespace nexus::cli
