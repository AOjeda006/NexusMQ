/// @file   cli/partition_commands.cpp
/// @brief  Implementación del subcomando `partitions` del CLI.
/// @ingroup cli

#include "cli/partition_commands.hpp"

#include <iomanip>
#include <ios>
#include <string>

#include "cli/render.hpp"
#include "ingress/json_value.hpp"

namespace nexus::cli {

int run_partitions(AdminClient& client, std::span<const std::string_view> args, std::ostream& out,
                   std::ostream& err) {
    if (args.empty()) {
        err << "uso: partitions <topic>\n";
        return 1;
    }
    const std::string path = std::string{"/api/v1/topics/"} + std::string{args[0]};
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
    out << std::left << std::setw(6) << "ID" << std::setw(8) << "LEADER" << std::setw(16)
        << "HIGH-WATERMARK" << "LEADER-EPOCH" << '\n';
    if (const JsonValue* partitions = json->find("partitions");
        partitions != nullptr && partitions->is_array()) {
        for (const JsonValue& part : partitions->as_array()) {
            if (!part.is_object()) {
                continue;
            }
            out << std::left << std::setw(6) << json_int(part, "id") << std::setw(8)
                << json_int(part, "leader") << std::setw(16) << json_int(part, "highWatermark")
                << json_int(part, "leaderEpoch") << '\n';
        }
    }
    return 0;
}

}  // namespace nexus::cli
