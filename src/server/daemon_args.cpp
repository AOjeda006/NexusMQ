/// @file   server/daemon_args.cpp
/// @brief  Implementación del parseo de argumentos de `nexusd` (ver `daemon_args.hpp`).
/// @ingroup server

#include "server/daemon_args.hpp"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace nexus {

namespace {

/// Convierte @p text a entero; devuelve @p fallback si no es un número decimal completo y válido.
int parse_int(std::string_view text, int fallback) {
    int value = fallback;
    const char* begin = text.data();
    const char* end = begin + text.size();
    const std::from_chars_result result = std::from_chars(begin, end, value);
    if (result.ec != std::errc{} || result.ptr != end) {
        return fallback;
    }
    return value;
}

/// Parsea "nombre:particiones" (p. ej. "eventos:4"); particiones=1 si se omite o es inválido.
std::pair<std::string, std::int32_t> parse_topic_spec(std::string_view spec) {
    const std::size_t colon = spec.find(':');
    if (colon == std::string_view::npos) {
        return {std::string{spec}, 1};
    }
    const std::string name{spec.substr(0, colon)};
    const int partitions = parse_int(spec.substr(colon + 1), 1);
    return {name, partitions > 0 ? partitions : 1};
}

}  // namespace

bool parse_daemon_args(std::span<char* const> args, Server::Config& config,
                       std::vector<DaemonTopicSpec>& topics) {
    for (std::size_t i = 1; i < args.size(); ++i) {
        const std::string_view arg{args[i]};
        const bool has_next = i + 1 < args.size();
        if (arg == "--port" && has_next) {
            config.port = static_cast<std::uint16_t>(parse_int(args[++i], config.port));
        } else if (arg == "--data-dir" && has_next) {
            config.data_dir = args[++i];
        } else if (arg == "--host" && has_next) {
            config.host = args[++i];
            config.advertised_host = config.host;
        } else if (arg == "--admin-port" && has_next) {
            config.admin_port = static_cast<std::uint16_t>(parse_int(args[++i], 0));
        } else if (arg == "--kafka-port" && has_next) {
            config.kafka_port = static_cast<std::uint16_t>(parse_int(args[++i], 0));
        } else if (arg == "--jwt-secret" && has_next) {
            config.jwt_secret = args[++i];
        } else if (arg == "--tls-cert" && has_next) {
            config.tls.cert_chain = args[++i];
        } else if (arg == "--tls-key" && has_next) {
            config.tls.private_key = args[++i];
        } else if (arg == "--node-id" && has_next) {
            config.node_id = static_cast<NodeId>(parse_int(args[++i], config.node_id));
        } else if (arg == "--topic" && has_next) {
            topics.push_back(parse_topic_spec(args[++i]));
        } else {
            return false;
        }
    }
    return true;
}

}  // namespace nexus
