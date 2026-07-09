/// @file   server/daemon_args.hpp
/// @brief  Parseo de los argumentos de línea de comandos del daemon `nexusd`.
/// @ingroup server

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "server/server.hpp"

namespace nexus {

/// @brief Un topic a declarar al arrancar: `nombre` + número de particiones.
using DaemonTopicSpec = std::pair<std::string, std::int32_t>;

/// @brief Texto de uso del daemon (una línea), para diagnóstico ante un argumento inválido.
inline constexpr std::string_view kDaemonUsage =
    "uso: nexusd [--port N] [--admin-port N] [--kafka-port N] [--data-dir DIR] [--host H] "
    "[--node-id N] [--jwt-secret S] [--tls-cert FILE] [--tls-key FILE] [--topic nombre:parts]\n";

/// @brief Parsea los argumentos de `nexusd` sobre @p config y @p topics.
/// @param args  argv completo (incluye `argv[0]`, que se ignora).
/// @param config  configuración del servidor a poblar (se modifica in situ).
/// @param topics  topics a crear al arrancar (se van anexando).
/// @return `true` si todos los argumentos son válidos; `false` si hay uno desconocido o una opción
///   con valor a la que le falta el argumento. No emite diagnósticos: el llamante decide (el `main`
///   imprime `kDaemonUsage` por *stderr*).
/// @note Afinidad: pura, sin estado global; se prueba sin arrancar el daemon.
[[nodiscard]] bool parse_daemon_args(std::span<char* const> args, Server::Config& config,
                                     std::vector<DaemonTopicSpec>& topics);

}  // namespace nexus
