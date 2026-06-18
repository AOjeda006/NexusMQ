/// @file   cli/cli.hpp
/// @brief  nexus-cli: opciones globales, parseo y despacho de subcomandos.
/// @ingroup cli

#pragma once

#include <cstdint>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "common/error.hpp"

namespace nexus::cli {

/// @brief Opciones globales del CLI (destino del REST admin). Afinidad: INMUTABLE.
struct GlobalOptions {
    std::string host = "127.0.0.1";  ///< Host del puerto de operación.
    std::uint16_t port = 8080;       ///< Puerto de operación.
    std::string token;               ///< Token JWT Bearer (vacío = sin autenticación).
};

/// @brief Resultado de separar las opciones globales del comando. Afinidad: INMUTABLE.
struct ParsedArgs {
    GlobalOptions options;
    std::vector<std::string_view> rest;  ///< comando + sus argumentos.
};

/// @brief Parsea las opciones globales (`--host`/`--port`/`--token`) hasta el primer no-flag.
/// @details Acepta `--flag valor` y `--flag=valor`. `InvalidArgument` ante un flag desconocido o un
///   puerto no numérico/fuera de rango.
[[nodiscard]] expected<ParsedArgs> parse_global_options(std::span<const std::string_view> args);

/// @brief Ejecuta el CLI: parsea, construye el cliente HTTP y despacha el comando.
/// @return el código de salida del proceso (0 = ok).
[[nodiscard]] int run_cli(std::span<const std::string_view> args, std::ostream& out,
                          std::ostream& err);

}  // namespace nexus::cli
