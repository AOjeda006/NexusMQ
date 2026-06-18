/// @file   cli/admin_commands.hpp
/// @brief  Subcomandos de operación del CLI: `metrics` y `diagnostics`.
/// @ingroup cli

#pragma once

#include <ostream>

#include "cli/admin_client.hpp"

namespace nexus::cli {

/// @brief Ejecuta `metrics`: vuelca la exposición Prometheus de `/metrics`. @return exit code.
[[nodiscard]] int run_metrics(AdminClient& client, std::ostream& out, std::ostream& err);

/// @brief Ejecuta `diagnostics`: consulta `/healthz` y `/readyz` e imprime un resumen de salud.
/// @return 0 si vivo **y** listo; 1 en caso contrario o ante error de transporte.
[[nodiscard]] int run_diagnostics(AdminClient& client, std::ostream& out, std::ostream& err);

}  // namespace nexus::cli
