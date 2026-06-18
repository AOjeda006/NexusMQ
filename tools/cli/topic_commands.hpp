/// @file   cli/topic_commands.hpp
/// @brief  Subcomandos `topic` del CLI (create | list | describe | delete) sobre el REST admin.
/// @ingroup cli

#pragma once

#include <ostream>
#include <span>
#include <string_view>

#include "cli/admin_client.hpp"

namespace nexus::cli {

/// @brief Ejecuta `topic <sub> [args…]` contra @p client e imprime el resultado.
/// @details @p args son los tokens tras `topic` (`args[0]` es el subcomando). Imprime el resultado
///   en @p out y los errores en @p err. @return el código de salida (0 = ok; 1 = error de uso, de
///   transporte o respuesta `>= 400`).
[[nodiscard]] int run_topic(AdminClient& client, std::span<const std::string_view> args,
                            std::ostream& out, std::ostream& err);

}  // namespace nexus::cli
