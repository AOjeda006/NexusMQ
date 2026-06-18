/// @file   cli/group_commands.hpp
/// @brief  Subcomandos `group` del CLI (list | describe) sobre el REST admin.
/// @ingroup cli

#pragma once

#include <ostream>
#include <span>
#include <string_view>

#include "cli/admin_client.hpp"

namespace nexus::cli {

/// @brief Ejecuta `group <sub> [args…]` (`list`, `describe <id>`) contra @p client.
/// @details Lista los grupos (`GET /api/v1/groups`) o describe uno filtrando el listado por id
///   (el REST admin aún no expone `/groups/{id}`). @return el código de salida (0 = ok).
[[nodiscard]] int run_group(AdminClient& client, std::span<const std::string_view> args,
                            std::ostream& out, std::ostream& err);

}  // namespace nexus::cli
