/// @file   cli/partition_commands.hpp
/// @brief  Subcomando `partitions <topic>` del CLI (lista las particiones de un topic).
/// @ingroup cli

#pragma once

#include <ostream>
#include <span>
#include <string_view>

#include "cli/admin_client.hpp"

namespace nexus::cli {

/// @brief Ejecuta `partitions <topic>`: lista id/líder/high-watermark de cada partición.
/// @details Lee `GET /api/v1/topics/{name}` y tabula sus particiones. La reasignación
///   (`reassign`) es multi-nodo (Fase 4) y no se ofrece en el modo mono-nodo. @return exit code.
[[nodiscard]] int run_partitions(AdminClient& client, std::span<const std::string_view> args,
                                 std::ostream& out, std::ostream& err);

}  // namespace nexus::cli
