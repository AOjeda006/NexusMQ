/// @file   protocol/versioning.hpp
/// @brief  Negociación de versión de API entre cliente y servidor.
/// @ingroup protocol

#pragma once

#include <cstdint>

#include "protocol/frame.hpp"

namespace nexus {

/// @brief Rango de versiones soportadas por el servidor para una `ApiKey`. Afinidad: INMUTABLE.
struct ApiVersionRange {
    ApiKey key = ApiKey::ApiVersions;
    std::uint16_t min = 0;
    std::uint16_t max = 0;

    bool operator==(const ApiVersionRange&) const = default;
};

/// @brief Versión a usar: la mayor que ambos soportan. 0 si no hay solape.
/// @details El cliente soporta `[0, client_max]`; el servidor `[server.min, server.max]`. Devuelve
///   `min(client_max, server.max)` si alcanza `server.min`, o 0 (la `ApiKey` no es negociable).
[[nodiscard]] std::uint16_t negotiate(std::uint16_t client_max,
                                      const ApiVersionRange& server) noexcept;

}  // namespace nexus
