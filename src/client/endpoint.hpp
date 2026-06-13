/// @file   client/endpoint.hpp
/// @brief  Endpoint: dirección (host:puerto) de un broker, para el cliente nativo.
/// @ingroup client

#pragma once

#include <cstdint>
#include <string>

namespace nexus {

/// @brief Dirección de un broker (host IPv4 punteado + puerto TCP). Afinidad: INMUTABLE.
struct Endpoint {
    std::string host;
    std::uint16_t port = 0;

    bool operator==(const Endpoint&) const = default;
};

}  // namespace nexus
