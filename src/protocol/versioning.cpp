#include "protocol/versioning.hpp"

#include <algorithm>

namespace nexus {

std::uint16_t negotiate(std::uint16_t client_max, const ApiVersionRange& server) noexcept {
    const std::uint16_t common = std::min(client_max, server.max);
    return common >= server.min ? common : 0;
}

}  // namespace nexus
