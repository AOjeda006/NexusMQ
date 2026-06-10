#include "common/version.hpp"

namespace nexus {

std::string_view version() noexcept {
    return NEXUSMQ_VERSION;
}

}  // namespace nexus
