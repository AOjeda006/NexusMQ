/// @file   broker/group_catalog.cpp
/// @brief  Implementación de GroupCatalog (un GroupShard por núcleo, sharding ADR-0026).
/// @ingroup broker

#include "broker/group_catalog.hpp"

#include <algorithm>
#include <cstddef>

namespace nexus {

GroupCatalog::GroupCatalog(int num_cores) {
    const int cores = std::max(1, num_cores);
    shards_.reserve(static_cast<std::size_t>(cores));
    for (int core = 0; core < cores; ++core) {
        shards_.push_back(std::make_unique<GroupShard>());
    }
}

std::vector<GroupCoordinator*> GroupCatalog::all_groups() const {
    std::vector<GroupCoordinator*> out;
    out.reserve(shards_.size());
    for (const std::unique_ptr<GroupShard>& shard : shards_) {
        out.push_back(&shard->groups);
    }
    return out;
}

std::vector<OffsetManager*> GroupCatalog::all_offsets() const {
    std::vector<OffsetManager*> out;
    out.reserve(shards_.size());
    for (const std::unique_ptr<GroupShard>& shard : shards_) {
        out.push_back(&shard->offsets);
    }
    return out;
}

}  // namespace nexus
