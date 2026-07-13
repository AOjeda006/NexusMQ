/// @file   broker/offset_manager.cpp
/// @brief  Implementación de OffsetManager.
/// @ingroup broker

#include "broker/offset_manager.hpp"

#include <algorithm>
#include <tuple>
#include <utility>

namespace nexus {

void OffsetManager::commit(std::string group, std::string topic, PartitionId partition,
                           Offset offset, std::string metadata) {
    OffsetKey key{.group = std::move(group), .topic = std::move(topic), .partition = partition};
    commits_[std::move(key)] = Entry{.offset = offset, .metadata = std::move(metadata)};
}

expected<Offset> OffsetManager::fetch(std::string_view group, std::string_view topic,
                                      PartitionId partition) const {
    const OffsetKey key{
        .group = std::string{group}, .topic = std::string{topic}, .partition = partition};
    const auto it = commits_.find(key);
    if (it == commits_.end()) {
        return make_error(ErrorCode::NotFound, "el grupo no ha confirmado offset en esa partición");
    }
    return it->second.offset;
}

std::vector<GroupOffsetEntry> OffsetManager::list_for_group(std::string_view group) const {
    std::vector<GroupOffsetEntry> out;
    for (const auto& [key, entry] : commits_) {
        if (key.group == group) {
            out.push_back(GroupOffsetEntry{
                .topic = key.topic, .partition = key.partition, .offset = entry.offset});
        }
    }
    std::ranges::sort(out, {}, [](const GroupOffsetEntry& e) {
        return std::tie(e.topic, e.partition);
    });
    return out;
}

}  // namespace nexus
