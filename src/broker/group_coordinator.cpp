/// @file   broker/group_coordinator.cpp
/// @brief  Implementación de GroupCoordinator (mapa de grupos de consumidores del reactor).
/// @ingroup broker

#include "broker/group_coordinator.hpp"

#include <algorithm>
#include <string>
#include <utility>

namespace nexus {

ConsumerGroup* GroupCoordinator::find(std::string_view group_id) {
    const auto it = groups_.find(std::string{group_id});
    return it == groups_.end() ? nullptr : &it->second;
}

std::vector<GroupDigest> GroupCoordinator::list_groups() const {
    std::vector<GroupDigest> digests;
    digests.reserve(groups_.size());
    for (const auto& [id, group] : groups_) {
        digests.push_back(GroupDigest{.group_id = group.group_id(),
                                      .state = group.state(),
                                      .generation = group.generation(),
                                      .member_count = group.member_count()});
    }
    std::ranges::sort(digests, {}, &GroupDigest::group_id);
    return digests;
}

expected<JoinResult> GroupCoordinator::join(MonoTime now, const std::string& group_id,
                                            std::string member_id,
                                            std::vector<std::byte> subscription,
                                            std::chrono::milliseconds session_timeout) {
    ConsumerGroup& group = groups_.try_emplace(group_id, group_id).first->second;
    return group.join(now, std::move(member_id), std::move(subscription), session_timeout);
}

expected<SyncResult> GroupCoordinator::sync(MonoTime now, std::string_view group_id,
                                            std::string_view member_id, Generation generation,
                                            const std::vector<MemberAssignment>& assignments) {
    ConsumerGroup* group = find(group_id);
    if (group == nullptr) {
        return make_error(ErrorCode::NotFound, "sync: grupo desconocido");
    }
    return group->sync(now, member_id, generation, assignments);
}

expected<HeartbeatStatus> GroupCoordinator::heartbeat(MonoTime now, std::string_view group_id,
                                                      std::string_view member_id,
                                                      Generation generation) {
    ConsumerGroup* group = find(group_id);
    if (group == nullptr) {
        return make_error(ErrorCode::NotFound, "heartbeat: grupo desconocido");
    }
    return group->heartbeat(now, member_id, generation);
}

expected<void> GroupCoordinator::leave(std::string_view group_id, std::string_view member_id) {
    ConsumerGroup* group = find(group_id);
    if (group == nullptr) {
        return make_error(ErrorCode::NotFound, "leave: grupo desconocido");
    }
    return group->leave(member_id);
}

void GroupCoordinator::tick(MonoTime now) {
    for (auto& [id, group] : groups_) {
        group.tick(now);
    }
}

}  // namespace nexus
