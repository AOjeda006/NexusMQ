/// @file   broker/consumer_group.cpp
/// @brief  Implementación de la FSM de membresía de un grupo de consumidores.
/// @ingroup broker

#include "broker/consumer_group.hpp"

#include <algorithm>
#include <string>
#include <utility>

namespace nexus {

void ConsumerGroup::begin_rebalance() {
    state_ = GroupState::PreparingRebalance;
    leader_id_.clear();  // el primero en reincorporarse será el nuevo líder.
    for (auto& [id, member] : members_) {
        member.joined_round = false;
    }
}

void ConsumerGroup::complete_if_ready() {
    if (state_ != GroupState::PreparingRebalance || members_.empty()) {
        return;
    }
    for (const auto& [id, member] : members_) {
        if (!member.joined_round) {
            return;  // falta alguien por reincorporarse: sigue en PreparingRebalance.
        }
    }
    state_ = GroupState::CompletingRebalance;
    ++generation_;
    for (auto& [id, member] : members_) {
        member.assignment.clear();  // nueva ronda: las asignaciones previas quedan obsoletas.
    }
}

std::string ConsumerGroup::next_member_id() {
    return group_id_ + "-" + std::to_string(++member_seq_);
}

expected<JoinResult> ConsumerGroup::join(MonoTime now, std::string member_id,
                                         std::vector<std::byte> subscription,
                                         std::chrono::milliseconds session_timeout) {
    if (state_ == GroupState::Dead) {
        return make_error(ErrorCode::Unsupported, "join: el grupo está eliminado");
    }
    const std::string id = member_id.empty() ? next_member_id() : std::move(member_id);
    if (state_ == GroupState::Stable || state_ == GroupState::Empty) {
        begin_rebalance();  // un cambio de membresía sobre un grupo estable abre rebalanceo.
    }
    Member& member = members_[id];
    member.subscription = std::move(subscription);
    member.session_timeout = session_timeout;
    member.last_seen = now;
    member.joined_round = true;
    if (leader_id_.empty()) {
        leader_id_ = id;  // el primero en reincorporarse a la ronda es el líder.
    }
    complete_if_ready();

    JoinResult result;
    result.member_id = id;
    result.generation = generation_;
    result.leader_id = leader_id_;
    result.is_leader = (id == leader_id_);
    if (result.is_leader && state_ == GroupState::CompletingRebalance) {
        result.members = members();  // el líder reparte sobre esta lista.
    }
    return result;
}

expected<SyncResult> ConsumerGroup::sync(MonoTime now, std::string_view member_id,
                                         Generation generation,
                                         const std::vector<MemberAssignment>& assignments) {
    if (state_ == GroupState::Dead) {
        return make_error(ErrorCode::Unsupported, "sync: el grupo está eliminado");
    }
    const auto it = members_.find(std::string{member_id});
    if (it == members_.end()) {
        return make_error(ErrorCode::NotFound, "sync: miembro desconocido");
    }
    if (generation != generation_) {
        return make_error(ErrorCode::InvalidArgument, "sync: generación obsoleta");
    }
    it->second.last_seen = now;

    if (state_ == GroupState::Stable) {
        return SyncResult{
            .generation = generation_, .assigned = true, .assignment = it->second.assignment};
    }
    if (state_ != GroupState::CompletingRebalance) {
        return SyncResult{.generation = generation_, .assigned = false, .assignment = {}};
    }
    if (it->first == leader_id_) {
        for (const MemberAssignment& entry : assignments) {
            const auto target = members_.find(entry.member_id);
            if (target != members_.end()) {
                target->second.assignment = entry.assignment;
            }
        }
        state_ = GroupState::Stable;  // el líder ya repartió: el grupo queda estable.
        return SyncResult{
            .generation = generation_, .assigned = true, .assignment = it->second.assignment};
    }
    return SyncResult{.generation = generation_, .assigned = false, .assignment = {}};
}

expected<HeartbeatStatus> ConsumerGroup::heartbeat(MonoTime now, std::string_view member_id,
                                                   Generation generation) {
    if (state_ == GroupState::Dead) {
        return make_error(ErrorCode::Unsupported, "heartbeat: el grupo está eliminado");
    }
    const auto it = members_.find(std::string{member_id});
    if (it == members_.end()) {
        return make_error(ErrorCode::NotFound, "heartbeat: miembro desconocido");
    }
    it->second.last_seen = now;
    if (state_ == GroupState::PreparingRebalance || state_ == GroupState::CompletingRebalance) {
        return HeartbeatStatus::RebalanceInProgress;  // debe re-join (la generación cambiará).
    }
    if (generation != generation_) {
        return make_error(ErrorCode::InvalidArgument, "heartbeat: generación obsoleta");
    }
    return HeartbeatStatus::Ok;
}

expected<void> ConsumerGroup::leave(std::string_view member_id) {
    if (state_ == GroupState::Dead) {
        return make_error(ErrorCode::Unsupported, "leave: el grupo está eliminado");
    }
    const auto it = members_.find(std::string{member_id});
    if (it == members_.end()) {
        return make_error(ErrorCode::NotFound, "leave: miembro desconocido");
    }
    members_.erase(it);
    if (members_.empty()) {
        state_ = GroupState::Empty;
        leader_id_.clear();
    } else {
        begin_rebalance();  // la baja cambia la membresía: rebalanceo.
    }
    return {};
}

void ConsumerGroup::tick(MonoTime now) {
    if (state_ == GroupState::Dead) {
        return;
    }
    bool removed = false;
    for (auto it = members_.begin(); it != members_.end();) {
        if (now - it->second.last_seen > it->second.session_timeout) {
            it = members_.erase(it);
            removed = true;
        } else {
            ++it;
        }
    }
    if (!removed) {
        return;
    }
    if (members_.empty()) {
        state_ = GroupState::Empty;
        leader_id_.clear();
    } else {
        begin_rebalance();  // alguien expiró: la membresía cambió, rebalancea.
    }
}

bool ConsumerGroup::contains(std::string_view member_id) const {
    return members_.contains(std::string{member_id});
}

std::vector<MemberInfo> ConsumerGroup::members() const {
    std::vector<MemberInfo> out;
    out.reserve(members_.size());
    for (const auto& [id, member] : members_) {
        out.push_back(MemberInfo{.member_id = id, .subscription = member.subscription});
    }
    std::ranges::sort(out, {}, &MemberInfo::member_id);
    return out;
}

}  // namespace nexus
