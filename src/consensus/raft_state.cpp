/// @file   consensus/raft_state.cpp
/// @brief  Implementación del progreso de réplica del líder (RaftVolatileState).
/// @ingroup consensus

#include "consensus/raft_state.hpp"

namespace nexus {

std::string_view raft_role_name(RaftRole role) noexcept {
    switch (role) {
        case RaftRole::Follower:
            return "follower";
        case RaftRole::PreCandidate:
            return "pre_candidate";
        case RaftRole::Candidate:
            return "candidate";
        case RaftRole::Leader:
            return "leader";
    }
    return "unknown";
}

void RaftVolatileState::reset_leader_progress(std::span<const NodeId> peers, Index last_log_index) {
    next_index_.clear();
    match_index_.clear();
    for (const NodeId peer : peers) {
        next_index_[peer] = last_log_index + 1;
        match_index_[peer] = 0;
    }
}

void RaftVolatileState::clear_leader_progress() noexcept {
    next_index_.clear();
    match_index_.clear();
}

Index RaftVolatileState::next_index(NodeId peer) const {
    const auto it = next_index_.find(peer);
    return it == next_index_.end() ? 0 : it->second;
}

Index RaftVolatileState::match_index(NodeId peer) const {
    const auto it = match_index_.find(peer);
    return it == match_index_.end() ? 0 : it->second;
}

void RaftVolatileState::set_next_index(NodeId peer, Index value) {
    next_index_[peer] = value;
}

void RaftVolatileState::set_match_index(NodeId peer, Index value) {
    match_index_[peer] = value;
}

}  // namespace nexus
