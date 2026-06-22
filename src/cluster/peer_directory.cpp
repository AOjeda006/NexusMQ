/// @file   cluster/peer_directory.cpp
/// @brief  Implementación de PeerDirectory (resolución NodeId -> dirección inter-nodo).
/// @ingroup cluster

#include "cluster/peer_directory.hpp"

#include <algorithm>
#include <utility>

namespace nexus {

PeerDirectory::PeerDirectory(std::unordered_map<NodeId, PeerAddress> peers) noexcept
    : peers_(std::move(peers)) {}

const PeerAddress* PeerDirectory::find(NodeId node) const noexcept {
    const auto it = peers_.find(node);
    return it == peers_.end() ? nullptr : &it->second;
}

bool PeerDirectory::contains(NodeId node) const noexcept {
    return peers_.contains(node);
}

std::vector<NodeId> PeerDirectory::node_ids() const {
    std::vector<NodeId> ids;
    ids.reserve(peers_.size());
    for (const auto& [node, address] : peers_) {
        ids.push_back(node);
    }
    std::ranges::sort(ids);
    return ids;
}

}  // namespace nexus
