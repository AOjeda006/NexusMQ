/// @file   ingress/load_balancer.cpp
/// @brief  Implementación de LoadBalancer (round-robin / least-conn / consistent-hash).
/// @ingroup ingress

#include "ingress/load_balancer.hpp"

#include <algorithm>
#include <string>

namespace nexus {

LoadBalancer::LoadBalancer(BalanceStrategy strategy, std::uint32_t vnodes)
    : strategy_(strategy), vnodes_(std::max<std::uint32_t>(1, vnodes)) {}

void LoadBalancer::add_node(NodeId node) {
    const auto pos = std::ranges::lower_bound(nodes_, node);
    if (pos != nodes_.end() && *pos == node) {
        return;  // ya estaba.
    }
    nodes_.insert(pos, node);
    active_.try_emplace(node, 0);
    if (strategy_ == BalanceStrategy::ConsistentHashing) {
        rebuild_ring();
    }
}

void LoadBalancer::remove_node(NodeId node) {
    const auto pos = std::ranges::find(nodes_, node);
    if (pos == nodes_.end()) {
        return;  // no estaba.
    }
    nodes_.erase(pos);
    active_.erase(node);
    if (cursor_ >= nodes_.size()) {
        cursor_ = 0;
    }
    if (strategy_ == BalanceStrategy::ConsistentHashing) {
        rebuild_ring();
    }
}

std::optional<NodeId> LoadBalancer::pick(std::string_view key) {
    if (nodes_.empty()) {
        return std::nullopt;
    }
    switch (strategy_) {
        case BalanceStrategy::RoundRobin: {
            const NodeId chosen = nodes_[cursor_];
            cursor_ = (cursor_ + 1) % nodes_.size();
            return chosen;
        }
        case BalanceStrategy::LeastConnections: {
            NodeId best = nodes_.front();
            std::size_t best_active = active(best);
            for (const NodeId node : nodes_) {
                // `nodes_` está ordenado ascendente, así que el primer mínimo (con `<` estricto)
                // desempata por el menor id de forma determinista.
                if (const std::size_t a = active(node); a < best_active) {
                    best = node;
                    best_active = a;
                }
            }
            return best;
        }
        case BalanceStrategy::ConsistentHashing: {
            if (ring_.empty()) {
                return std::nullopt;
            }
            const std::uint64_t point = hash64(key);
            auto it = ring_.lower_bound(point);
            if (it == ring_.end()) {
                it = ring_.begin();  // el anillo cierra: vuelve al primer punto.
            }
            return it->second;
        }
    }
    return std::nullopt;
}

void LoadBalancer::on_acquire(NodeId node) {
    ++active_[node];
}

void LoadBalancer::on_release(NodeId node) {
    const auto it = active_.find(node);
    if (it != active_.end() && it->second > 0) {
        --it->second;
    }
}

std::size_t LoadBalancer::active(NodeId node) const {
    const auto it = active_.find(node);
    return it == active_.end() ? 0 : it->second;
}

std::uint64_t LoadBalancer::hash64(std::string_view bytes) noexcept {
    std::uint64_t hash = 14695981039346656037ULL;  // offset basis FNV-1a 64.
    for (const char byte : bytes) {
        hash ^= static_cast<std::uint8_t>(byte);
        hash *= 1099511628211ULL;  // primo FNV.
    }
    return hash;
}

void LoadBalancer::rebuild_ring() {
    ring_.clear();
    for (const NodeId node : nodes_) {
        for (std::uint32_t replica = 0; replica < vnodes_; ++replica) {
            const std::string vkey = std::to_string(node) + "#" + std::to_string(replica);
            ring_.emplace(hash64(vkey), node);
        }
    }
}

}  // namespace nexus
