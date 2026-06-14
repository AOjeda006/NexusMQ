// Arnés de simulación determinista de Raft (ADR-0015): N RaftNode sobre un VirtualClock y una
// VirtualNetwork. Todo es síncrono y reproducible (semillas fijas) — sin hilos, sin sockets, sin
// reloj real. Permite ejercitar elecciones, particiones y failover, y verificar invariantes de
// seguridad (a lo sumo un líder por término; las entradas confirmadas nunca divergen).
#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "common/record.hpp"
#include "common/types.hpp"
#include "consensus/raft_node.hpp"
#include "consensus/raft_rpc.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace nexus::sim {

using Millis = std::chrono::milliseconds;

/// Reloj lógico inyectable: avanza solo cuando la simulación lo ordena.
class VirtualClock {
public:
    [[nodiscard]] MonoTime now() const noexcept { return now_; }
    void advance(Millis delta) noexcept { now_ += delta; }

private:
    MonoTime now_{};
};

/// Un RPC en vuelo por la red virtual, con su instante de entrega.
struct InFlight {
    MonoTime due;
    RaftMessage msg;
};

/// Directorio temporal autolimpiante para el log de un nodo simulado.
class SimDir {
public:
    explicit SimDir(const std::string& tag)
        : path_(std::filesystem::temp_directory_path() / ("nexus_raftsim_" + tag)) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~SimDir() { std::filesystem::remove_all(path_); }
    SimDir(const SimDir&) = delete;
    SimDir& operator=(const SimDir&) = delete;

    [[nodiscard]] std::string meta_path() const { return (path_ / "raft-meta").string(); }
    [[nodiscard]] std::filesystem::path log_dir() const { return path_ / "log"; }

private:
    std::filesystem::path path_;
};

/// Un nodo simulado completo: su almacenamiento + su RaftNode + su grupo de partición de red.
struct SimNode {
    std::unique_ptr<SimDir> dir;
    std::unique_ptr<PartitionLog> plog;
    std::unique_ptr<RaftLog> rlog;
    std::unique_ptr<RaftNode> node;
    bool up = true;     ///< false tras `crash` (no procesa ni emite).
    int partition = 0;  ///< grupo de partición de red (mismo grupo ⇒ se alcanzan).
};

/// @brief Cluster de Raft determinista sobre reloj y red virtuales (afinidad: test).
/// @details Entrega RPC con latencia fija; las respuestas de `on_*` también viajan por la red (con
///   latencia), modelando un sistema asíncrono real. `partition`/`heal` y `crash`/`restart`
///   permiten inyectar fallos. En cada paso registra invariantes de seguridad.
class Cluster {
public:
    Cluster(std::vector<NodeId> voters, std::uint64_t seed, std::vector<NodeId> learners = {}) {
        std::vector<NodeId> all = voters;
        all.insert(all.end(), learners.begin(), learners.end());
        for (const NodeId id : all) {
            std::vector<NodeId> peers;
            for (const NodeId other : all) {
                if (other != id) {
                    peers.push_back(other);
                }
            }
            auto box = std::make_unique<SimNode>();
            box->dir = std::make_unique<SimDir>(std::to_string(seed) + "_" + std::to_string(id));
            auto plog = PartitionLog::open(box->dir->log_dir(), LogConfig{});
            box->plog = std::make_unique<PartitionLog>(std::move(*plog));
            auto rlog = RaftLog::open(*box->plog, box->dir->meta_path());
            box->rlog = std::make_unique<RaftLog>(std::move(*rlog));
            RaftConfig cfg;
            cfg.random_seed = seed;
            box->node = std::make_unique<RaftNode>(id, std::move(peers), *box->rlog, cfg, learners);
            nodes_.emplace(id, std::move(box));
        }
    }

    [[nodiscard]] MonoTime now() const noexcept { return clock_.now(); }
    [[nodiscard]] RaftNode& node(NodeId id) { return *nodes_.at(id)->node; }
    [[nodiscard]] RaftLog& log(NodeId id) { return *nodes_.at(id)->rlog; }

    /// Avanza la simulación `total` ms en pasos de `step` ms (por defecto 10).
    void run_for(Millis total, Millis step = Millis{10}) {
        for (Millis elapsed{0}; elapsed < total; elapsed += step) {
            tick(step);
        }
    }

    /// Avanza la simulación hasta que `pred()` sea cierto o se agote `limit`. Devuelve si se
    /// cumplió.
    template <class Pred>
    bool run_until(Pred pred, Millis limit, Millis step = Millis{10}) {
        for (Millis elapsed{0}; elapsed < limit; elapsed += step) {
            tick(step);
            if (pred()) {
                return true;
            }
        }
        return pred();
    }

    /// Particiona la red: los nodos de `side` quedan en un grupo aparte del resto.
    void partition(const std::vector<NodeId>& side) {
        for (auto& [id, box] : nodes_) {
            box->partition = 0;
        }
        for (const NodeId id : side) {
            nodes_.at(id)->partition = 1;
        }
    }

    /// Sana la red: todos vuelven al mismo grupo.
    void heal() {
        for (auto& [id, box] : nodes_) {
            box->partition = 0;
        }
    }

    void crash(NodeId id) { nodes_.at(id)->up = false; }
    void restart(NodeId id) { nodes_.at(id)->up = true; }

    [[nodiscard]] bool is_up(NodeId id) const { return nodes_.at(id)->up; }

    /// Número de nodos vivos que se creen líderes ahora mismo.
    [[nodiscard]] std::size_t leader_count() const {
        std::size_t count = 0;
        for (const auto& [id, box] : nodes_) {
            if (box->up && box->node->is_leader()) {
                ++count;
            }
        }
        return count;
    }

    /// El líder vivo (si hay exactamente uno).
    [[nodiscard]] std::optional<NodeId> leader() const {
        std::optional<NodeId> found;
        for (const auto& [id, box] : nodes_) {
            if (box->up && box->node->is_leader()) {
                if (found) {
                    return std::nullopt;  // más de uno: ambiguo.
                }
                found = id;
            }
        }
        return found;
    }

    [[nodiscard]] bool invariants_hold() const { return violations_.empty(); }
    [[nodiscard]] const std::vector<std::string>& violations() const { return violations_; }

private:
    void tick(Millis step) {
        clock_.advance(step);
        const MonoTime t = clock_.now();
        for (auto& [id, box] : nodes_) {
            if (box->up) {
                box->node->tick(t);
            }
        }
        pump_outboxes(t);  // mensajes proactivos (RequestVote/AppendEntries/heartbeats/TimeoutNow).
        deliver_due(t);    // entrega lo que vence; los handlers producen respuestas y más salida.
        pump_outboxes(t);
        record_invariants();
    }

    [[nodiscard]] bool reachable(NodeId from, NodeId to) const {
        const SimNode& a = *nodes_.at(from);
        const SimNode& b = *nodes_.at(to);
        return a.up && b.up && a.partition == b.partition;
    }

    void schedule(MonoTime now, RaftMessage msg) {
        if (!nodes_.contains(msg.to) || !reachable(msg.from, msg.to)) {
            return;  // destino inalcanzable: el mensaje se pierde (partición/caída).
        }
        wire_.push_back(InFlight{.due = now + kLatency, .msg = std::move(msg)});
    }

    void pump_outboxes(MonoTime now) {
        for (auto& [id, box] : nodes_) {
            if (!box->up) {
                continue;
            }
            for (RaftMessage& msg : box->node->take_messages()) {
                schedule(now, std::move(msg));
            }
        }
    }

    void deliver_due(MonoTime now) {
        std::vector<InFlight> due;
        std::vector<InFlight> rest;
        for (InFlight& f : wire_) {
            (f.due <= now ? due : rest).push_back(std::move(f));
        }
        wire_ = std::move(rest);
        for (const InFlight& f : due) {
            deliver(now, f.msg);
        }
    }

    void deliver(MonoTime now, const RaftMessage& msg) {
        if (!nodes_.contains(msg.to) || !nodes_.at(msg.to)->up) {
            return;
        }
        RaftNode& target = *nodes_.at(msg.to)->node;
        if (const auto* a = std::get_if<RequestVoteArgs>(&msg.payload)) {
            const auto reply = target.on_request_vote(now, *a);
            schedule(now, RaftMessage{.from = msg.to, .to = msg.from, .payload = reply});
        } else if (const auto* a = std::get_if<AppendEntriesArgs>(&msg.payload)) {
            const auto reply = target.on_append_entries(now, *a);
            schedule(now, RaftMessage{.from = msg.to, .to = msg.from, .payload = reply});
        } else if (const auto* a = std::get_if<TimeoutNowArgs>(&msg.payload)) {
            target.on_timeout_now(now, *a);
        } else if (const auto* r = std::get_if<RequestVoteReply>(&msg.payload)) {
            target.on_request_vote_reply(now, msg.from, *r);
        } else if (const auto* r = std::get_if<AppendEntriesReply>(&msg.payload)) {
            target.on_append_entries_reply(now, msg.from, *r);
        }
    }

    void record_invariants() {
        // Invariante 1: a lo sumo un líder por término (Election Safety, §5.2).
        for (const auto& [id, box] : nodes_) {
            if (!box->up || !box->node->is_leader()) {
                continue;
            }
            const Term term = box->node->current_term();
            const auto seen = term_leader_.find(term);
            if (seen != term_leader_.end() && seen->second != id) {
                violations_.push_back("dos líderes en el término " + std::to_string(term));
            } else {
                term_leader_[term] = id;
            }
        }
        // Invariante 2: las entradas confirmadas no divergen (State Machine Safety, §5.4): el
        // término de cada índice confirmado es estable entre todos los nodos.
        for (const auto& [id, box] : nodes_) {
            if (!box->up) {
                continue;
            }
            const Index commit = box->node->commit_index();
            for (Index i = 1; i <= commit; ++i) {
                const auto term = box->rlog->term_at(i);
                if (!term) {
                    continue;
                }
                const auto seen = committed_term_.find(i);
                if (seen != committed_term_.end() && seen->second != *term) {
                    violations_.push_back("entrada confirmada divergente en el índice " +
                                          std::to_string(i));
                } else {
                    committed_term_[i] = *term;
                }
            }
        }
    }

    static constexpr Millis kLatency{10};

    VirtualClock clock_;
    std::unordered_map<NodeId, std::unique_ptr<SimNode>> nodes_;
    std::vector<InFlight> wire_;
    std::unordered_map<Term, NodeId> term_leader_;
    std::unordered_map<Index, Term> committed_term_;
    std::vector<std::string> violations_;
};

}  // namespace nexus::sim
