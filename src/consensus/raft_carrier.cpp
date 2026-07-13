/// @file   consensus/raft_carrier.cpp
/// @brief  Implementación de RaftCarrier: enrutado de RPC y transporte de la FSM (ADR-0025).
/// @ingroup consensus

#include "consensus/raft_carrier.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <variant>

#include "consensus/raft_log.hpp"
#include "consensus/raft_node.hpp"
#include "consensus/raft_state.hpp"
#include "consensus/raft_state_store.hpp"
#include "telemetry/metrics.hpp"

namespace nexus {

RaftCarrier::RaftCarrier(std::string topic, PartitionId partition, RaftNode& node,
                         RaftMessageSink& sink, RaftStateStore* store, RaftLog* log,
                         CompactionPolicy compaction)
    : topic_(std::move(topic)),
      partition_(partition),
      node_(node),
      sink_(sink),
      store_(store),
      log_(log),
      compaction_(compaction) {}

RaftObservation RaftCarrier::observe() const {
    RaftObservation obs;
    obs.topic = topic_;
    obs.partition = partition_;
    obs.role = node_.role();
    obs.term = node_.current_term();
    obs.commit_index = node_.commit_index();
    obs.last_log_index = node_.last_log_index();
    obs.leader_epoch = node_.leader_epoch();
    obs.leader_hint = node_.leader_hint();
    for (const NodeId peer : node_.peers()) {
        obs.peers.push_back(
            RaftPeerObservation{.peer = peer, .match_index = node_.match_index(peer)});
    }
    return obs;
}

void RaftCarrier::set_metrics(MetricsRegistry& metrics) {
    metrics.describe("nexus_raft_commit_index",
                     "High-watermark de la réplica de Raft (entradas aplicadas).");
    metrics.describe("nexus_raft_term", "Término actual de la réplica de Raft.");
    metrics.describe("nexus_raft_leader", "Rol de la réplica: 1 si es líder, 0 en otro caso.");
    metrics.describe("nexus_raft_log_last_index", "Último índice del log local de la réplica.");
    metrics.describe("nexus_raft_uncommitted_entries",
                     "Entradas escritas aún no confirmadas a quórum (last_log_index - commit).");
    metrics.describe("nexus_raft_follower_lag",
                     "Retraso de un seguidor: last_log_index - match_index (visto por el líder).");
    metrics.describe("nexus_raft_messages_sent_total",
                     "Mensajes de Raft transportados por la réplica.");
    metrics.describe("nexus_raft_messages_received_total",
                     "RPC de Raft entregados a la FSM de la réplica.");
    metrics.describe("nexus_raft_entries_replicated_total",
                     "Entradas enviadas en AppendEntries por el líder.");
    metrics.describe("nexus_raft_commit_latency_seconds",
                     "Latencia de confirmación a quórum de una entrada (propose -> commit).");

    const Labels labels{{"topic", topic_}, {"partition", std::to_string(partition_)}};
    metrics_.commit_index = &metrics.gauge("nexus_raft_commit_index", labels);
    metrics_.term = &metrics.gauge("nexus_raft_term", labels);
    metrics_.leader = &metrics.gauge("nexus_raft_leader", labels);
    metrics_.log_last_index = &metrics.gauge("nexus_raft_log_last_index", labels);
    metrics_.uncommitted_entries = &metrics.gauge("nexus_raft_uncommitted_entries", labels);
    metrics_.messages_sent = &metrics.counter("nexus_raft_messages_sent_total", labels);
    metrics_.messages_received = &metrics.counter("nexus_raft_messages_received_total", labels);
    metrics_.entries_replicated = &metrics.counter("nexus_raft_entries_replicated_total", labels);
    metrics_.commit_latency = &metrics.histogram("nexus_raft_commit_latency_seconds", labels);

    // Un gauge de *lag* por peer (conjunto fijo): se etiqueta además con el peer.
    follower_lag_.clear();
    follower_lag_.reserve(node_.peers().size());
    for (const NodeId peer : node_.peers()) {
        Labels peer_labels = labels;
        peer_labels.emplace_back("peer", std::to_string(peer));
        follower_lag_.push_back(
            {.peer = peer,
             .lag = &metrics.gauge("nexus_raft_follower_lag", std::move(peer_labels))});
    }
    publish_state();
}

expected<void> RaftCarrier::recover() {
    if (store_ == nullptr) {
        return {};
    }
    expected<RaftPersistentState> loaded = store_->load();
    if (!loaded) {
        return std::unexpected(loaded.error());
    }
    node_.restore_persistent_state(*loaded);
    return {};
}

void RaftCarrier::note_proposed(MonoTime proposed_at) {
    if (metrics_.commit_latency == nullptr || !node_.is_leader()) {
        return;
    }
    pending_commits_[node_.last_log_index()] = proposed_at;
}

void RaftCarrier::on_tick(MonoTime now) {
    node_.tick(now);
    flush_outbox();
    maybe_compact();
    publish_state();
    observe_commits(now);
}

void RaftCarrier::on_message(MonoTime now, const RaftMessage& message) {
    if (metrics_.messages_received != nullptr) {
        metrics_.messages_received->inc();
    }
    const NodeId from = message.from;
    if (const auto* args = std::get_if<RequestVoteArgs>(&message.payload)) {
        emit(RaftMessage{
            .from = node_.id(), .to = from, .payload = node_.on_request_vote(now, *args)});
    } else if (const auto* args = std::get_if<AppendEntriesArgs>(&message.payload)) {
        emit(RaftMessage{
            .from = node_.id(), .to = from, .payload = node_.on_append_entries(now, *args)});
    } else if (const auto* args = std::get_if<InstallSnapshotArgs>(&message.payload)) {
        emit(RaftMessage{
            .from = node_.id(), .to = from, .payload = node_.on_install_snapshot(now, *args)});
    } else if (const auto* args = std::get_if<TimeoutNowArgs>(&message.payload)) {
        node_.on_timeout_now(now, *args);
    } else if (const auto* reply = std::get_if<RequestVoteReply>(&message.payload)) {
        node_.on_request_vote_reply(now, from, *reply);
    } else if (const auto* reply = std::get_if<AppendEntriesReply>(&message.payload)) {
        node_.on_append_entries_reply(now, from, *reply);
    } else if (const auto* reply = std::get_if<InstallSnapshotReply>(&message.payload)) {
        node_.on_install_snapshot_reply(now, from, *reply);
    }
    flush_outbox();
    maybe_compact();
    publish_state();
    observe_commits(now);
}

void RaftCarrier::flush_outbox() {
    if (store_ != nullptr && node_.persistent_state_dirty()) {
        if (!store_->save(node_.persistent_state())) {
            return;  // §5: no transportar antes de persistir; se reintenta en el próximo tick.
        }
        node_.clear_persistent_dirty();
    }
    for (const RaftMessage& message : node_.take_messages()) {
        emit(message);
    }
}

void RaftCarrier::maybe_compact() {
    if (log_ == nullptr || compaction_.applied_entries_threshold == 0) {
        return;
    }
    const Index commit = node_.commit_index();
    const Index base = log_->snapshot_index();
    if (commit <= base || (commit - base) < compaction_.applied_entries_threshold) {
        return;
    }
    // Best-effort: compacta el prefijo ya replicado en mayoría y aplicado (commit_index). Un fallo
    // de E/S deja las entradas en su sitio y se reintenta en el próximo tick (no rompe el
    // consenso).
    [[maybe_unused]] const expected<void> compacted = log_->compact_to(commit);
}

void RaftCarrier::emit(const RaftMessage& message) {
    if (metrics_.messages_sent != nullptr) {
        metrics_.messages_sent->inc();
        if (const auto* args = std::get_if<AppendEntriesArgs>(&message.payload)) {
            metrics_.entries_replicated->inc(args->entries.size());
        }
    }
    sink_.send(RaftEnvelope{.topic = topic_, .partition = partition_, .message = message});
}

void RaftCarrier::publish_state() {
    if (metrics_.commit_index == nullptr) {
        return;
    }
    const Index commit = node_.commit_index();
    const Index last = node_.last_log_index();
    metrics_.commit_index->set(static_cast<std::int64_t>(commit));
    metrics_.term->set(static_cast<std::int64_t>(node_.current_term()));
    metrics_.leader->set(node_.is_leader() ? 1 : 0);
    metrics_.log_last_index->set(static_cast<std::int64_t>(last));
    metrics_.uncommitted_entries->set(
        static_cast<std::int64_t>(last >= commit ? last - commit : 0));

    // Solo el líder sigue el progreso de los seguidores (`match_index`); un seguidor no lo conoce.
    const bool leader = node_.is_leader();
    for (const FollowerLagGauge& follower : follower_lag_) {
        const Index matched = leader ? node_.match_index(follower.peer) : last;
        follower.lag->set(static_cast<std::int64_t>(last >= matched ? last - matched : 0));
    }
}

void RaftCarrier::observe_commits(MonoTime now) {
    if (metrics_.commit_latency == nullptr) {
        return;
    }
    if (!node_.is_leader()) {
        pending_commits_.clear();  // ya no lidera: esas propuestas no se le pueden atribuir.
        return;
    }
    const Index commit = node_.commit_index();
    while (!pending_commits_.empty() && pending_commits_.begin()->first <= commit) {
        const std::chrono::duration<double> elapsed = now - pending_commits_.begin()->second;
        metrics_.commit_latency->observe(elapsed.count());
        pending_commits_.erase(pending_commits_.begin());
    }
}

}  // namespace nexus
