/// @file   consensus/raft_carrier.cpp
/// @brief  Implementación de RaftCarrier: enrutado de RPC y transporte de la FSM (ADR-0025).
/// @ingroup consensus

#include "consensus/raft_carrier.hpp"

#include <utility>
#include <variant>

#include "consensus/raft_node.hpp"
#include "consensus/raft_state.hpp"
#include "consensus/raft_state_store.hpp"

namespace nexus {

RaftCarrier::RaftCarrier(std::string topic, PartitionId partition, RaftNode& node,
                         RaftMessageSink& sink, RaftStateStore* store)
    : topic_(std::move(topic)), partition_(partition), node_(node), sink_(sink), store_(store) {}

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

void RaftCarrier::on_tick(MonoTime now) {
    node_.tick(now);
    flush_outbox();
}

void RaftCarrier::on_message(MonoTime now, const RaftMessage& message) {
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

void RaftCarrier::emit(const RaftMessage& message) {
    sink_.send(RaftEnvelope{.topic = topic_, .partition = partition_, .message = message});
}

}  // namespace nexus
