/// @file   broker/transaction_coordinator.cpp
/// @brief  Implementación de la FSM del coordinador de transacciones (2PC recuperable).

#include "broker/transaction_coordinator.hpp"

#include <algorithm>
#include <utility>

namespace nexus {

namespace {

/// Estado `Prepare*` correspondiente a la decisión @p decision.
[[nodiscard]] TransactionState prepare_state_of(ControlRecordType decision) noexcept {
    return decision == ControlRecordType::Commit ? TransactionState::PrepareCommit
                                                 : TransactionState::PrepareAbort;
}

/// Estado `Complete*` correspondiente a la decisión @p decision.
[[nodiscard]] TransactionState complete_state_of(ControlRecordType decision) noexcept {
    return decision == ControlRecordType::Commit ? TransactionState::CompleteCommit
                                                 : TransactionState::CompleteAbort;
}

/// ¿La transacción está en un estado `Prepare*` (decisión tomada, marcadores en vuelo)?
[[nodiscard]] bool is_prepared(TransactionState state) noexcept {
    return state == TransactionState::PrepareCommit || state == TransactionState::PrepareAbort;
}

/// La decisión implícita en un estado `Prepare*`.
[[nodiscard]] ControlRecordType decision_of(TransactionState state) noexcept {
    return state == TransactionState::PrepareCommit ? ControlRecordType::Commit
                                                    : ControlRecordType::Abort;
}

}  // namespace

TransactionCoordinator::TransactionCoordinator(Epoch coordinator_epoch,
                                               std::chrono::milliseconds txn_timeout)
    : coordinator_epoch_(coordinator_epoch), txn_timeout_(txn_timeout) {}

expected<void> TransactionCoordinator::begin(MonoTime now, ProducerId producer_id,
                                             std::int16_t epoch) {
    if (const auto it = txns_.find(producer_id); it != txns_.end()) {
        const TransactionMetadata& cur = it->second;
        if (epoch < cur.producer_epoch) {
            return make_error(ErrorCode::Fenced, "begin: época obsoleta");
        }
        if (epoch == cur.producer_epoch && cur.state != TransactionState::CompleteCommit &&
            cur.state != TransactionState::CompleteAbort) {
            return make_error(ErrorCode::InvalidArgument, "begin: ya hay una transacción en curso");
        }
        // epoch > cur (nueva encarnación) o transacción anterior concluida: se reemplaza.
    }
    txns_[producer_id] = TransactionMetadata{.producer_id = producer_id,
                                             .producer_epoch = epoch,
                                             .state = TransactionState::Ongoing,
                                             .partitions = {},
                                             .unacked_partitions = {},
                                             .coordinator_epoch = coordinator_epoch_,
                                             .last_update = now};
    return {};
}

expected<TransactionMetadata*> TransactionCoordinator::require(ProducerId producer_id,
                                                               std::int16_t epoch) {
    const auto it = txns_.find(producer_id);
    if (it == txns_.end()) {
        return make_error(ErrorCode::NotFound, "transacción inexistente");
    }
    TransactionMetadata& txn = it->second;
    if (epoch < txn.producer_epoch) {
        return make_error(ErrorCode::Fenced, "época obsoleta");
    }
    if (epoch > txn.producer_epoch) {
        return make_error(ErrorCode::InvalidArgument, "época adelantada sin begin");
    }
    return &txn;
}

expected<void> TransactionCoordinator::add_partitions(
    MonoTime now, ProducerId producer_id, std::int16_t epoch,
    const std::vector<TopicPartition>& partitions) {
    const expected<TransactionMetadata*> txn = require(producer_id, epoch);
    if (!txn) {
        return std::unexpected(txn.error());
    }
    TransactionMetadata& meta = **txn;
    if (meta.state != TransactionState::Ongoing) {
        return make_error(ErrorCode::InvalidArgument,
                          "add_partitions: la transacción no está abierta");
    }
    for (const TopicPartition& p : partitions) {
        const auto pos = std::ranges::lower_bound(meta.partitions, p);
        if (pos == meta.partitions.end() || *pos != p) {
            meta.partitions.insert(pos, p);
        }
    }
    meta.last_update = now;
    return {};
}

expected<void> TransactionCoordinator::commit(MonoTime now, ProducerId producer_id,
                                              std::int16_t epoch) {
    const expected<TransactionMetadata*> txn = require(producer_id, epoch);
    if (!txn) {
        return std::unexpected(txn.error());
    }
    TransactionMetadata& meta = **txn;
    if (meta.state != TransactionState::Ongoing) {
        return make_error(ErrorCode::InvalidArgument, "commit: la transacción no está abierta");
    }
    enter_prepare(meta, ControlRecordType::Commit, now);
    return {};
}

expected<void> TransactionCoordinator::abort(MonoTime now, ProducerId producer_id,
                                             std::int16_t epoch) {
    const expected<TransactionMetadata*> txn = require(producer_id, epoch);
    if (!txn) {
        return std::unexpected(txn.error());
    }
    TransactionMetadata& meta = **txn;
    if (meta.state != TransactionState::Ongoing) {
        return make_error(ErrorCode::InvalidArgument, "abort: la transacción no está abierta");
    }
    enter_prepare(meta, ControlRecordType::Abort, now);
    return {};
}

void TransactionCoordinator::enter_prepare(TransactionMetadata& txn, ControlRecordType decision,
                                           MonoTime now) {
    txn.state = prepare_state_of(decision);
    txn.coordinator_epoch = coordinator_epoch_;
    txn.unacked_partitions = txn.partitions;
    txn.last_update = now;
    if (txn.unacked_partitions.empty()) {
        // Transacción vacía: la decisión queda registrada y concluye sin marcadores.
        txn.state = complete_state_of(decision);
        return;
    }
    enqueue_markers(txn, decision);
}

void TransactionCoordinator::enqueue_markers(const TransactionMetadata& txn,
                                             ControlRecordType decision) {
    for (const TopicPartition& p : txn.unacked_partitions) {
        pending_markers_.push_back(MarkerWrite{.producer_id = txn.producer_id,
                                               .producer_epoch = txn.producer_epoch,
                                               .coordinator_epoch = coordinator_epoch_,
                                               .decision = decision,
                                               .partition = p});
    }
}

void TransactionCoordinator::on_marker_written(ProducerId producer_id, std::int16_t epoch,
                                               const TopicPartition& partition) {
    const auto it = txns_.find(producer_id);
    if (it == txns_.end()) {
        return;
    }
    TransactionMetadata& txn = it->second;
    if (epoch != txn.producer_epoch || !is_prepared(txn.state)) {
        return;  // ack obsoleto (época distinta) o transacción ya concluida.
    }
    const auto pos = std::ranges::lower_bound(txn.unacked_partitions, partition);
    if (pos == txn.unacked_partitions.end() || *pos != partition) {
        return;  // partición ya acusada o no participante (idempotente).
    }
    txn.unacked_partitions.erase(pos);
    if (txn.unacked_partitions.empty()) {
        txn.state = complete_state_of(decision_of(txn.state));
    }
}

void TransactionCoordinator::tick(MonoTime now) {
    for (auto& [pid, txn] : txns_) {
        if (txn.state == TransactionState::Ongoing && now - txn.last_update > txn_timeout_) {
            enter_prepare(txn, ControlRecordType::Abort, now);
        }
    }
}

void TransactionCoordinator::resume_pending() {
    for (auto& [pid, txn] : txns_) {
        if (!is_prepared(txn.state)) {
            continue;
        }
        txn.coordinator_epoch = coordinator_epoch_;
        enqueue_markers(txn, decision_of(txn.state));
    }
}

std::vector<MarkerWrite> TransactionCoordinator::take_pending_markers() {
    return std::exchange(pending_markers_, {});
}

const TransactionMetadata* TransactionCoordinator::find(ProducerId producer_id) const {
    const auto it = txns_.find(producer_id);
    return it == txns_.end() ? nullptr : &it->second;
}

}  // namespace nexus
