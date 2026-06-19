/// @file   broker/replicated_partition.cpp
/// @brief  Implementación de ReplicatedPartition (produce→propose; high-watermark=commit_index).
/// @ingroup broker

#include "broker/replicated_partition.hpp"

#include <string>
#include <utility>

namespace nexus {

expected<ReplicatedPartition> ReplicatedPartition::create(NodeId self, std::vector<NodeId> peers,
                                                          PartitionLog log, RaftConfig config,
                                                          std::vector<NodeId> learners) {
    auto plog = std::make_unique<PartitionLog>(std::move(log));
    const std::string meta_path = (plog->dir() / "raft-meta").string();
    auto rlog = RaftLog::open(*plog, meta_path);
    if (!rlog) {
        return std::unexpected(rlog.error());
    }
    auto rlog_ptr = std::make_unique<RaftLog>(std::move(*rlog));
    auto raft =
        std::make_unique<RaftNode>(self, std::move(peers), *rlog_ptr, config, std::move(learners));
    return ReplicatedPartition{std::move(plog), std::move(rlog_ptr), std::move(raft)};
}

expected<Offset> ReplicatedPartition::produce(const RecordBatch& batch) {
    if (!raft_->is_leader()) {
        return make_error(ErrorCode::Unsupported, "produce: la partición no es líder");
    }
    const RecordBatchHeader& header = batch.header();

    // Propone la entrada y traduce el índice de Raft asignado a su último offset de partición.
    const auto propose_offset = [this](const RecordBatch& b) -> expected<Offset> {
        const auto index = raft_->propose(b);
        if (!index) {
            return std::unexpected(index.error());
        }
        const auto offsets = rlog_->offsets_at(*index);
        if (!offsets) {
            return std::unexpected(offsets.error());
        }
        return offsets->second;  // último offset del batch.
    };

    // Productor no idempotente (producer_id < 0): se propone directamente.
    if (header.producer_id < 0) {
        return propose_offset(batch);
    }

    // Idempotencia *effectively-once* (§5.9): clasifica (época, secuencia) frente a la sesión.
    ProducerSession& session =
        producers_.try_emplace(header.producer_id, header.producer_id, header.producer_epoch)
            .first->second;
    switch (session.check(header.producer_epoch, header.base_sequence, header.record_count)) {
        case ProducerSession::SeqCheck::Fenced:
            return make_error(ErrorCode::Fenced, "época de productor obsoleta (fenced)");
        case ProducerSession::SeqCheck::Duplicate: {
            // Reintento ya aplicado: se reconoce sin re-proponer y se devuelve el offset original.
            const Offset dup_base = session.duplicate_base_offset(header.base_sequence);
            return dup_base >= 0 ? dup_base + header.record_count - 1 : log_->log_end_offset() - 1;
        }
        case ProducerSession::SeqCheck::Gap:
            return make_error(ErrorCode::OutOfRange,
                              "secuencia idempotente fuera de orden (hueco)");
        case ProducerSession::SeqCheck::Accept:
            break;
    }

    const expected<Offset> last = propose_offset(batch);
    if (!last) {
        return last;
    }
    session.accept(header.producer_epoch, header.base_sequence, header.record_count,
                   *last - header.record_count + 1);
    return last;
}

expected<FetchResult> ReplicatedPartition::fetch(Offset offset, std::size_t max_bytes) const {
    return log_->read(offset, max_bytes);
}

Offset ReplicatedPartition::high_watermark() const {
    const Index commit = raft_->commit_index();
    if (commit == 0) {
        return log_->log_start_offset();  // nada confirmado todavía.
    }
    const auto offsets = rlog_->offsets_at(commit);
    if (!offsets) {
        return log_->log_start_offset();
    }
    return offsets->second + 1;  // frontera exclusiva: uno más que el último offset confirmado.
}

}  // namespace nexus
