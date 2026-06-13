/// @file   broker/partition.cpp
/// @brief  Implementación de Partition (produce idempotente + fetch sobre el PartitionLog).
/// @ingroup broker

#include "broker/partition.hpp"

#include <utility>

namespace nexus {

Partition::Partition(PartitionLog log, Epoch leader_epoch) noexcept
    : log_(std::move(log)), leader_epoch_(leader_epoch) {}

expected<Offset> Partition::produce(const RecordBatch& batch) {
    const RecordBatchHeader& header = batch.header();

    // Productor no idempotente (producer_id < 0): se anexa directamente.
    if (header.producer_id < 0) {
        return log_.append(batch);
    }

    // Idempotencia (§5.9): clasifica la secuencia del batch frente a la sesión del productor.
    ProducerSession& session =
        producers_.try_emplace(header.producer_id, header.producer_id, header.producer_epoch)
            .first->second;
    switch (session.check(header.base_sequence, header.record_count)) {
        case ProducerSession::SeqCheck::Duplicate:
            // Reintento ya aplicado: se reconoce sin re-anexar. 1b: devolvemos el último offset del
            // log (no se rastrea el offset original por secuencia; pendiente para una capa
            // superior).
            return log_.log_end_offset() - 1;
        case ProducerSession::SeqCheck::Gap:
            return make_error(ErrorCode::OutOfRange,
                              "secuencia idempotente fuera de orden (hueco)");
        case ProducerSession::SeqCheck::Accept:
            break;
    }

    const expected<Offset> last = log_.append(batch);
    if (!last) {
        return last;
    }
    session.advance(header.base_sequence + header.record_count - 1);
    return last;
}

expected<FetchResult> Partition::fetch(Offset offset, std::size_t max_bytes) const {
    return log_.read(offset, max_bytes);
}

expected<void> Partition::sync() {
    return log_.sync();
}

}  // namespace nexus
