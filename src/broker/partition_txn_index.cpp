/// @file   broker/partition_txn_index.cpp
/// @brief  Implementación del índice transaccional de partición (LSO + filtrado read_committed).

#include "broker/partition_txn_index.hpp"

#include <algorithm>
#include <unordered_set>

namespace nexus {

namespace {

/// @brief Rastrea, al recorrer los batches en orden de offset, qué productores están **dentro** de
///   una transacción abortada (entre su `first_offset` y su marcador). Afinidad: transitoria.
class AbortedTracker {
public:
    explicit AbortedTracker(const std::vector<AbortedTxn>& aborted) {
        for (const AbortedTxn& a : aborted) {
            firsts_[a.producer_id].push_back(a.first_offset);
        }
        for (auto& [producer_id, offsets] : firsts_) {
            std::ranges::sort(offsets);
        }
    }

    /// El marcador de @p producer_id cierra su región abortada activa (avanza a la siguiente).
    void on_marker(ProducerId producer_id) {
        if (active_.erase(producer_id) > 0) {
            ++next_[producer_id];
        }
    }

    /// ¿La data transaccional de @p producer_id en @p base_offset pertenece a una txn abortada?
    [[nodiscard]] bool is_aborted(ProducerId producer_id, Offset base_offset) {
        if (!active_.contains(producer_id)) {
            const auto it = firsts_.find(producer_id);
            if (it != firsts_.end()) {
                const std::size_t i = next_[producer_id];
                if (i < it->second.size() && it->second[i] <= base_offset) {
                    active_.insert(producer_id);
                }
            }
        }
        return active_.contains(producer_id);
    }

private:
    std::unordered_map<ProducerId, std::vector<Offset>>
        firsts_;                                        ///< pid → inicios abortados (asc).
    std::unordered_map<ProducerId, std::size_t> next_;  ///< pid → próxima txn abortada.
    std::unordered_set<ProducerId> active_;             ///< pid dentro de una txn abortada.
};

}  // namespace

void PartitionTxnIndex::on_data(ProducerId producer_id, Offset base_offset) {
    // Solo el primer batch de la transacción fija su inicio (idempotente).
    open_.try_emplace(producer_id, base_offset);
}

void PartitionTxnIndex::on_marker(ProducerId producer_id, ControlRecordType decision,
                                  Offset marker_offset) {
    const auto it = open_.find(producer_id);
    if (it == open_.end()) {
        return;  // sin transacción abierta: nada que cerrar (idempotente ante reprocesado).
    }
    if (decision == ControlRecordType::Abort) {
        aborted_.push_back(AbortedRange{.producer_id = producer_id,
                                        .first_offset = it->second,
                                        .marker_offset = marker_offset});
    }
    open_.erase(it);
}

Offset PartitionTxnIndex::last_stable_offset(Offset high_watermark) const {
    Offset lso = high_watermark;
    for (const auto& [producer_id, first_offset] : open_) {
        lso = std::min(lso, first_offset);
    }
    return lso;
}

std::vector<AbortedTxn> PartitionTxnIndex::aborted_transactions(Offset fetch_offset) const {
    std::vector<AbortedTxn> result;
    for (const AbortedRange& range : aborted_) {
        // La data abortada ocupa [first_offset, marker_offset); solapa un fetch desde fetch_offset
        // solo si su marcador queda estrictamente por encima del inicio del fetch.
        if (range.marker_offset > fetch_offset) {
            result.push_back(
                AbortedTxn{.producer_id = range.producer_id, .first_offset = range.first_offset});
        }
    }
    std::ranges::sort(result, [](const AbortedTxn& a, const AbortedTxn& b) {
        return a.first_offset < b.first_offset;
    });
    return result;
}

void PartitionTxnIndex::evict_below(Offset log_start) {
    std::erase_if(aborted_, [log_start](const AbortedRange& range) {
        return range.marker_offset < log_start;
    });
}

std::vector<RecordBatch> filter_committed(const std::vector<RecordBatch>& batches,
                                          const std::vector<AbortedTxn>& aborted) {
    AbortedTracker tracker{aborted};
    std::vector<RecordBatch> out;
    out.reserve(batches.size());

    for (const RecordBatch& batch : batches) {
        const std::uint16_t attrs = batch.header().attrs;
        const ProducerId producer_id = batch.header().producer_id;

        if (is_control(attrs)) {
            tracker.on_marker(producer_id);  // el marcador nunca se entrega al consumidor.
            continue;
        }
        if (is_transactional(attrs) &&
            tracker.is_aborted(producer_id, batch.header().base_offset)) {
            continue;  // batch de una transacción abortada: filtrado.
        }
        out.push_back(batch);
    }
    return out;
}

}  // namespace nexus
