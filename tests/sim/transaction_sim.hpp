// Arnés de simulación determinista de transacciones multi-partición (ADR-0033). Un
// TransactionCoordinator (FSM sin E/S) más un conjunto de SimPartition (log en memoria + índice
// transaccional), sobre un reloj virtual y un RNG sembrado. Modela el 2PC de punta a punta:
// init/begin/produce/commit/abort → escritura de marcadores → visibilidad read_committed, con
// entrega caótica de marcadores y failover del coordinador. Sin hilos, sin sockets, reproducible.
#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "broker/partition_txn_index.hpp"
#include "broker/transaction_coordinator.hpp"
#include "common/control_record.hpp"
#include "common/record.hpp"
#include "common/record_codec.hpp"
#include "common/types.hpp"

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

/// @brief Log de una partición en memoria + su índice transaccional (afinidad: test).
/// @details Cada batch ocupa un offset (un record por batch), de modo que `base_offset` == índice
/// en
///   el vector. Aplica los marcadores de forma idempotente y con fencing del coordinador obsoleto
///   (descarta un marcador cuya época de coordinador sea inferior a la última vista), y sirve la
///   lectura `read_committed` (hasta el LSO, filtrando abortados y marcadores).
class SimPartition {
public:
    /// Anexa un record de datos con id de payload único; devuelve su offset. Si es transaccional,
    /// abre/continúa la transacción del productor en esta partición.
    Offset append_data(ProducerId pid, std::int16_t epoch, bool transactional, int payload_id) {
        const Offset base = next_offset();
        RecordBatchHeader header;
        header.base_offset = base;
        header.producer_id = pid;
        header.producer_epoch = epoch;
        header.record_count = 1;
        header.attrs = transactional ? attrs_with_transactional(0, true) : 0;
        Record rec;
        rec.value = std::vector<std::byte>{std::byte{0}};
        RecordBatchBuilder builder;
        builder.add(std::move(rec));
        log_.push_back(builder.build(header));
        payload_at_.emplace(base, payload_id);
        if (transactional) {
            index_.on_data(pid, base);
            open_pids_.insert(pid);
        }
        return base;
    }

    /// Aplica el marcador de fin de transacción de @p pid. Idempotente (ignora si la transacción ya
    /// está cerrada) y con **fencing del coordinador**: descarta un marcador de época inferior a la
    /// última vista para ese productor.
    void apply_marker(ProducerId pid, std::int16_t epoch, ControlRecordType decision,
                      Epoch coordinator_epoch) {
        const auto seen = last_coordinator_epoch_.find(pid);
        if (seen != last_coordinator_epoch_.end() && coordinator_epoch < seen->second) {
            return;  // marcador de un coordinador obsoleto: fenced.
        }
        last_coordinator_epoch_[pid] = coordinator_epoch;
        if (!open_pids_.contains(pid)) {
            return;  // transacción ya cerrada: marcador idempotente.
        }
        const Offset base = next_offset();
        log_.push_back(build_control_batch(
            {.type = decision, .coordinator_epoch = coordinator_epoch, .version = 0}, pid, epoch));
        index_.on_marker(pid, decision, base);
        open_pids_.erase(pid);
    }

    [[nodiscard]] Offset high_watermark() const noexcept { return next_offset(); }
    [[nodiscard]] Offset lso() const { return index_.last_stable_offset(high_watermark()); }

    /// Ids de payload visibles bajo `read_committed`: hasta el LSO, sin marcadores ni abortados.
    [[nodiscard]] std::vector<int> read_committed() const {
        const auto stable = static_cast<std::size_t>(lso());
        const std::vector<RecordBatch> window(log_.begin(),
                                              log_.begin() + static_cast<std::ptrdiff_t>(stable));
        const std::vector<RecordBatch> visible =
            filter_committed(window, index_.aborted_transactions(0));
        std::vector<int> out;
        out.reserve(visible.size());
        for (const RecordBatch& batch : visible) {
            out.push_back(payload_at_.at(batch.header().base_offset));
        }
        std::ranges::sort(out);
        return out;
    }

private:
    [[nodiscard]] Offset next_offset() const noexcept { return static_cast<Offset>(log_.size()); }

    std::vector<RecordBatch> log_;
    std::unordered_map<Offset, int> payload_at_;
    PartitionTxnIndex index_;
    std::unordered_set<ProducerId> open_pids_;
    std::unordered_map<ProducerId, Epoch> last_coordinator_epoch_;
};

/// @brief Cluster transaccional determinista: coordinador + particiones + reloj virtual.
/// @details Expone la API nativa (init/begin/add_partitions/produce/commit/abort) y el transporte
/// de
///   marcadores (`flush_markers`, con posibilidad de entrega parcial para modelar caos). El
///   failover se simula con `failover(new_epoch)` (sube la época del coordinador y reanuda el 2PC
///   pendiente).
class TxnSim {
public:
    explicit TxnSim(Epoch coordinator_epoch = 1) : coord_(coordinator_epoch) {}

    [[nodiscard]] TransactionCoordinator& coordinator() noexcept { return coord_; }
    [[nodiscard]] SimPartition& partition(const std::string& name) { return partitions_[name]; }
    [[nodiscard]] MonoTime now() const noexcept { return clock_.now(); }
    void advance(Millis delta) noexcept { clock_.advance(delta); }

    /// Asigna un id de payload único y creciente (para verificar visibilidad sin ambigüedad).
    [[nodiscard]] int next_payload() noexcept { return next_payload_++; }

    /// @brief Drena los marcadores pendientes del coordinador y los aplica a sus particiones.
    /// @param deliver_fraction Fracción [0,1] de marcadores a entregar (el resto queda sin
    /// entregar,
    ///   modelando pérdidas que un failover/reanudación posterior re-emitirá). Los entregados
    ///   acusan al coordinador.
    void flush_markers(double deliver_fraction = 1.0) {
        std::vector<MarkerWrite> pending = coord_.take_pending_markers();
        const auto deliver =
            static_cast<std::size_t>(static_cast<double>(pending.size()) * deliver_fraction + 0.5);
        for (std::size_t i = 0; i < pending.size(); ++i) {
            const MarkerWrite& marker = pending[i];
            if (i >= deliver) {
                break;  // marcador no entregado: se re-emitirá tras el failover/reanudación.
            }
            partitions_[marker.partition.topic].apply_marker(marker.producer_id,
                                                             marker.producer_epoch, marker.decision,
                                                             marker.coordinator_epoch);
            coord_.on_marker_written(marker.producer_id, marker.producer_epoch, marker.partition);
        }
    }

    /// Simula un failover del coordinador: adopta @p new_epoch y reanuda el 2PC de las
    /// transacciones que quedaron a medias (re-emite sus marcadores con la época nueva).
    void failover(Epoch new_epoch) {
        coord_.set_coordinator_epoch(new_epoch);
        coord_.resume_pending();
    }

private:
    VirtualClock clock_;
    TransactionCoordinator coord_;
    std::map<std::string, SimPartition> partitions_;
    int next_payload_ = 1;
};

}  // namespace nexus::sim
