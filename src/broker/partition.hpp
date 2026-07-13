/// @file   broker/partition.hpp
/// @brief  Partition: une PartitionLog + idempotencia; unidad de serialización del broker.
/// @ingroup broker

#pragma once

#include <cstddef>
#include <filesystem>
#include <unordered_map>

#include "broker/partition_base.hpp"
#include "broker/producer_session.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "storage/fetch_result.hpp"
#include "storage/partition_log.hpp"
#include "storage/retention.hpp"

namespace nexus {

/// @brief Une el `PartitionLog` con la idempotencia por productor: la unidad de serialización de
///   escrituras de una partición no replicada. Afinidad: REACTOR-LOCAL.
/// @details Variante mono-nodo de `PartitionBase` (`replication_factor == 1`): las escrituras se
///   confirman localmente (*ack* local), sin Raft, así que `produce`/`fetch` son **síncronas** (la
///   E/S del log es bloqueante por ahora) y el `high_watermark` es el `log_end_offset` (todo lo
///   anexado está confirmado). `produce` aplica la máquina de idempotencia (§5.9) por `producer_id`
///   antes de anexar. La variante replicada es `ReplicatedPartition`.
/// @invariant `high_watermark()` == `log().log_end_offset()` (ack local, Fase 1b).
class Partition : public PartitionBase {
public:
    /// Adopta @p log (toma posesión) y fija la época de liderazgo (1b: nodo único, siempre líder).
    explicit Partition(PartitionLog log, Epoch leader_epoch = 0) noexcept;

    /// @copydoc PartitionBase::produce
    /// @details Para un productor idempotente (`producer_id >= 0`) clasifica la secuencia: un
    ///   duplicado se reconoce sin re-anexar; un hueco devuelve `OutOfRange`
    ///   (`OUT_OF_ORDER_SEQUENCE` en el wire).
    [[nodiscard]] expected<Offset> produce(const RecordBatch& batch) override;

    /// @copydoc PartitionBase::fetch
    [[nodiscard]] expected<FetchResult> fetch(Offset offset, std::size_t max_bytes) const override;

    /// Fuerza la durabilidad del log (`fsync`).
    [[nodiscard]] expected<void> sync();

    /// @copydoc PartitionBase::enforce_retention
    /// @details Mono-nodo: delega en `PartitionLog::enforce_retention` (reclama segmentos sellados).
    [[nodiscard]] expected<void> enforce_retention(
        const RetentionPolicy& policy, std::filesystem::file_time_type now) override {
        return log_.enforce_retention(policy, now);
    }

    /// Frontera visible para los consumidores. 1b: `log_end_offset` (ack local).
    [[nodiscard]] Offset high_watermark() const noexcept override { return log_.log_end_offset(); }
    /// 1b mono-nodo: la partición siempre es líder (sin replicación).
    [[nodiscard]] bool is_leader() const noexcept override { return true; }
    [[nodiscard]] Epoch leader_epoch() const noexcept override { return leader_epoch_; }
    [[nodiscard]] const PartitionLog& log() const noexcept override { return log_; }

private:
    PartitionLog log_;
    /// Idempotencia por productor: `producer_id` → sesión (§5.9).
    std::unordered_map<ProducerId, ProducerSession> producers_;
    Epoch leader_epoch_;
};

}  // namespace nexus
