/// @file   broker/partition.hpp
/// @brief  Partition: une PartitionLog + idempotencia; unidad de serialización del broker.
/// @ingroup broker

#pragma once

#include <cstddef>
#include <unordered_map>

#include "broker/producer_session.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "storage/fetch_result.hpp"
#include "storage/partition_log.hpp"

namespace nexus {

/// @brief Une el `PartitionLog` con la idempotencia por productor: la unidad de serialización de
///   escrituras de una partición. Afinidad: REACTOR-LOCAL.
/// @details En **Fase 1b (mono-nodo)** las escrituras se confirman localmente (*ack* local): no hay
///   Raft, así que `produce`/`fetch` son **síncronas** (la E/S del log es bloqueante por ahora) y
///   el `high_watermark` es el `log_end_offset` (todo lo anexado está confirmado). En Fase 2 se
///   intercalará `RaftNode` y pasarán a `task<expected<...>>` (`co_await raft.propose`), y se
///   añadirá `PartitionState`/replicación. `produce` aplica la máquina de idempotencia (§5.9) por
///   `producer_id` antes de anexar.
/// @invariant `high_watermark()` == `log().log_end_offset()` (ack local, Fase 1b).
class Partition {
public:
    /// Adopta @p log (toma posesión) y fija la época de liderazgo (1b: nodo único, siempre líder).
    explicit Partition(PartitionLog log, Epoch leader_epoch = 0) noexcept;

    /// @brief Anexa @p batch tras validar idempotencia. HOT PATH (§7.11 #1).
    /// @details Para un productor idempotente (`producer_id >= 0`) clasifica la secuencia: un
    ///   duplicado se reconoce sin re-anexar; un hueco devuelve `OutOfRange`
    ///   (`OUT_OF_ORDER_SEQUENCE` en el wire).
    /// @return El último offset asignado al batch, o un error.
    [[nodiscard]] expected<Offset> produce(const RecordBatch& batch);

    /// @brief Lee batches desde @p offset hasta ~@p max_bytes (hasta el `high_watermark`).
    [[nodiscard]] expected<FetchResult> fetch(Offset offset, std::size_t max_bytes) const;

    /// Fuerza la durabilidad del log (`fsync`).
    [[nodiscard]] expected<void> sync();

    /// Frontera visible para los consumidores. 1b: `log_end_offset` (ack local).
    [[nodiscard]] Offset high_watermark() const noexcept { return log_.log_end_offset(); }
    /// 1b mono-nodo: la partición siempre es líder (sin replicación).
    [[nodiscard]] bool is_leader() const noexcept { return true; }
    [[nodiscard]] Epoch leader_epoch() const noexcept { return leader_epoch_; }
    [[nodiscard]] const PartitionLog& log() const noexcept { return log_; }

private:
    PartitionLog log_;
    /// Idempotencia por productor: `producer_id` → sesión (§5.9).
    std::unordered_map<ProducerId, ProducerSession> producers_;
    Epoch leader_epoch_;
};

}  // namespace nexus
