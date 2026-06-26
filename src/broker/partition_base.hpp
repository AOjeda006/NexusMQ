/// @file   broker/partition_base.hpp
/// @brief  PartitionBase: interfaz común de las particiones servibles del broker.
/// @ingroup broker

#pragma once

#include <cstddef>

#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "storage/fetch_result.hpp"
#include "storage/partition_log.hpp"

namespace nexus {

/// @brief Superficie común de una partición servible por el broker. Afinidad: REACTOR-LOCAL.
/// @details Unifica la `Partition` (mono-nodo, *ack* local) y la `ReplicatedPartition` (respaldada
///   por Raft, ADR-0016) tras una única interfaz de hot-path, de modo que el `Topic` y el
///   `RequestRouter` sirven una partición **sin conocer** su clase concreta: el tipo se elige al
///   crearla según `replication_factor` (ADR-0026). Es polimorfismo en el **borde** del hot-path
///   —una indirección virtual por `produce`/`fetch`, despreciable frente a la E/S del log— que
///   mantiene a los llamantes cerrados a modificación al añadir tipos de partición (OCP).
/// @invariant `high_watermark()` ≤ `log().log_end_offset()` (lo visible nunca supera lo escrito).
/// @note Base polimórfica: copia/movimiento **protegidos** (sin *slicing* desde el llamante; las
///   derivadas siguen siendo movibles) y destructor virtual público.
class PartitionBase {
public:
    PartitionBase() = default;
    virtual ~PartitionBase() = default;

    /// @brief Anexa (o propone, en réplica) @p batch tras validar idempotencia. HOT PATH (§7.11
    /// #1).
    /// @return El último offset asignado al batch, o un error.
    [[nodiscard]] virtual expected<Offset> produce(const RecordBatch& batch) = 0;

    /// @brief Lee batches desde @p offset hasta ~@p max_bytes; el llamante respeta el
    /// `high_watermark`.
    [[nodiscard]] virtual expected<FetchResult> fetch(Offset offset,
                                                      std::size_t max_bytes) const = 0;

    /// @brief Frontera visible para los consumidores (ack local en `Partition`; offset del
    ///   `commit_index` en `ReplicatedPartition`).
    [[nodiscard]] virtual Offset high_watermark() const = 0;

    /// @brief ¿Es esta réplica el líder que acepta escrituras?
    [[nodiscard]] virtual bool is_leader() const noexcept = 0;

    /// @brief Época de liderazgo vigente (metadata / cierre de productores).
    [[nodiscard]] virtual Epoch leader_epoch() const noexcept = 0;

    /// @brief ¿Está respaldada por Raft (`ReplicatedPartition`)? Por defecto no (`Partition`).
    /// @details Lo usa el camino de produce para decidir si registrar la latencia de confirmación a
    ///   quórum (solo aplica a réplicas); evita el coste de ese registro en particiones mono-nodo.
    [[nodiscard]] virtual bool is_replicated() const noexcept { return false; }

    /// @brief Acceso de solo lectura al log subyacente (offsets de inicio/fin, lecturas).
    [[nodiscard]] virtual const PartitionLog& log() const noexcept = 0;

protected:
    PartitionBase(const PartitionBase&) = default;
    PartitionBase& operator=(const PartitionBase&) = default;
    PartitionBase(PartitionBase&&) = default;
    PartitionBase& operator=(PartitionBase&&) = default;
};

}  // namespace nexus
