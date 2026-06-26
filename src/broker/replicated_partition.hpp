/// @file   broker/replicated_partition.hpp
/// @brief  ReplicatedPartition: partición respaldada por Raft (produce→propose, hwm=commit_index).
/// @ingroup broker

#pragma once

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <vector>

#include "broker/partition_base.hpp"
#include "broker/producer_session.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "consensus/raft_log.hpp"
#include "consensus/raft_node.hpp"
#include "storage/fetch_result.hpp"
#include "storage/partition_log.hpp"

namespace nexus {

/// @brief Partición replicada con Raft: une `PartitionLog` + `RaftLog` + `RaftNode` con la
///   idempotencia por productor. Afinidad: REACTOR-LOCAL.
/// @details Es la evolución de `Partition` (1b, *ack* local) hacia el consenso (ADR-0003/0015): una
///   escritura se **propone** al `RaftNode` (solo el líder acepta) y se considera confirmada cuando
///   `commit_index` la alcanza (acks=quorum); el **high-watermark** visible para los consumidores
///   es el offset del `commit_index`. Posee la pila por `unique_ptr` para que las referencias
///   internas
///   (`RaftLog`→`PartitionLog`, `RaftNode`→`RaftLog`) sigan válidas aunque el objeto se mueva.
/// @note La FSM de Raft no hace E/S (ADR-0015): el portador (reactor/arnés) debe drenar `raft()`
///   (`tick`/`on_*`/`take_messages`) para avanzar el consenso. El cableado al servidor con
///   transporte real llega con el *routing* multi-reactor (C11). Aquí queda la **unidad de
///   partición** lista y probada vía enrutado directo/simulado.
/// @invariant `high_watermark()` ≤ `log().log_end_offset()` (lo confirmado nunca supera lo
/// escrito).
class ReplicatedPartition : public PartitionBase {
public:
    /// @brief Construye la partición replicada sobre @p log para el nodo @p self.
    /// @param peers Los demás miembros del grupo Raft (votantes + @p learners).
    /// @details Crea el `RaftLog` (sidecar `raft-meta` en el directorio del log) y el `RaftNode`.
    [[nodiscard]] static expected<ReplicatedPartition> create(NodeId self,
                                                              std::vector<NodeId> peers,
                                                              PartitionLog log, RaftConfig config,
                                                              std::vector<NodeId> learners = {});

    ReplicatedPartition(ReplicatedPartition&&) noexcept = default;
    ReplicatedPartition& operator=(ReplicatedPartition&&) noexcept = default;
    ReplicatedPartition(const ReplicatedPartition&) = delete;
    ReplicatedPartition& operator=(const ReplicatedPartition&) = delete;
    ~ReplicatedPartition() override = default;

    /// @brief (Solo líder) Propone @p batch tras validar idempotencia. HOT PATH (§7.11 #1).
    /// @details `Unsupported` si el nodo no es líder (`NOT_LEADER_FOR_PARTITION` en el wire).
    /// Aplica
    ///   la idempotencia por productor (§5.9) y propone la entrada; devuelve el **último offset**
    ///   asignado. La escritura es durable cuando `high_watermark()` lo supera (acks=quorum).
    [[nodiscard]] expected<Offset> produce(const RecordBatch& batch) override;

    /// @copydoc PartitionBase::fetch
    [[nodiscard]] expected<FetchResult> fetch(Offset offset, std::size_t max_bytes) const override;

    /// @brief Frontera visible para los consumidores: offset del `commit_index` (exclusivo).
    [[nodiscard]] Offset high_watermark() const override;

    [[nodiscard]] Index commit_index() const noexcept { return raft_->commit_index(); }
    [[nodiscard]] bool is_replicated() const noexcept override { return true; }
    [[nodiscard]] bool is_leader() const noexcept override { return raft_->is_leader(); }
    [[nodiscard]] Epoch leader_epoch() const noexcept override { return raft_->leader_epoch(); }

    /// Acceso a la FSM de Raft para que el portador la conduzca (`tick`/`on_*`/`take_messages`).
    [[nodiscard]] RaftNode& raft() noexcept { return *raft_; }
    /// @brief Acceso al `RaftLog` para que el portador dispare la compactación (`compact_to`,
    ///   ADR-0024). La E/S de compactación la hace el portador, no la FSM (ADR-0015).
    [[nodiscard]] RaftLog& raft_log() noexcept { return *rlog_; }
    [[nodiscard]] const PartitionLog& log() const noexcept override { return *log_; }

private:
    ReplicatedPartition(std::unique_ptr<PartitionLog> log, std::unique_ptr<RaftLog> rlog,
                        std::unique_ptr<RaftNode> raft) noexcept
        : log_(std::move(log)), rlog_(std::move(rlog)), raft_(std::move(raft)) {}

    std::unique_ptr<PartitionLog> log_;
    std::unique_ptr<RaftLog> rlog_;
    std::unique_ptr<RaftNode> raft_;
    /// Idempotencia por productor: `producer_id` → sesión (§5.9).
    std::unordered_map<ProducerId, ProducerSession> producers_;
};

}  // namespace nexus
