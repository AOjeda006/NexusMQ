/// @file   broker/topic.hpp
/// @brief  Topic + TopicMetadata: agrupa las particiones de un topic y su configuración.
/// @ingroup broker

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "broker/partition.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Configuración (inmutable) de un topic: gobierna los logs de sus particiones. INMUTABLE.
/// @details `segment_bytes`/`retention_*` se trasladan al `LogConfig` de cada `PartitionLog`.
///   `compaction` y `compression` quedan reservados (compactación y LZ4/Zstd llegan en Fase 4).
struct TopicConfig {
    std::size_t segment_bytes = 64UL * 1024 * 1024;  ///< Tamaño del segmento activo.
    std::int64_t retention_ms = -1;                  ///< Retención por tiempo (`-1` = sin límite).
    std::int64_t retention_bytes = -1;               ///< Retención por tamaño (`-1` = sin límite).
    bool compaction = false;                         ///< Compactación por clave (Fase 4).
    Codec compression = Codec::None;                 ///< Compresión por batch (Fase 4).
};

/// @brief Metadatos (inmutables) de un topic. Afinidad: INMUTABLE.
struct TopicMetadata {
    std::string name;
    std::int32_t partition_count = 0;
    std::int16_t replication_factor = 1;  ///< 1 en Fase 1b (mono-nodo); >1 con Raft (Fase 2).
    TopicConfig config;
    std::int64_t created_at_ms = 0;  ///< Marca de creación (epoch ms; la fija el TopicManager).
};

/// @brief Agrupa las particiones de un topic en un reactor. Afinidad: REACTOR-LOCAL por partición.
/// @details Posee sus `Partition` (cada una con su `PartitionLog`). El `TopicManager` (plano de
///   control) crea el topic y le añade las particiones; el hot-path solo consulta `partition()`.
/// @invariant Las claves de `partitions_` están en `[0, meta_.partition_count)`.
class Topic {
public:
    explicit Topic(TopicMetadata meta) : meta_(std::move(meta)) {}
    Topic(Topic&&) noexcept = default;
    Topic& operator=(Topic&&) noexcept = default;
    Topic(const Topic&) = delete;
    Topic& operator=(const Topic&) = delete;
    ~Topic() = default;

    /// Inserta la @p partition bajo @p id (la toma en posesión). Sobrescribe si @p id ya existía.
    void add_partition(PartitionId id, std::unique_ptr<Partition> partition) {
        partitions_.insert_or_assign(id, std::move(partition));
    }

    /// @return La partición @p id, o `nullptr` si no existe en este topic.
    [[nodiscard]] Partition* partition(PartitionId id) noexcept {
        const auto it = partitions_.find(id);
        return it == partitions_.end() ? nullptr : it->second.get();
    }

    [[nodiscard]] const TopicMetadata& meta() const noexcept { return meta_; }
    /// Número de particiones efectivamente instaladas (observabilidad / pruebas).
    [[nodiscard]] std::size_t partition_count() const noexcept { return partitions_.size(); }

private:
    TopicMetadata meta_;
    std::unordered_map<PartitionId, std::unique_ptr<Partition>> partitions_;
};

}  // namespace nexus
