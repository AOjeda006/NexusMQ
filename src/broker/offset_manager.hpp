/// @file   broker/offset_manager.hpp
/// @brief  OffsetManager: almacén de offsets confirmados por grupo de consumidores.
/// @ingroup broker

#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/error.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Offset confirmado de un grupo en una partición (para el `describe` de grupo). INMUTABLE.
struct GroupOffsetEntry {
    std::string topic;
    PartitionId partition = 0;
    Offset offset = -1;
};

/// @brief Clave de un commit de offset: (grupo, topic, partición). Afinidad: INMUTABLE.
struct OffsetKey {
    std::string group;
    std::string topic;
    PartitionId partition = 0;

    bool operator==(const OffsetKey&) const = default;
};

/// @brief Hash de `OffsetKey` (combina los hashes de sus campos, estilo `hash_combine`).
struct OffsetKeyHash {
    [[nodiscard]] std::size_t operator()(const OffsetKey& key) const noexcept {
        std::size_t seed = std::hash<std::string>{}(key.group);
        const auto mix = [&seed](std::size_t value) noexcept {
            // Constante de difusión de boost::hash_combine (no es criptográfica).
            seed ^= value + 0x9e3779b9UL + (seed << 6U) + (seed >> 2U);
        };
        mix(std::hash<std::string>{}(key.topic));
        mix(std::hash<PartitionId>{}(key.partition));
        return seed;
    }
};

/// @brief Almacena el último offset confirmado por (grupo, topic, partición). Afinidad:
///   REACTOR-LOCAL (vive en el `RequestRouter` de su reactor; sin sincronización).
/// @details Fase 1b: almacén **en memoria** (no durable; la persistencia del topic interno de
///   offsets llega con la distribución de Fase 2). Solo guarda el offset confirmado; la membresía
///   de grupo (Join/Sync/Heartbeat, rebalanceo) es independiente y también es de Fase 2. El último
///   commit gana (sin exigir monotonía: el cliente decide su semántica de reproceso).
class OffsetManager {
public:
    /// Guarda (o sobrescribe) el offset confirmado de @p group en @p topic/@p partition.
    void commit(std::string group, std::string topic, PartitionId partition, Offset offset,
                std::string metadata = {});

    /// @brief Offset confirmado de @p group en @p topic/@p partition.
    /// @return el offset, o `NotFound` si ese grupo no ha confirmado nada en esa partición.
    [[nodiscard]] expected<Offset> fetch(std::string_view group, std::string_view topic,
                                         PartitionId partition) const;

    /// @brief Todos los offsets confirmados de @p group, **ordenados** por (topic, partición).
    /// @details Para el `describe` de grupo (plano de control/observabilidad): el orden es
    ///   determinista. Vacío si el grupo no ha confirmado nada.
    [[nodiscard]] std::vector<GroupOffsetEntry> list_for_group(std::string_view group) const;

    /// Número de commits almacenados (para pruebas/diagnóstico).
    [[nodiscard]] std::size_t size() const noexcept { return commits_.size(); }

private:
    /// Valor almacenado: offset confirmado + metadatos opacos del cliente.
    struct Entry {
        Offset offset = -1;
        std::string metadata;
    };

    std::unordered_map<OffsetKey, Entry, OffsetKeyHash> commits_;
};

}  // namespace nexus
