/// @file   broker/group_catalog.hpp
/// @brief  GroupCatalog: posee el coordinador de grupos y el almacén de offsets de cada núcleo
///         (sharding de grupos por hash, ADR-0026).
/// @ingroup broker

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "broker/group_coordinator.hpp"
#include "broker/offset_manager.hpp"

namespace nexus {

/// @brief Estado de coordinación confinado a **un** núcleo: la membresía de los grupos
///   (`GroupCoordinator`) y sus offsets confirmados (`OffsetManager`) cuyo núcleo coordinador es
///   este. Afinidad: REACTOR-LOCAL de su núcleo (lo toca solo ese reactor, vía `call_on`).
struct GroupShard {
    GroupCoordinator groups;  ///< Membresía/rebalanceo de los grupos de este núcleo.
    OffsetManager offsets;    ///< Offsets confirmados de los grupos de este núcleo.
};

/// @brief Posee un `GroupShard` por núcleo (sharding de grupos por `hash(group_id) % N`, ADR-0026).
///   Afinidad: se construye en el *composition root* (`Server`) antes de servir; cada `GroupShard`
///   es luego REACTOR-LOCAL de su núcleo.
/// @details Cada grupo de consumidores tiene un **único** núcleo coordinador (linealizable sin
///   locks): su membresía y sus offsets viven solo ahí, **no replicados** (a diferencia de los
///   metadatos de topics, ADR-0026). El catálogo materializa ese reparto creando N shards; el
///   `RequestRouter` enruta cada operación de grupo al shard del núcleo `hash(group_id) % N` por
///   paso de mensajes (`call_on`).
/// @invariant `core_count() >= 1`; los shards no cambian de identidad tras construir.
class GroupCatalog {
public:
    /// @param num_cores Número de núcleos del nodo (>= 1; valores < 1 se tratan como 1).
    explicit GroupCatalog(int num_cores = 1);
    GroupCatalog(const GroupCatalog&) = delete;
    GroupCatalog& operator=(const GroupCatalog&) = delete;
    GroupCatalog(GroupCatalog&&) = delete;
    GroupCatalog& operator=(GroupCatalog&&) = delete;
    ~GroupCatalog() = default;

    /// Número de núcleos (= número de shards).
    [[nodiscard]] int core_count() const noexcept { return static_cast<int>(shards_.size()); }

    /// @brief Coordinador de grupos del núcleo @p core. @pre `0 <= core < core_count()`.
    [[nodiscard]] GroupCoordinator& groups(int core) noexcept {
        return shards_[static_cast<std::size_t>(core)]->groups;
    }

    /// @brief Almacén de offsets del núcleo @p core. @pre `0 <= core < core_count()`.
    [[nodiscard]] OffsetManager& offsets(int core) noexcept {
        return shards_[static_cast<std::size_t>(core)]->offsets;
    }

    /// Punteros a los coordinadores de todos los núcleos (para `RequestRouter::bind_cluster`).
    [[nodiscard]] std::vector<GroupCoordinator*> all_groups() const;
    /// Punteros a los almacenes de offsets de todos los núcleos (para
    /// `RequestRouter::bind_cluster`).
    [[nodiscard]] std::vector<OffsetManager*> all_offsets() const;

private:
    std::vector<std::unique_ptr<GroupShard>> shards_;  ///< Uno por núcleo (indexado por core_id).
};

}  // namespace nexus
