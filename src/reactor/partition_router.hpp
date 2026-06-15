/// @file   reactor/partition_router.hpp
/// @brief  PartitionRouter: mapea (partición → núcleo dueño) y enruta la operación a su reactor.
/// @ingroup reactor

#pragma once

#include <cstddef>
#include <utility>
#include <vector>

#include "common/types.hpp"
#include "reactor/cross_core_call.hpp"
#include "reactor/reactor.hpp"

namespace nexus {

/// @brief Enruta una operación de partición al **reactor dueño** de esa partición. Afinidad:
///   THREAD-SAFE de solo lectura tras construir (referencias no propietarias a los reactores).
/// @details Materializa el reparto *shared-nothing* (ADR-0005): un único núcleo sirve cada
///   partición (`owner_core(p) = p % core_count`, la misma regla que `ReactorPool::reactor_for`),
///   de modo que su estado vive sin compartir en ese reactor. `route` ejecuta @p fn en el dueño
///   vía `call_on`
///   (petición/respuesta cross-core) y reanuda al llamante con el resultado: si el dueño es el
///   propio reactor de la conexión, el viaje es local; si no, cruza núcleos por el buzón SPSC.
/// @invariant Los reactores (indexados por `core_id`) deben vivir más que el router; `core_count`
///   es fijo tras construir.
class PartitionRouter {
public:
    /// @param reactors Reactores del nodo, **indexados por `core_id`** (no propietario).
    explicit PartitionRouter(std::vector<Reactor*> reactors) : reactors_(std::move(reactors)) {}

    [[nodiscard]] int core_count() const noexcept { return static_cast<int>(reactors_.size()); }

    /// Núcleo dueño de @p partition (misma regla que `ReactorPool::reactor_for`).
    [[nodiscard]] int owner_core(PartitionId partition) const noexcept {
        return static_cast<int>(static_cast<std::size_t>(partition) % reactors_.size());
    }

    /// Reactor dueño de @p partition.
    [[nodiscard]] Reactor& owner(PartitionId partition) const {
        return *reactors_[static_cast<std::size_t>(owner_core(partition))];
    }

    /// @brief Ejecuta @p fn en el reactor dueño de @p partition y reanuda al llamante (en @p self)
    ///   con el resultado. @p fn corre en el hilo del dueño: debe tocar solo su estado.
    template <class Fn>
    [[nodiscard]] auto route(Reactor& self, PartitionId partition, Fn&& fn) {
        return call_on(self, owner(partition), std::forward<Fn>(fn));
    }

private:
    std::vector<Reactor*> reactors_;  // no propietario; indexado por core_id.
};

}  // namespace nexus
