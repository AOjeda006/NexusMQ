/// @file   ingress/load_balancer.hpp
/// @brief  LoadBalancer: selección de nodo (round-robin / least-conn / consistent-hash) (ADR-0006).
/// @ingroup ingress

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"

namespace nexus {

/// @brief Estrategia de balanceo. Afinidad: INMUTABLE (enum).
enum class BalanceStrategy : std::uint8_t {
    RoundRobin,        ///< Reparte por turnos en orden estable.
    LeastConnections,  ///< Elige el nodo con menos conexiones activas (desempata por menor id).
    ConsistentHashing  ///< Mapea la *partition key* a un anillo de hash (mínima perturbación).
};

/// @brief Balanceador de carga del *ingress* en modo proxy (§6.4, ADR-0006). Afinidad:
///   REACTOR-LOCAL.
/// @details Selecciona el nodo destino con una de tres estrategias: **round-robin** (turnos sobre
/// el
///   conjunto ordenado), **least-connections** (el de menos conexiones activas, que el llamante
///   contabiliza con `on_acquire`/`on_release`) y **consistent-hashing** (un anillo con `vnodes`
///   nodos virtuales por nodo real, de modo que añadir/quitar un nodo reubica solo una fracción de
///   las claves). El hash es **FNV-1a de 64 bits**, determinista y estable entre plataformas (no
///   `std::hash`, que no lo es), apto para pruebas reproducibles.
/// @note **Ajuste del desglose:** `pick` es no-`const` (round-robin avanza un cursor interno). No
/// es
///   thread-safe: vive en su reactor.
/// @invariant `node_count()` = nodos distintos añadidos y no quitados; en consistent-hashing el
///   anillo tiene `node_count() * vnodes` puntos.
class LoadBalancer {
public:
    /// @param strategy Estrategia de selección.
    /// @param vnodes Nodos virtuales por nodo real en el anillo (solo consistent-hashing).
    explicit LoadBalancer(BalanceStrategy strategy, std::uint32_t vnodes = 128);

    /// @brief Añade @p node al conjunto (no-op si ya estaba).
    void add_node(NodeId node);
    /// @brief Quita @p node del conjunto (no-op si no estaba).
    void remove_node(NodeId node);

    /// @brief Selecciona un nodo. @p key solo se usa en consistent-hashing (se ignora en el resto).
    /// @return El nodo elegido, o `nullopt` si el conjunto está vacío.
    [[nodiscard]] std::optional<NodeId> pick(std::string_view key = {});

    /// @brief (LeastConnections) Contabiliza una conexión que se abre hacia @p node.
    void on_acquire(NodeId node);
    /// @brief (LeastConnections) Contabiliza una conexión que se cierra de @p node.
    void on_release(NodeId node);
    /// @brief Conexiones activas contabilizadas para @p node (0 si ninguna).
    [[nodiscard]] std::size_t active(NodeId node) const;

    [[nodiscard]] std::size_t node_count() const noexcept { return nodes_.size(); }
    [[nodiscard]] BalanceStrategy strategy() const noexcept { return strategy_; }

private:
    /// FNV-1a de 64 bits (determinista y estable entre plataformas).
    [[nodiscard]] static std::uint64_t hash64(std::string_view bytes) noexcept;
    /// Reconstruye el anillo de hash desde `nodes_` (solo consistent-hashing).
    void rebuild_ring();

    BalanceStrategy strategy_;
    std::uint32_t vnodes_;
    std::vector<NodeId> nodes_;  ///< ordenado ascendente (selección determinista).
    std::size_t cursor_ = 0;     ///< posición de round-robin.
    std::unordered_map<NodeId, std::size_t> active_;  ///< conexiones activas (least-connections).
    std::map<std::uint64_t, NodeId> ring_;            ///< anillo de hash (consistent-hashing).
};

}  // namespace nexus
