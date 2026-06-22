/// @file   cluster/peer_directory.hpp
/// @brief  PeerDirectory: directorio inmutable NodeId -> dirección del plano inter-nodo (ADR-0025).
/// @ingroup cluster

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"

namespace nexus {

/// @brief Dirección de red de un nodo del clúster en el plano inter-nodo (Raft). Afinidad:
///   INMUTABLE.
/// @details El plano inter-nodo va por un puerto **separado** del plano de cliente
///   (ADR-0004/0013/0025); `port` es ese puerto, no el del plano de datos.
struct PeerAddress {
    std::string host;        ///< Host del peer (IPv4 punteada o nombre resoluble).
    std::uint16_t port = 0;  ///< Puerto del plano inter-nodo (Raft) del peer.

    bool operator==(const PeerAddress&) const = default;
};

/// @brief Directorio inmutable que resuelve `NodeId` -> `PeerAddress` del plano inter-nodo.
///   Afinidad: INMUTABLE / THREAD-SAFE (solo-lectura tras construirse).
/// @details Lo construye el *composition root* desde la configuración del clúster y se comparte
///   **por valor o por referencia const** entre los reactores (cada núcleo lo lee al (re)conectar
///   con un peer). Al ser inmutable tras la construcción, las lecturas concurrentes son seguras sin
///   sincronización. Suele **excluir** al propio nodo (un nodo no se conecta a sí mismo), pero el
///   directorio no lo impone: lo decide quien lo puebla.
/// @invariant Tras construirse, el conjunto de pares `(NodeId, PeerAddress)` no cambia.
class PeerDirectory {
public:
    PeerDirectory() = default;

    /// @brief Construye el directorio adoptando @p peers (uno por nodo conocido del clúster).
    explicit PeerDirectory(std::unordered_map<NodeId, PeerAddress> peers) noexcept;

    /// @brief Resuelve la dirección de @p node.
    /// @return Puntero **no propietario** a la dirección (válido mientras viva el directorio), o
    ///   `nullptr` si @p node no está registrado.
    [[nodiscard]] const PeerAddress* find(NodeId node) const noexcept;

    /// @brief ¿Está @p node registrado en el directorio?
    [[nodiscard]] bool contains(NodeId node) const noexcept;

    /// @brief Identidades de todos los nodos registrados, ordenadas ascendentemente (determinista).
    [[nodiscard]] std::vector<NodeId> node_ids() const;

    [[nodiscard]] std::size_t size() const noexcept { return peers_.size(); }
    [[nodiscard]] bool empty() const noexcept { return peers_.empty(); }

private:
    std::unordered_map<NodeId, PeerAddress> peers_;
};

}  // namespace nexus
