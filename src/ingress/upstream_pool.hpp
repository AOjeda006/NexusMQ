/// @file   ingress/upstream_pool.hpp
/// @brief  UpstreamPool: pool por reactor de conexiones al plano de datos de nodos aguas arriba.
/// @ingroup ingress

#pragma once

#include <cstddef>
#include <unordered_map>
#include <vector>

#include "cluster/peer_directory.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "common/types.hpp"
#include "io/socket.hpp"

namespace nexus {

class Proactor;

/// @brief Pool de conexiones al **plano de datos** de nodos aguas arriba para el modo proxy
///   (ADR-0006/0027). Afinidad: REACTOR-LOCAL.
/// @details El modo proxy releva las tramas de un cliente "tonto" a un nodo del clúster
///   (`Proxy::forward`). Para no pagar un *handshake* TCP por conexión de cliente (normativa de
///   redes: *connection pooling*), cada reactor mantiene su propio pool —**sin locks**, coherente
///   con shared-nothing (ADR-0005)— de conexiones reutilizables por `NodeId`:
///   - `acquire` entrega una conexión **en préstamo exclusivo**: reúsa una ociosa de la *free-list*
///     del nodo o **diala una nueva** de forma asíncrona (`Socket::async_connect`) si no hay. El
///     préstamo es exclusivo mientras dura el relevo (petición/respuesta a nivel de trama, I18):
///     así nunca se intercalan tramas de dos clientes en la misma conexión aguas arriba.
///   - `release` devuelve la conexión a la *free-list* para reúso; si está llena
///     (`max_idle_per_node`), la cierra (acotación: ni fuga de descriptores ni crecimiento sin
///     límite, normativa de concurrencia/datos-distribuidos).
///   La dirección del plano de datos de cada nodo se resuelve con un `PeerDirectory` poblado con
///   direcciones de **cliente** (instancia separada de la del plano Raft de ADR-0025; mismo tipo,
///   distinta instancia).
/// @note Una conexión ociosa reusada puede haber sido cerrada por el par mientras esperaba: el
///   primer uso fallará y el llamante la descarta (sin devolverla). El *health-check*/reintento de
///   un salto queda como mejora futura (ADR-0027).
/// @invariant Ninguna *free-list* supera `max_idle_per_node`; el directorio vive más que el pool.
class UpstreamPool {
public:
    /// Tope por defecto de conexiones ociosas guardadas por nodo (acota la *free-list*).
    static constexpr std::size_t kDefaultMaxIdlePerNode = 8;

    /// @param peers Directorio `NodeId → host:puerto` del **plano de datos** (no propietario; debe
    ///   vivir más que el pool).
    /// @param max_idle_per_node Tope de conexiones ociosas guardadas por nodo.
    explicit UpstreamPool(const PeerDirectory& peers,
                          std::size_t max_idle_per_node = kDefaultMaxIdlePerNode) noexcept
        : peers_(peers), max_idle_per_node_(max_idle_per_node) {}

    /// @brief Obtiene una conexión **en préstamo exclusivo** al plano de datos de @p node: reúsa
    /// una
    ///   ociosa de la *free-list* o diala una nueva (`Socket::async_connect`) si no hay.
    /// @param[in,out] proactor Puerto de E/S del reactor sobre el que se diala.
    /// @return El socket conectado, o `NotFound` (nodo ausente del directorio) / `IoError`
    ///   (fallo de dialado).
    [[nodiscard]] task<expected<Socket>> acquire(Proactor& proactor, NodeId node);

    /// @brief Devuelve @p socket a la *free-list* de @p node para reúso; si está llena
    ///   (`max_idle_per_node`), lo cierra en su lugar (acotación).
    /// @pre @p socket es una conexión sana al plano de datos de @p node (una rota se descarta, no
    /// se
    ///   devuelve).
    void release(NodeId node, Socket socket);

    /// @brief Conexiones ociosas actualmente guardadas para @p node (0 si ninguna).
    [[nodiscard]] std::size_t idle(NodeId node) const noexcept;

private:
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    const PeerDirectory&
        peers_;  ///< REACTOR-LOCAL: directorio del plano de datos (no propietario).
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::size_t max_idle_per_node_;
    std::unordered_map<NodeId, std::vector<Socket>> idle_;  ///< *free-list* de conexiones por nodo.
};

}  // namespace nexus
