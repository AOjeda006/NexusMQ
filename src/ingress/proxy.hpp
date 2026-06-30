/// @file   ingress/proxy.hpp
/// @brief  Proxy: modo proxy del ingress — enruta clientes "tontos" al líder (consistent-hash).
/// @ingroup ingress

#pragma once

#include <cstddef>
#include <optional>
#include <string_view>

#include "common/error.hpp"
#include "common/task.hpp"
#include "common/types.hpp"
#include "ingress/load_balancer.hpp"
#include "io/proactor.hpp"
#include "io/socket.hpp"

namespace nexus {

/// @brief Ingress en **modo proxy** (ADR-0006): acepta clientes que no conocen la topología y los
///   atiende reenviando sus tramas al nodo líder. Afinidad: REACTOR-LOCAL.
/// @details Dos responsabilidades: (1) **enrutado** — `route` mapea una *partition key* a un nodo
///   con el anillo *consistent-hashing* del `LoadBalancer` (mínima perturbación al añadir/quitar
///   nodos); (2) **relevo** — `forward` ejecuta el bucle petición/respuesta del protocolo (framed,
///   con `correlation_id`, §7.2) entre el cliente y una conexión ya establecida con el líder: lee
///   una trama del cliente, la reenvía al líder, lee la respuesta y la devuelve al cliente, hasta
///   que el cliente cierra. El **dialado** del líder y el *pool* de conexiones aguas arriba son
///   cableado del servidor (se difieren); aquí vive la lógica enrutable y testeable.
/// @note **Ajuste del diseño:** el diseño firmaba `forward(Connection&)`; como aún no existe un
///   tipo `Connection`, `forward` toma los dos `Socket` (cliente y líder) ya conectados. El relevo
///   es a nivel de **trama** (no de bytes en bruto) porque el plano de datos es petición/respuesta.
class Proxy {
public:
    /// Tope por defecto del tamaño de trama relevada (anti-DoS); igual que el plano de datos.
    static constexpr std::size_t kDefaultMaxFrame = 16UL * 1024 * 1024;

    explicit Proxy(LoadBalancer& balancer) noexcept;

    /// @brief Elige el nodo destino para @p key con el anillo *consistent-hashing*.
    /// @return El nodo, o `nullopt` si no hay nodos en el anillo.
    [[nodiscard]] std::optional<NodeId> route(std::string_view key);

    /// @brief Releva tramas entre @p client y @p upstream (ya conectado al líder) hasta que el
    ///   cliente cierra (fin limpio) o falla el relevo (error). Cada vuelta: lee petición del
    ///   cliente → la envía al líder → lee la respuesta del líder → la devuelve al cliente.
    [[nodiscard]] task<expected<void>> forward(Proactor& proactor, Socket& client, Socket& upstream,
                                               std::size_t max_frame = kDefaultMaxFrame);

private:
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    LoadBalancer& balancer_;  ///< REACTOR-LOCAL: el balanceador vive en el mismo reactor.
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
};

}  // namespace nexus
