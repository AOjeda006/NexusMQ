/// @file   cluster/raft_transport.hpp
/// @brief  RaftTransport: sumidero saliente de Raft sobre conexiones TCP a peers (ADR-0025).
/// @ingroup cluster

#pragma once

#include <cstddef>
#include <deque>
#include <unordered_map>

#include "cluster/peer_directory.hpp"
#include "common/move_only_function.hpp"
#include "common/task.hpp"
#include "common/types.hpp"
#include "consensus/raft_carrier.hpp"  // RaftMessageSink
#include "consensus/raft_wire.hpp"
#include "io/socket.hpp"

namespace nexus {

class Proactor;

/// @brief Sumidero de Raft que transporta los sobres salientes por **conexiones TCP persistentes**
///   a los peers del clúster (ADR-0025). Afinidad: REACTOR-LOCAL.
/// @details Implementa `RaftMessageSink`: el `RaftCarrier` llama a `send` (síncrono, en el hilo del
///   reactor) y el transporte **encola** el sobre por peer y, si no hay uno en marcha, lanza una
///   **corrutina emisora** que conecta (la primera vez, `Socket::async_connect`) y drena la cola
///   con el `RaftEnvelopeWriter`. La conexión se **reutiliza** entre sobres; ante un fallo de envío
///   se cierra y se reconecta en el siguiente intento. Es **best-effort** (Raft tolera pérdida): si
///   la conexión falla o la cola se llena, se descartan sobres (Raft los reenvía en el próximo
///   tick). Todo el estado es reactor-local (un transporte por núcleo): sin locks.
/// @note No posee el `Proactor` ni el `PeerDirectory` (referencias no propietarias); el
/// *composition
///   root* (el `Server`) los provee y garantiza que sobreviven al transporte. No copiable ni
///   movible (mantiene corrutinas emisoras en vuelo que apuntan a su estado).
class RaftTransport final : public RaftMessageSink {
public:
    /// Lanza una corrutina de servicio en el reactor dueño (lo cablea el *composition root* a
    /// `Reactor::spawn`). El reactor posee el *frame* hasta que la corrutina termina.
    using Spawner = MoveOnlyFunction<void(task<void>)>;

    /// Parámetros del transporte (límites y cotas anti-DoS/backpressure).
    struct Config {
        /// Máximo de sobres encolados por peer; al excederse se descarta el más antiguo
        /// (backpressure: Raft reenvía).
        std::size_t max_queue = 1024;
    };

    /// @brief Construye el transporte del nodo @p self.
    /// @param self Identidad de este nodo (no se conecta a sí mismo; los sobres a `self` se
    /// ignoran).
    /// @param directory Directorio de peers (resuelve `NodeId` -> dirección inter-nodo). No
    /// propietario.
    /// @param[in,out] proactor Puerto de E/S del reactor dueño. No propietario.
    /// @param spawner Lanza las corrutinas emisoras en el reactor dueño.
    RaftTransport(NodeId self, const PeerDirectory& directory, Proactor& proactor, Spawner spawner);

    /// @copydoc RaftTransport
    /// @param config Límites del transporte (cola por peer).
    RaftTransport(NodeId self, const PeerDirectory& directory, Proactor& proactor, Spawner spawner,
                  Config config);

    RaftTransport(const RaftTransport&) = delete;
    RaftTransport& operator=(const RaftTransport&) = delete;
    RaftTransport(RaftTransport&&) = delete;
    RaftTransport& operator=(RaftTransport&&) = delete;
    ~RaftTransport() override = default;

    /// @brief Encola @p envelope hacia `envelope.message.to` y arranca la emisión si hace falta.
    /// @details No bloquea: serializa el envío a una corrutina. Descarta (best-effort) si el
    /// destino
    ///   es desconocido o es el propio nodo, o si la cola del peer está llena (el más antiguo).
    void send(const RaftEnvelope& envelope) override;

private:
    /// Estado por peer: cola de salida y conexión persistente (reutilizada entre sobres).
    struct PeerLink {
        std::deque<RaftEnvelope> outbox;  ///< Sobres pendientes de enviar a este peer.
        Socket socket;                    ///< Conexión TCP (cerrada si no conectada).
        bool sending = false;             ///< ¿Hay una corrutina emisora en marcha para este peer?
    };

    /// @brief Corrutina emisora de @p peer: conecta si hace falta y drena su cola de salida.
    /// @details Reutiliza la conexión; ante fallo de envío la cierra (reconecta en el siguiente
    ///   intento) y ante fallo de conexión descarta la cola (Raft reenvía). Sale al vaciar la cola
    ///   dejando la conexión abierta; un `send` posterior la relanza y reaprovecha el socket.
    [[nodiscard]] task<void> run_sender(NodeId peer);

    NodeId self_;
    const PeerDirectory& directory_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Proactor& proactor_;              // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Spawner spawner_;
    Config config_;
    /// Estado por peer. `unordered_map` da **estabilidad de referencias** a los `PeerLink` ante
    /// inserciones (las corrutinas emisoras retienen un puntero a su `PeerLink`); nunca se borran.
    std::unordered_map<NodeId, PeerLink> links_;
};

}  // namespace nexus
