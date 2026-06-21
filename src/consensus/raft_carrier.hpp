/// @file   consensus/raft_carrier.hpp
/// @brief  RaftCarrier: portador de la FSM de Raft de una partición (ADR-0025).
/// @ingroup consensus

#pragma once

#include <string>

#include "common/types.hpp"
#include "consensus/raft_rpc.hpp"
#include "consensus/raft_wire.hpp"

namespace nexus {

class RaftNode;
class RaftStateStore;

/// @brief Sumidero de mensajes de Raft hacia la red (transporte inter-nodo). Afinidad:
///   REACTOR-LOCAL.
/// @details Abstracción (DIP) que el portador usa para **enviar** un `RaftEnvelope` al peer
///   destino. El transporte real (conexiones TCP a peers) la implementa en producción; los tests
///   inyectan un doble que enruta los sobres por una red virtual determinista. La (de)serialización
///   del sobre la decide el transporte, no el portador.
class RaftMessageSink {
public:
    RaftMessageSink() = default;
    RaftMessageSink(const RaftMessageSink&) = delete;
    RaftMessageSink& operator=(const RaftMessageSink&) = delete;
    RaftMessageSink(RaftMessageSink&&) = delete;
    RaftMessageSink& operator=(RaftMessageSink&&) = delete;
    virtual ~RaftMessageSink() = default;

    /// @brief Envía @p envelope al peer `envelope.message.to` (lo entrega el transporte).
    virtual void send(const RaftEnvelope& envelope) = 0;
};

/// @brief Conduce la FSM de Raft (ADR-0015) de **una** réplica de partición: la avanza por tiempo,
///   le entrega los RPC recibidos y transporta sus mensajes salientes. Afinidad: REACTOR-LOCAL.
/// @details Materializa el plano inter-nodo de ADR-0025 como código de producción: lo que el arnés
///   de tests hacía con una red virtual lo hace aquí el portador, afinado al reactor dueño de la
///   partición. Envuelve cada `RaftMessage` saliente en un `RaftEnvelope` con la identidad de la
///   réplica `(topic, partition)` y lo manda por el `RaftMessageSink`; al recibir un RPC, lo enruta
///   al manejador `on_*` de su `RaftNode` y, si era una *request*, devuelve la *reply* por el mismo
///   sumidero. No serializa ni toca sockets (eso es el transporte): así es testeable sin red.
/// @invariant `topic`/`partition` identifican la réplica que conduce; no cambian en su vida.
/// @note No posee el `RaftNode`, el sumidero ni el almacén (referencias/puntero no propietarios):
///   los posee la `ReplicatedPartition`, el transporte del reactor y la composición. No copiable ni
///   movible.
class RaftCarrier {
public:
    /// @brief Construye el portador de la réplica `(topic, partition)` sobre @p node y @p sink.
    /// @param store Almacén durable del estado persistente (D1); `nullptr` desactiva la
    /// persistencia
    ///   (útil en pruebas de la lógica del portador). Si no es nulo, llama a `recover()` tras
    ///   construir y el portador persistirá el estado **antes** de transportar (regla §5).
    RaftCarrier(std::string topic, PartitionId partition, RaftNode& node, RaftMessageSink& sink,
                RaftStateStore* store = nullptr);

    RaftCarrier(const RaftCarrier&) = delete;
    RaftCarrier& operator=(const RaftCarrier&) = delete;
    RaftCarrier(RaftCarrier&&) = delete;
    RaftCarrier& operator=(RaftCarrier&&) = delete;
    ~RaftCarrier() = default;

    /// @brief Siembra el estado persistente leído de disco al arrancar (D1).
    /// @details Carga el `RaftStateStore` y lo restaura en el `RaftNode` antes de cualquier `tick`.
    ///   No-op (éxito) si no hay almacén. Llamar **una vez**, tras construir.
    /// @return El error de E/S del almacén si la carga falla.
    [[nodiscard]] expected<void> recover();

    /// @brief Avanza la FSM a @p now (vence *election*/*heartbeat*) y transporta lo que encole.
    void on_tick(MonoTime now);

    /// @brief Entrega un RPC recibido: lo enruta al `on_*` del `RaftNode`, devuelve la *reply* (si
    ///   la había) y transporta cualquier mensaje que el manejador genere.
    void on_message(MonoTime now, const RaftMessage& message);

    [[nodiscard]] const std::string& topic() const noexcept { return topic_; }
    [[nodiscard]] PartitionId partition() const noexcept { return partition_; }

private:
    /// @brief Persiste el estado (si cambió) y drena la cola de salida hacia el sumidero.
    /// @details Regla §5 de Raft: si `persistent_state_dirty()`, guarda con `fsync` **antes** de
    ///   transportar. Si el `save` falla, **no** transporta (los mensajes quedan en la cola para el
    ///   próximo intento): nunca se responde a un RPC sin haber persistido el término/voto.
    void flush_outbox();
    /// Envuelve @p message en un `RaftEnvelope` de esta réplica y lo manda por el sumidero.
    void emit(const RaftMessage& message);

    std::string topic_;
    PartitionId partition_;
    RaftNode& node_;         // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    RaftMessageSink& sink_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    RaftStateStore* store_;  ///< Almacén durable del estado persistente (no propietario; opcional).
};

}  // namespace nexus
