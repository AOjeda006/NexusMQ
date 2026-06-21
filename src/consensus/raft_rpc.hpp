/// @file   consensus/raft_rpc.hpp
/// @brief  RPC de Raft (§7.8): RequestVote, AppendEntries, InstallSnapshot, con (de)serialización
///         sobre el codec de `nexus-protocol`.
/// @ingroup consensus

#pragma once

#include <cstdint>
#include <variant>
#include <vector>

#include "common/error.hpp"
#include "common/types.hpp"
#include "consensus/raft_state.hpp"

namespace nexus {

class Encoder;
class Decoder;

/// @brief Solicitud de voto de un candidato (§5.2 del paper de Raft). Afinidad: INMUTABLE.
/// @details `pre_vote` distingue la fase de *pre-vote* (§6.6): un candidato pregunta si ganaría
///   **sin** incrementar su término, evitando perturbar al líder con nodos que se reincorporan.
struct RequestVoteArgs {
    /// Término del candidato.
    Term term = 0;
    /// Quién solicita el voto.
    NodeId candidate_id = 0;
    /// Índice de la última entrada del log del candidato.
    Index last_log_index = 0;
    /// Término de la última entrada del log del candidato.
    Term last_log_term = 0;
    /// `true` en la ronda de *pre-vote* (no incrementa término).
    bool pre_vote = false;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<RequestVoteArgs> decode(Decoder& dec);
    bool operator==(const RequestVoteArgs&) const = default;
};

/// @brief Respuesta a `RequestVote`. Afinidad: INMUTABLE.
struct RequestVoteReply {
    /// Término del votante (para que el candidato se actualice).
    Term term = 0;
    /// `true` si concede el voto.
    bool vote_granted = false;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<RequestVoteReply> decode(Decoder& dec);
    bool operator==(const RequestVoteReply&) const = default;
};

/// @brief Replicación de entradas + *heartbeat* del líder (§5.3). Afinidad: INMUTABLE.
/// @details `entries` vacío es un *heartbeat*. `leader_epoch` permite descartar peticiones de
///   líderes obsoletos en la capa de partición (`NOT_LEADER_FOR_PARTITION`).
struct AppendEntriesArgs {
    /// Término del líder.
    Term term = 0;
    /// Líder que replica (para redirigir clientes).
    NodeId leader_id = 0;
    /// Índice de la entrada inmediatamente anterior a `entries`.
    Index prev_log_index = 0;
    /// Término de `prev_log_index` (chequeo de consistencia).
    Term prev_log_term = 0;
    /// Entradas a replicar (vacío = heartbeat).
    std::vector<RaftLogEntry> entries;
    /// `commit_index` del líder.
    Index leader_commit = 0;
    /// Época de liderazgo de la partición.
    Epoch leader_epoch = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<AppendEntriesArgs> decode(Decoder& dec);
    bool operator==(const AppendEntriesArgs&) const = default;
};

/// @brief Respuesta a `AppendEntries`. Afinidad: INMUTABLE.
/// @details `conflict_index` acelera el retroceso del líder ante un fallo de consistencia: indica
///   desde dónde reintentar (optimización del §5.3, en vez de decrementar de uno en uno).
struct AppendEntriesReply {
    /// Término del seguidor.
    Term term = 0;
    /// `true` si el seguidor aceptó las entradas.
    bool success = false;
    /// Sugerencia de reintento ante fallo (0 si `success`).
    Index conflict_index = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<AppendEntriesReply> decode(Decoder& dec);
    bool operator==(const AppendEntriesReply&) const = default;
};

/// @brief Orden del líder a un seguidor al día para que arranque una elección de inmediato
///   (*leadership transfer*, §3.10). Afinidad: INMUTABLE.
/// @details El objetivo inicia una elección **real** (saltándose el *pre-vote* y el *lease*): como
///   su log está al día, gana enseguida y el líder anterior cede al observar el término mayor.
struct TimeoutNowArgs {
    /// Término del líder que transfiere.
    Term term = 0;
    /// Líder que cede el liderazgo.
    NodeId leader_id = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<TimeoutNowArgs> decode(Decoder& dec);
    bool operator==(const TimeoutNowArgs&) const = default;
};

/// @brief Transferencia de un snapshot a un seguidor muy rezagado (§7 del paper). Afinidad:
///   INMUTABLE.
struct InstallSnapshotArgs {
    /// Término del líder.
    Term term = 0;
    /// Líder que envía el snapshot.
    NodeId leader_id = 0;
    /// Snapshot (último índice/término incluidos + estado).
    Snapshot snapshot;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<InstallSnapshotArgs> decode(Decoder& dec);
    bool operator==(const InstallSnapshotArgs&) const = default;
};

/// @brief Respuesta a `InstallSnapshot`. Afinidad: INMUTABLE.
struct InstallSnapshotReply {
    /// Término del seguidor.
    Term term = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<InstallSnapshotReply> decode(Decoder& dec);
    bool operator==(const InstallSnapshotReply&) const = default;
};

/// @brief Mensaje saliente de Raft (un RPC dirigido a un peer). Afinidad: CROSS-CORE (se
/// transporta).
/// @details El núcleo (ADR-0015) **no hace E/S**: encola estos mensajes y el emisor los drena con
///   `take_messages()` y los envía por la red. `payload` es el RPC (request o reply); su
///   discriminante (`index()` del `variant`) viaja como `type` en el sobre de wire (ADR-0025).
struct RaftMessage {
    NodeId from = 0;
    NodeId to = 0;
    std::variant<RequestVoteArgs, RequestVoteReply, AppendEntriesArgs, AppendEntriesReply,
                 TimeoutNowArgs, InstallSnapshotArgs, InstallSnapshotReply>
        payload;

    bool operator==(const RaftMessage&) const = default;
};

}  // namespace nexus
