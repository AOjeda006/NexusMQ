/// @file   consensus/raft_node.hpp
/// @brief  RaftNode: máquina de estados de una réplica de partición (ADR-0003/0015).
/// @ingroup consensus

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "consensus/raft_log.hpp"
#include "consensus/raft_rpc.hpp"
#include "consensus/raft_state.hpp"

namespace nexus {

/// @brief Parámetros temporales de Raft (§7.10). Afinidad: INMUTABLE.
struct RaftConfig {
    /// Mínimo del *election timeout* (aleatorizado en `[min, max]` para evitar votos divididos).
    std::chrono::milliseconds election_timeout_min{1000};
    /// Máximo del *election timeout*.
    std::chrono::milliseconds election_timeout_max{1500};
    /// Periodo de *heartbeat* del líder (debe ser << election_timeout).
    std::chrono::milliseconds heartbeat_interval{150};
    /// Semilla del RNG del *election timeout* (se combina con el id del nodo; reproducible).
    std::uint64_t random_seed = 0;
};

/// @brief Mensaje saliente de Raft (un RPC dirigido a un peer). Afinidad: CROSS-CORE (se
/// transporta).
/// @details El núcleo (ADR-0015) **no hace E/S**: encola estos mensajes y el emisor los drena con
///   `take_messages()` y los envía por la red. `payload` es el RPC (request o reply).
struct RaftMessage {
    NodeId from = 0;
    NodeId to = 0;
    std::variant<RequestVoteArgs, RequestVoteReply, AppendEntriesArgs, AppendEntriesReply> payload;
};

/// @brief Réplica de Raft de una partición como **máquina de estados síncrona sin E/S** (ADR-0015).
/// @details Afinidad: REACTOR-LOCAL. Consume *entradas* (`tick`, `on_*`) con el instante `now`
///   inyectado y produce *salidas* (mensajes proactivos en una cola + avance de `commit_index`). No
///   tiene reloj, red ni corrutinas propias: el reactor (o el arnés de simulación) le inyecta el
///   tiempo, le entrega los RPC recibidos y transporta los mensajes de `take_messages()`. Los
///   manejadores `on_request_vote`/`on_append_entries` devuelven su *reply* directamente (el emisor
///   lo enruta al `on_*_reply` del origen).
/// @invariant Roles y términos siguen las reglas de Raft (§5): término monótono; a lo sumo un líder
///   por término; un voto por término.
/// @note Cubre el ciclo completo de réplica: **elección con pre-vote** (§5.2/§9.6), **voto**,
///   **manejo de `AppendEntries` en el seguidor** con *log matching* (§5.3/§7.11 #5), y la
///   **propuesta** del líder con replicación, retroceso de `next_index` por `conflict_index` y
///   avance de `commit_index` por mayoría del término actual (§5.4) — `commit_index` es la
///   *high-watermark*.
class RaftNode {
public:
    RaftNode(NodeId self, std::vector<NodeId> peers, RaftLog& log, RaftConfig config);

    /// @brief Avanza el reloj lógico a @p now: vence *election*/*heartbeat* y dispara transiciones.
    void tick(MonoTime now);

    /// @brief (Solo líder) Propone @p batch como una nueva entrada del log y la replica (§7.11 #1).
    /// @details Anexa la entrada (término actual) al `RaftLog`, emite `AppendEntries` a los peers y
    ///   reevalúa el `commit_index`. Devuelve el **índice** asignado; el llamante espera a que
    ///   `commit_index()` lo alcance para considerar la escritura confirmada (acks=quorum).
    /// @return `Unsupported` si el nodo no es líder; error de E/S si falla el log.
    [[nodiscard]] expected<Index> propose(const RecordBatch& batch);

    /// @brief Procesa un `RequestVote` recibido y devuelve la respuesta (§5.2/§5.4).
    [[nodiscard]] RequestVoteReply on_request_vote(MonoTime now, const RequestVoteArgs& args);

    /// @brief Procesa un `AppendEntries` recibido (§5.3/§7.11 #5) y devuelve la respuesta.
    [[nodiscard]] AppendEntriesReply on_append_entries(MonoTime now, const AppendEntriesArgs& args);

    /// @brief Procesa la respuesta a un `RequestVote` (cuenta votos → líder).
    void on_request_vote_reply(MonoTime now, NodeId from, const RequestVoteReply& reply);

    /// @brief Procesa la respuesta a un `AppendEntries`: actualiza `match_index`/`next_index`,
    ///   avanza `commit_index` por mayoría (éxito) o retrocede `next_index` y reintenta (fallo).
    void on_append_entries_reply(MonoTime now, NodeId from, const AppendEntriesReply& reply);

    /// @brief Extrae los mensajes proactivos encolados (el emisor los transporta).
    [[nodiscard]] std::vector<RaftMessage> take_messages();

    [[nodiscard]] RaftRole role() const noexcept { return role_; }
    [[nodiscard]] bool is_leader() const noexcept { return role_ == RaftRole::Leader; }
    [[nodiscard]] Term current_term() const noexcept { return persistent_.current_term(); }
    [[nodiscard]] Index commit_index() const noexcept { return volatile_.commit_index(); }
    [[nodiscard]] Epoch leader_epoch() const noexcept { return leader_epoch_; }
    [[nodiscard]] std::optional<NodeId> leader_hint() const noexcept { return leader_id_; }
    [[nodiscard]] NodeId id() const noexcept { return self_; }

private:
    void become_follower(MonoTime now, Term term);
    /// Inicia la ronda de **pre-votos** (§9.6): sondea sin subir el término ni votarse a sí mismo.
    void start_pre_election(MonoTime now);
    void become_candidate(MonoTime now);
    void become_leader(MonoTime now);
    void reset_election_timer(MonoTime now);
    void reset_heartbeat_timer(MonoTime now);
    /// Difunde `RequestVote` a los peers; @p pre_vote distingue el sondeo (§9.6) del voto real.
    void broadcast_request_vote(bool pre_vote);
    /// Decide si conceder un **pre-voto** sin mutar estado (no sube término ni rearma el
    /// temporizador).
    [[nodiscard]] RequestVoteReply make_pre_vote_reply(MonoTime now,
                                                       const RequestVoteArgs& args) const;
    /// Envía `AppendEntries` a @p peer desde su `next_index` (vacío = *heartbeat*).
    void replicate_to(NodeId peer);
    /// Replica a todos los peers (también sirve de ronda de *heartbeats*).
    void replicate_all();
    /// (Líder) Avanza `commit_index` al mayor índice replicado en mayoría del término actual
    /// (§5.4).
    void advance_commit_index();
    /// Aplica las entradas de un `AppendEntries` (saltar coincidentes, truncar conflicto, anexar).
    [[nodiscard]] bool append_entries_from(const std::vector<RaftLogEntry>& entries);
    /// ¿El log `(last_log_index, last_log_term)` es al menos tan reciente como el mío? (§5.4).
    [[nodiscard]] bool log_is_up_to_date(Index last_log_index, Term last_log_term) const;
    [[nodiscard]] std::size_t cluster_size() const noexcept { return peers_.size() + 1; }
    [[nodiscard]] bool has_majority(std::size_t votes) const noexcept {
        return votes * 2 > cluster_size();
    }
    [[nodiscard]] std::chrono::milliseconds random_election_timeout();

    /// Tope de entradas por `AppendEntries` (acota el tamaño del mensaje de replicación).
    static constexpr std::size_t kMaxEntriesPerAppend = 64;

    template <class Payload>
    void send(NodeId to, Payload payload) {
        outbox_.push_back(RaftMessage{.from = self_, .to = to, .payload = std::move(payload)});
    }

    NodeId self_;
    std::vector<NodeId> peers_;
    RaftLog& log_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    RaftConfig config_;

    RaftRole role_ = RaftRole::Follower;
    RaftPersistentState persistent_;
    RaftVolatileState volatile_;
    Epoch leader_epoch_ = 0;
    std::optional<NodeId> leader_id_;
    /// Votantes a favor en la elección en curso.
    std::unordered_set<NodeId> votes_granted_;
    /// Último índice enviado a cada peer en el último `AppendEntries` (solo líder).
    std::unordered_map<NodeId, Index> last_sent_;

    /// Cuándo, sin contacto del líder, pasar a candidato.
    MonoTime election_deadline_;
    /// Cuándo el líder debe emitir el próximo *heartbeat*.
    MonoTime heartbeat_deadline_;
    /// El primer `tick` arma el temporizador sin disparar.
    bool started_ = false;

    std::minstd_rand rng_;
    std::vector<RaftMessage> outbox_;
};

}  // namespace nexus
