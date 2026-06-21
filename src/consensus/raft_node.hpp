/// @file   consensus/raft_node.hpp
/// @brief  RaftNode: máquina de estados de una réplica de partición (ADR-0003/0015).
/// @ingroup consensus

#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <unordered_map>
#include <unordered_set>
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
///   **manejo de `AppendEntries` en el seguidor** con *log matching* (§5.3/§7.11 #5), la
///   **propuesta** del líder con replicación, retroceso de `next_index` por `conflict_index` y
///   avance de `commit_index` por mayoría del término actual (§5.4) — `commit_index` es la
///   *high-watermark* —, la **transferencia de liderazgo** ordenada vía `TimeoutNow` (§3.10) y
///   miembros **learner** no votantes (§4.2.1: replican sin contar para el quórum ni votar).
class RaftNode {
public:
    /// @param learners Subconjunto de @p peers que son **no votantes** (§4.2.1): el líder les
    ///   replica el log, pero no cuentan para el quórum, no se les pide voto y, si el propio nodo
    ///   es learner, nunca se postula.
    RaftNode(NodeId self, std::vector<NodeId> peers, RaftLog& log, RaftConfig config,
             std::vector<NodeId> learners = {});

    /// @brief Avanza el reloj lógico a @p now: vence *election*/*heartbeat* y dispara transiciones.
    void tick(MonoTime now);

    /// @brief (Solo líder) Propone @p batch como una nueva entrada del log y la replica (§7.11 #1).
    /// @details Anexa la entrada (término actual) al `RaftLog`, emite `AppendEntries` a los peers y
    ///   reevalúa el `commit_index`. Devuelve el **índice** asignado; el llamante espera a que
    ///   `commit_index()` lo alcance para considerar la escritura confirmada (acks=quorum).
    /// @return `Unsupported` si el nodo no es líder; error de E/S si falla el log.
    [[nodiscard]] expected<Index> propose(const RecordBatch& batch);

    /// @brief (Solo líder) Transfiere el liderazgo a @p target de forma ordenada (§3.10).
    /// @details Si @p target ya está al día, le envía `TimeoutNow` para que se postule ya; si va
    ///   rezagado, lo pone al día primero y envía `TimeoutNow` al confirmar que alcanzó la cola. El
    ///   líder cede al observar el término mayor del nuevo líder.
    /// @return `Unsupported` si el nodo no es líder; `InvalidArgument` si @p target no es un peer.
    [[nodiscard]] expected<void> transfer_leadership(NodeId target);

    /// @brief Procesa un `RequestVote` recibido y devuelve la respuesta (§5.2/§5.4).
    [[nodiscard]] RequestVoteReply on_request_vote(MonoTime now, const RequestVoteArgs& args);

    /// @brief Procesa un `AppendEntries` recibido (§5.3/§7.11 #5) y devuelve la respuesta.
    [[nodiscard]] AppendEntriesReply on_append_entries(MonoTime now, const AppendEntriesArgs& args);

    /// @brief Procesa un `TimeoutNow` recibido: arranca una elección real inmediata (§3.10).
    void on_timeout_now(MonoTime now, const TimeoutNowArgs& args);

    /// @brief Procesa un `InstallSnapshot` recibido (seguidor, §7) y devuelve la respuesta.
    /// @details Rechaza líderes obsoletos; ante un término ≥ propio reconoce al líder y rearma.
    ///   Adopta el snapshot en el `RaftLog` (reposiciona la base) y avanza `commit_index` hasta el
    ///   índice incluido. La E/S durable la hace el `RaftLog` (instala y persiste la base).
    [[nodiscard]] InstallSnapshotReply on_install_snapshot(MonoTime now,
                                                           const InstallSnapshotArgs& args);

    /// @brief Procesa la respuesta a un `InstallSnapshot` (líder): pone `match_index`/`next_index`
    ///   del seguidor en el índice del snapshot y reanuda la replicación normal.
    void on_install_snapshot_reply(MonoTime now, NodeId from, const InstallSnapshotReply& reply);

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

    /// @brief ¿Cambió el estado persistente (término/voto) desde el último
    /// `clear_persistent_dirty`?
    /// @details Gancho de durabilidad para el portador de la FSM (ADR-0015): tras cada entrada
    ///   (`tick`/`on_*`/`propose`), si esto es `true`, debe persistir `persistent_state()` con
    ///   `fsync` (vía `RaftStateStore::save`) **antes** de transportar `take_messages()` —regla de
    ///   seguridad de Raft §5: el término y el voto se guardan antes de responder a un RPC— y luego
    ///   llamar `clear_persistent_dirty()`.
    [[nodiscard]] bool persistent_state_dirty() const noexcept { return persistent_dirty_; }

    /// @brief Estado persistente actual (término y voto), para que el portador lo persista.
    [[nodiscard]] const RaftPersistentState& persistent_state() const noexcept {
        return persistent_;
    }

    /// @brief Marca el estado persistente como ya guardado (tras un `save` durable correcto).
    void clear_persistent_dirty() noexcept { persistent_dirty_ = false; }

    /// @brief Restaura el estado persistente cargado de disco **al arrancar**.
    /// @details Lo invoca el portador justo tras construir el nodo (con lo que devuelve
    ///   `RaftStateStore::load`) y antes de la primera entrada. No marca *dirty*: lo recién leído
    ///   del disco no hay que volver a escribirlo.
    /// @pre Se llama una sola vez, antes del primer `tick`/`on_*`/`propose`.
    void restore_persistent_state(RaftPersistentState state) noexcept {
        persistent_ = state;
        persistent_dirty_ = false;
    }

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
    /// Envía `InstallSnapshot` a @p peer cuando su `next_index` cae en/bajo el snapshot del líder.
    void send_snapshot_to(NodeId peer);
    /// Replica a todos los peers (también sirve de ronda de *heartbeats*).
    void replicate_all();
    /// (Líder) Avanza `commit_index` al mayor índice replicado en mayoría del término actual
    /// (§5.4).
    void advance_commit_index();
    /// (Líder) Envía `TimeoutNow` a @p peer si está al día, o sigue replicando si va rezagado.
    void maybe_transfer_to(NodeId peer);
    /// Aplica las entradas de un `AppendEntries` (saltar coincidentes, truncar conflicto, anexar).
    [[nodiscard]] bool append_entries_from(const std::vector<RaftLogEntry>& entries);
    /// ¿El log `(last_log_index, last_log_term)` es al menos tan reciente como el mío? (§5.4).
    [[nodiscard]] bool log_is_up_to_date(Index last_log_index, Term last_log_term) const;
    /// ¿@p node es un miembro **votante** (no figura en `learners_`)?
    [[nodiscard]] bool is_voter(NodeId node) const {
        return std::ranges::find(learners_, node) == learners_.end();
    }
    /// Número de miembros **votantes** del clúster (self + peers, excluyendo learners).
    [[nodiscard]] std::size_t cluster_size() const noexcept {
        std::size_t voters = is_voter(self_) ? 1 : 0;
        for (const NodeId peer : peers_) {
            voters += is_voter(peer) ? 1 : 0;
        }
        return voters;
    }
    [[nodiscard]] bool has_majority(std::size_t votes) const noexcept {
        return votes * 2 > cluster_size();
    }
    [[nodiscard]] std::chrono::milliseconds random_election_timeout();

    /// Avanza el término **y** marca el estado persistente como sucio (debe persistirse antes de
    /// transportar; ver `persistent_state_dirty`). Centraliza la regla de durabilidad en un punto.
    void advance_term(Term term) {
        persistent_.advance_term(term);
        persistent_dirty_ = true;
    }
    /// Registra el voto del término actual **y** marca el estado persistente como sucio.
    void record_vote(NodeId candidate) {
        persistent_.record_vote(candidate);
        persistent_dirty_ = true;
    }

    /// Tope de entradas por `AppendEntries` (acota el tamaño del mensaje de replicación).
    static constexpr std::size_t kMaxEntriesPerAppend = 64;

    template <class Payload>
    void send(NodeId to, Payload payload) {
        outbox_.push_back(RaftMessage{.from = self_, .to = to, .payload = std::move(payload)});
    }

    NodeId self_;
    std::vector<NodeId> peers_;
    /// Peers no votantes (§4.2.1): replican el log pero no cuentan para el quórum ni votan.
    std::vector<NodeId> learners_;
    RaftLog& log_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    RaftConfig config_;

    RaftRole role_ = RaftRole::Follower;
    RaftPersistentState persistent_;
    /// ¿`persistent_` cambió y aún no se ha persistido en disco? (gancho de durabilidad, §5).
    bool persistent_dirty_ = false;
    RaftVolatileState volatile_;
    Epoch leader_epoch_ = 0;
    std::optional<NodeId> leader_id_;
    /// Votantes a favor en la elección en curso.
    std::unordered_set<NodeId> votes_granted_;
    /// Último índice enviado a cada peer en el último `AppendEntries` (solo líder).
    std::unordered_map<NodeId, Index> last_sent_;
    /// Destino de una transferencia de liderazgo en curso (espera a que se ponga al día).
    std::optional<NodeId> transfer_target_;

    /// Cuándo, sin contacto del líder, pasar a candidato.
    MonoTime election_deadline_;
    /// Último contacto válido de un líder (`AppendEntries`); base del *lease* del pre-vote (§9.6).
    MonoTime last_leader_contact_;
    /// Cuándo el líder debe emitir el próximo *heartbeat*.
    MonoTime heartbeat_deadline_;
    /// El primer `tick` arma el temporizador sin disparar.
    bool started_ = false;

    std::minstd_rand rng_;
    std::vector<RaftMessage> outbox_;
};

}  // namespace nexus
