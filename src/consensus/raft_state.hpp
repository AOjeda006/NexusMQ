/// @file   consensus/raft_state.hpp
/// @brief  Tipos de estado de Raft por partición (ADR-0003): rol, entrada de log, estado
///         persistente y volátil, snapshot.
/// @ingroup consensus

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"

namespace nexus {

/// @brief Rol de una réplica en su grupo Raft. Afinidad: REACTOR-LOCAL.
/// @details Transiciones (§5.5 + pre-vote §9.6): `Follower` →(election timeout)→ `PreCandidate`
///   →(mayoría de pre-votos)→ `Candidate` →(mayoría de votos)→ `Leader`; cualquier rol →(término
///   mayor observado)→ `Follower`.
enum class RaftRole : std::uint8_t {
    Follower,      ///< Réplica pasiva: replica del líder y vota.
    PreCandidate,  ///< Sondea pre-votos sin subir su término (anti-disrupción §9.6).
    Candidate,     ///< Solicita votos tras ganar la ronda de pre-votos.
    Leader,        ///< Sirve produce/fetch y replica su log a los seguidores.
};

/// @brief Nombre estable de un `RaftRole`
/// (`"follower"`/`"pre_candidate"`/`"candidate"`/`"leader"`),
///   para observabilidad (plano de operación).
[[nodiscard]] std::string_view raft_role_name(RaftRole role) noexcept;

/// @brief Naturaleza de una entrada del log de Raft.
enum class RaftEntryType : std::uint8_t {
    Data,    ///< Carga de datos (un `RecordBatch` serializado).
    Config,  ///< Cambio de configuración del grupo (membresía).
};

/// @brief Una entrada del log replicado de Raft. Afinidad: INMUTABLE.
/// @details Asocia un `(term, index)` a una carga. **Posee** sus bytes (`payload`): es un tipo de
///   valor que debe sobrevivir a cualquier búfer transitorio del que provenga (mismo criterio que
///   `RecordBatch`, que posee sus records). El diseño preveía `ByteSpan`; se ajusta a propiedad
///   por seguridad de vida.
/// @invariant `index >= 1` (índices de Raft 1-based; `0` es el centinela "antes de la primera").
struct RaftLogEntry {
    /// Término en que el líder creó la entrada.
    Term term = 0;
    /// Posición 1-based en el log de Raft.
    Index index = 0;
    /// Datos o cambio de configuración.
    RaftEntryType type = RaftEntryType::Data;
    /// Carga serializada (RecordBatch o config).
    std::vector<std::byte> payload;

    bool operator==(const RaftLogEntry&) const = default;
};

/// @brief *Snapshot* del estado de una partición hasta un punto del log. Afinidad: INMUTABLE.
/// @details Permite compactar el log descartando entradas ya aplicadas y cubiertas por el snapshot.
struct Snapshot {
    /// Último índice incluido en el snapshot.
    Index last_included_index = 0;
    /// Término de `last_included_index`.
    Term last_included_term = 0;
    /// Offset de partición del último record cubierto por el snapshot (base para reposicionar el
    /// `PartitionLog` del seguidor; ADR-0024).
    Offset last_included_offset = 0;
    /// Estado serializado de la máquina de estados.
    std::vector<std::byte> state;

    bool operator==(const Snapshot&) const = default;
};

/// @brief Estado persistente de Raft (sobrevive a reinicios). Afinidad: REACTOR-LOCAL.
/// @details En producción se persiste con `fsync` **antes** de responder a un RPC (regla de
///   seguridad de Raft). Mantiene dos invariantes del algoritmo: el término solo crece y, al
///   crecer, el voto del término se descarta (un voto por término).
/// @invariant `current_term()` es monótono no decreciente; `voted_for()` se resetea al avanzar.
/// @note La persistencia en disco (`persist`/`load`) se cablea en un incremento posterior; los
///   tests deterministas de Raft operan en memoria.
class RaftPersistentState {
public:
    [[nodiscard]] Term current_term() const noexcept { return current_term_; }
    [[nodiscard]] std::optional<NodeId> voted_for() const noexcept { return voted_for_; }

    /// @brief Avanza a un término estrictamente mayor y descarta el voto del término anterior.
    /// @pre `new_term > current_term()`.
    void advance_term(Term new_term) noexcept {
        current_term_ = new_term;
        voted_for_.reset();
    }

    /// @brief Registra el voto del término actual a @p candidate (a lo sumo uno por término).
    void record_vote(NodeId candidate) noexcept { voted_for_ = candidate; }

    /// @brief Reconstruye el estado a partir de valores **persistidos** en disco.
    /// @details No aplica la regla de monotonía de `advance_term`: al recuperar, el disco es la
    ///   fuente de verdad (el estado ya era válido cuando se persistió). Lo usa
    ///   `RaftStateStore::load`.
    [[nodiscard]] static RaftPersistentState restore(Term term,
                                                     std::optional<NodeId> voted_for) noexcept {
        RaftPersistentState state;
        state.current_term_ = term;
        state.voted_for_ = voted_for;
        return state;
    }

    bool operator==(const RaftPersistentState&) const = default;

private:
    /// Mayor término observado.
    Term current_term_ = 0;
    /// A quién se votó en `current_term_` (si a alguien).
    std::optional<NodeId> voted_for_;
};

/// @brief Estado volátil de Raft (se reconstruye al arrancar). Afinidad: REACTOR-LOCAL.
/// @details Común a todos los roles: `commit_index` (mayor índice replicado en mayoría) y
///   `last_applied` (mayor índice aplicado a la máquina de estados). En el líder, además, el
///   progreso por seguidor: `next_index` (próxima entrada a enviar) y `match_index` (mayor índice
///   replicado y confirmado por ese seguidor).
/// @invariant `last_applied <= commit_index`; `match_index[p] < next_index[p]` salvo al
/// inicializar.
class RaftVolatileState {
public:
    [[nodiscard]] Index commit_index() const noexcept { return commit_index_; }
    [[nodiscard]] Index last_applied() const noexcept { return last_applied_; }

    void set_commit_index(Index value) noexcept { commit_index_ = value; }
    void set_last_applied(Index value) noexcept { last_applied_ = value; }

    /// @brief Inicializa el progreso de réplica al convertirse en líder (§líder de Raft).
    /// @details Para cada peer: `next_index = last_log_index + 1`, `match_index = 0`.
    void reset_leader_progress(std::span<const NodeId> peers, Index last_log_index);

    /// @brief Descarta el progreso de réplica al dejar de ser líder.
    void clear_leader_progress() noexcept;

    [[nodiscard]] Index next_index(NodeId peer) const;
    [[nodiscard]] Index match_index(NodeId peer) const;
    void set_next_index(NodeId peer, Index value);
    void set_match_index(NodeId peer, Index value);

private:
    /// Mayor índice confirmado por mayoría (0 = nada confirmado).
    Index commit_index_ = 0;
    /// Mayor índice aplicado a la máquina de estados.
    Index last_applied_ = 0;
    /// Líder: próxima entrada a enviar por peer.
    std::unordered_map<NodeId, Index> next_index_;
    /// Líder: índice replicado confirmado por peer.
    std::unordered_map<NodeId, Index> match_index_;
};

}  // namespace nexus
