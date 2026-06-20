/// @file   consensus/raft_state_store.hpp
/// @brief  RaftStateStore: persistencia durable del estado persistente de Raft (término y voto).
/// @ingroup consensus

#pragma once

#include <cstddef>
#include <string>

#include "common/error.hpp"
#include "consensus/raft_state.hpp"
#include "io/file.hpp"

namespace nexus {

/// @brief Almacén durable del estado persistente de Raft (`current_term`/`voted_for`).
/// @details Afinidad: REACTOR-LOCAL (posee un `File`; no es thread-safe). Materializa la regla de
///   seguridad de Raft (§5): el término y el voto **deben** sobrevivir a un reinicio. Si se
///   perdieran, un nodo podría votar dos veces en el mismo término tras recuperarse y romper la
///   garantía de **un solo líder por término**. Por eso `save` fuerza `fsync` antes de devolver: el
///   portador de la FSM (ADR-0015) persiste **antes** de transportar las respuestas a los RPC.
///
///   Formato en disco (un registro fijo en el offset 0, todo little-endian):
///   `crc:u32 | term:i64 | has_vote:u8 | voted_for:i32`. El `crc` es CRC32C sobre los 13 bytes de
///   carga (term+has_vote+voted_for); detecta corrupción silenciosa (*bit rot*) y escrituras
///   parciales. El registro (17 B) cabe en un sector de disco, así que la reescritura es atómica a
///   nivel de sector (no hay *torn write* que partir el registro).
/// @invariant Un fichero vacío representa el estado inicial (`term = 0`, sin voto).
class RaftStateStore {
public:
    /// @brief Abre (o crea) el almacén en @p path.
    [[nodiscard]] static expected<RaftStateStore> open(const std::string& path);

    /// @brief Carga el estado persistido; un fichero vacío devuelve el estado inicial.
    /// @return `Corrupt` si el registro está truncado o su CRC32C no casa.
    [[nodiscard]] expected<RaftPersistentState> load() const;

    /// @brief Persiste @p state de forma durable (reescribe el registro y hace `fsync`).
    [[nodiscard]] expected<void> save(const RaftPersistentState& state) const;

private:
    explicit RaftStateStore(File file) : file_(std::move(file)) {}

    /// Tamaño del registro en disco: `crc:u32 | term:i64 | has_vote:u8 | voted_for:i32`.
    static constexpr std::size_t kRecordSize = 4 + 8 + 1 + 4;

    File file_;
};

}  // namespace nexus
