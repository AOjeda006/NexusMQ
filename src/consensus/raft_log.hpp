/// @file   consensus/raft_log.hpp
/// @brief  RaftLog: vista (term, index) sobre un PartitionLog (ADR-0014).
/// @ingroup consensus

#pragma once

#include <cstddef>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/error.hpp"
#include "common/types.hpp"
#include "consensus/raft_state.hpp"
#include "io/file.hpp"
#include "storage/partition_log.hpp"

namespace nexus {

/// @brief El log replicado de Raft como vista `(term, index)` sobre un `PartitionLog` (ADR-0014).
/// @details Afinidad: REACTOR-LOCAL (no es thread-safe). Una **entrada de Raft ↔ un
/// `RecordBatch`**;
///   el **índice de Raft es el ordinal de la entrada** (1-based, +1 por entrada), espacio distinto
///   del offset por record del `PartitionLog`. El `PartitionLog` almacena los bytes de cada entrada
///   y asigna sus offsets; este tipo posee el mapeo `índice → (term, base_offset, last_offset,
///   type)` y lo persiste en un **sidecar** de registros de tamaño fijo, de modo que el
///   `RecordBatch` queda **intacto** en disco (el *fetch* del consumidor lo lee sin desenvolver) y
///   el término se recupera sin tocar el formato del batch.
/// @invariant `last_index()` == nº de entradas; el `base_offset` de la entrada `i` casa con la
///   frontera de batch que el `PartitionLog` le asignó.
class RaftLog {
public:
    /// @brief Abre el RaftLog sobre @p log, cargando el sidecar de metadatos de @p meta_path.
    /// @details Crea el sidecar si no existe. Valida que el final del sidecar concuerde con
    ///   `log.log_end_offset()` (`Corrupt` si no). Un `PartitionLog` vacío da un RaftLog vacío.
    [[nodiscard]] static expected<RaftLog> open(PartitionLog& log, const std::string& meta_path);

    /// @brief Añade @p entries al final del log; devuelve el nuevo `last_index`.
    /// @details Cada `payload` es un `RecordBatch` serializado: se decodifica, se anexa al
    ///   `PartitionLog` (que le asigna sus offsets) y se registra su metadato en el sidecar.
    [[nodiscard]] expected<Index> append(std::span<const RaftLogEntry> entries);

    /// @brief Elimina las entradas con índice >= @p index (resolución de conflictos, §7.11 #5).
    /// @details Trunca el `PartitionLog` por la frontera de batch de la entrada @p index y recorta
    ///   el sidecar. `index > last_index()` es no-op; `index < 1` es `InvalidArgument`.
    [[nodiscard]] expected<void> truncate_from(Index index);

    /// @brief Término de la entrada @p index. `index == 0` → `0` (centinela); fuera de rango alto →
    ///   `OutOfRange`.
    [[nodiscard]] expected<Term> term_at(Index index) const;

    /// Número de entradas del log (índice de la última; `0` si vacío).
    [[nodiscard]] Index last_index() const noexcept { return static_cast<Index>(entries_.size()); }

    /// Término de la última entrada (`0` si el log está vacío).
    [[nodiscard]] Term last_term() const noexcept {
        return entries_.empty() ? Term{0} : entries_.back().term;
    }

    /// @brief Hasta @p max entradas a partir de @p index (para `AppendEntries`).
    [[nodiscard]] expected<std::vector<RaftLogEntry>> entries_from(Index index,
                                                                   std::size_t max) const;

    /// @brief Offsets de partición `[base, last]` de la entrada @p index (para el high-watermark).
    [[nodiscard]] expected<std::pair<Offset, Offset>> offsets_at(Index index) const;

private:
    /// Metadato por entrada (lo que vive en el sidecar; los bytes viven en el `PartitionLog`).
    struct EntryMeta {
        Term term = 0;
        Offset base_offset = 0;
        Offset last_offset = 0;
        RaftEntryType type = RaftEntryType::Data;
    };

    /// Tamaño en bytes de un registro del sidecar: `term:i64 | base:i64 | last:i64 | type:u8`.
    static constexpr std::size_t kMetaRecordSize = 25;

    RaftLog(PartitionLog& log, File meta, std::vector<EntryMeta> entries);

    /// Escribe el metadato @p meta al final del sidecar (en la posición de la próxima entrada).
    [[nodiscard]] expected<void> persist_meta(const EntryMeta& meta);
    /// Lee el `RecordBatch` (bytes) de la entrada @p entry desde el `PartitionLog`.
    [[nodiscard]] expected<std::vector<std::byte>> read_payload(const EntryMeta& entry) const;

    // Vista no propietaria del log subyacente (RaftLog no es asignable, a propósito).
    PartitionLog& log_;               // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    File meta_file_;                  ///< Sidecar de metadatos por entrada (RAII).
    std::vector<EntryMeta> entries_;  ///< Metadatos por índice (1-based: entrada `i` en `[i-1]`).
};

}  // namespace nexus
