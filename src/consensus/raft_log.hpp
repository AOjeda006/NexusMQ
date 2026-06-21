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
    ///   el sidecar. `index > last_index()` es no-op; `index < 1` o `index <= snapshot_index()`
    ///   (cola ya compactada) es `InvalidArgument`.
    [[nodiscard]] expected<void> truncate_from(Index index);

    /// @brief Compacta el log descartando el prefijo aplicado hasta @p up_to_index (snapshot,
    ///   ADR-0024).
    /// @details Fija la **base de snapshot** `(up_to_index, term_at(up_to_index),
    ///   last_offset(up_to_index))`, descarta las entradas con índice ≤ @p up_to_index del sidecar
    ///   (exacto en el índice), recorta el prefijo **físico** del `PartitionLog`
    ///   (`truncate_prefix_to`, best-effort por segmentos) y persiste la base en el sidecar de
    ///   snapshot con `fsync`. Tras compactar, `term_at(up_to_index)` sigue devolviendo su término;
    ///   `term_at(index < up_to_index)` es `OutOfRange` (compactado).
    /// @pre @p up_to_index ≤ `commit_index` del consenso (responsabilidad del llamante: solo se
    ///   compacta lo ya replicado en mayoría y aplicado).
    /// @return No-op si @p up_to_index ≤ `snapshot_index()`; `InvalidArgument` si supera
    ///   `last_index()`; error de E/S si falla el recorte o la persistencia.
    [[nodiscard]] expected<void> compact_to(Index up_to_index);

    /// @brief Adopta un snapshot recibido del líder (seguidor, ADR-0024): fija la base
    ///   `(index, term, last_offset)` del log.
    /// @details Lo invoca el portador al aplicar un `InstallSnapshot`. No-op si @p index ≤
    ///   `snapshot_index()` (snapshot obsoleto). Si el log ya contiene la entrada `(index, term)`,
    ///   compacta hasta ella conservando la cola consistente (optimización del §7 del paper); si
    ///   no,
    ///   **descarta todo el log** y reabre el `PartitionLog` vacío en `last_offset + 1` (la cola
    ///   posterior llegará por `AppendEntries`). Persiste la base con `fsync`.
    [[nodiscard]] expected<void> install_snapshot(Index index, Term term, Offset last_offset);

    /// @brief Término de la entrada @p index. `index == 0` → `0` (centinela); fuera de rango alto →
    ///   `OutOfRange`.
    [[nodiscard]] expected<Term> term_at(Index index) const;

    /// Índice de la última entrada (`0` si vacío); incluye el prefijo cubierto por el snapshot.
    [[nodiscard]] Index last_index() const noexcept {
        return snapshot_index_ + static_cast<Index>(entries_.size());
    }

    /// Término de la última entrada (`snapshot_term` si solo queda el snapshot; `0` si vacío).
    [[nodiscard]] Term last_term() const noexcept {
        if (!entries_.empty()) {
            return entries_.back().term;
        }
        return snapshot_index_ > 0 ? snapshot_term_ : Term{0};
    }

    /// Último índice cubierto por el snapshot (`0` si no se ha compactado).
    [[nodiscard]] Index snapshot_index() const noexcept { return snapshot_index_; }
    /// Término del último índice cubierto por el snapshot.
    [[nodiscard]] Term snapshot_term() const noexcept { return snapshot_term_; }

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
    /// Tamaño del registro del sidecar de snapshot: `crc:u32 | index:i64 | term:i64 | offset:i64`.
    static constexpr std::size_t kSnapshotRecordSize = 28;

    RaftLog(PartitionLog& log, File meta, File snapshot, std::vector<EntryMeta> entries,
            Index snapshot_index, Term snapshot_term, Offset snapshot_last_offset);

    /// Escribe el metadato @p meta al final del sidecar (en la posición de la próxima entrada).
    [[nodiscard]] expected<void> persist_meta(const EntryMeta& meta);
    /// Persiste la base de snapshot actual (registro fijo + CRC32C) con `fsync`.
    [[nodiscard]] expected<void> persist_snapshot();
    /// Reescribe el sidecar de metadatos con exactamente las entradas vivas (tras compactar).
    [[nodiscard]] expected<void> rewrite_meta();
    /// Lee el `RecordBatch` (bytes) de la entrada @p entry desde el `PartitionLog`.
    [[nodiscard]] expected<std::vector<std::byte>> read_payload(const EntryMeta& entry) const;
    /// Posición en `entries_` de la entrada de índice @p index (relativa a la base de snapshot).
    [[nodiscard]] std::size_t slot_of(Index index) const noexcept {
        return static_cast<std::size_t>(index - snapshot_index_ - 1);
    }

    // Vista no propietaria del log subyacente (RaftLog no es asignable, a propósito).
    PartitionLog& log_;               // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    File meta_file_;                  ///< Sidecar de metadatos por entrada viva (RAII).
    File snapshot_file_;              ///< Sidecar de la base de snapshot (RAII).
    std::vector<EntryMeta> entries_;  ///< Metadatos de las entradas **vivas** (índice > snapshot).
    Index snapshot_index_ = 0;  ///< Último índice cubierto por el snapshot (0 = sin snapshot).
    Term snapshot_term_ = 0;    ///< Término de `snapshot_index_`.
    Offset snapshot_last_offset_ = 0;  ///< Offset de partición del último record del snapshot.
};

}  // namespace nexus
