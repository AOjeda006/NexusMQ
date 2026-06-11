/// @file   storage/index.hpp
/// @brief  SparseIndex: índice disperso offset→posición de un segmento (.index).
/// @ingroup storage

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/error.hpp"
#include "common/types.hpp"
#include "io/file.hpp"

namespace nexus {

/// @brief Entrada del índice disperso: ancla un offset relativo a una posición del .log.
/// @details Afinidad: INMUTABLE. 'relative_offset' = offset absoluto − base del segmento
///   (cabe en u32 porque un segmento está acotado por 'segment_bytes'); 'file_position' es el
///   byte donde empieza ese batch en el fichero .log. Ambos crecen de forma estrictamente
///   monótona a lo largo del índice.
struct IndexEntry {
    std::uint32_t relative_offset = 0;  ///< Offset relativo a la base del segmento.
    std::uint32_t file_position = 0;    ///< Byte de inicio del batch en el .log.
};

/// @brief Índice **disperso** de un segmento: ancla ~cada N bytes un offset a su posición.
/// @details Afinidad: REACTOR-LOCAL (no es thread-safe). No indexa cada batch, sino uno
///   cada 'index_interval_bytes' aprox.: un compromiso espacio/tiempo (índice pequeño,
///   búsqueda binaria + barrido corto del .log). 'floor(offset)' da la entrada ancla **≤**
///   offset; el segmento barre el .log hacia delante desde ahí hasta el batch exacto. Posee
///   el fichero .index (RAII) y mantiene las entradas en memoria (espejo del disco).
///
///   Formato en disco: secuencia de entradas, cada una `relative_offset:u32 | file_position:u32`
///   en little-endian (8 bytes), ordenadas de forma **estrictamente creciente** por offset.
/// @invariant Las entradas están estrictamente ordenadas por 'relative_offset'.
class SparseIndex {
public:
    /// Tamaño en disco de una entrada: dos u32 little-endian.
    static constexpr std::size_t kEntrySize = 8;

    /// @brief Abre (o crea) el .index en @p path y carga las entradas existentes.
    /// @param base_offset Offset base del segmento (ancla de los offsets relativos).
    /// @param index_interval_bytes Bytes de log entre anclas (densidad del índice; 0 = denso).
    /// @return El índice, 'IoError' si falla la E/S, o 'Corrupt' si el fichero está dañado.
    [[nodiscard]] static expected<SparseIndex> open(const std::string& path, Offset base_offset,
                                                    std::size_t index_interval_bytes);

    /// @brief Tras escribir un batch, siembra una entrada si toca por intervalo.
    /// @param offset Offset base del batch recién escrito.
    /// @param file_position Byte donde empieza el batch en el .log.
    /// @param batch_len Tamaño en bytes del batch (alimenta el contador de intervalo).
    /// @pre @p offset ≥ base_offset() y crece de forma monótona entre llamadas.
    void maybe_append(Offset offset, std::uint32_t file_position, std::size_t batch_len);

    /// @brief Mayor entrada con 'relative_offset' ≤ (@p target − base); {0,0} si ninguna.
    /// @details {0,0} significa «barre desde el inicio del segmento». No falla.
    [[nodiscard]] IndexEntry floor(Offset target) const noexcept;

    /// @brief Persiste al .index las entradas aún no escritas y hace fsync (sella).
    [[nodiscard]] expected<void> flush();

    [[nodiscard]] Offset base_offset() const noexcept { return base_offset_; }
    [[nodiscard]] std::size_t size() const noexcept { return entries_.size(); }
    [[nodiscard]] bool empty() const noexcept { return entries_.empty(); }

private:
    SparseIndex(File file, Offset base_offset, std::size_t index_interval_bytes,
                std::vector<IndexEntry> entries);

    File file_;                        ///< Fichero .index (RAII).
    Offset base_offset_;               ///< Base del segmento.
    std::size_t interval_;             ///< Bytes de log entre anclas.
    std::size_t bytes_since_ = 0;      ///< Bytes de log desde la última ancla.
    std::vector<IndexEntry> entries_;  ///< Anclas en memoria (espejo del .index).
    std::size_t flushed_ = 0;          ///< Cuántas entradas ya están en disco.
};

}  // namespace nexus
