/// @file   storage/segment.hpp
/// @brief  Segment: un tramo del log append-only (.log + .index).
/// @ingroup storage

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>

#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "io/file.hpp"
#include "storage/index.hpp"

namespace nexus {

/// @brief Un tramo contiguo del log de una partición: fichero `.log` + su `.index`.
/// @details Afinidad: REACTOR-LOCAL (no es thread-safe). El `.log` es **append-only**:
///   los batches se escriben en orden por su offset y nunca se modifican. El `.index`
///   (SparseIndex) ancla ~cada N bytes un offset a su posición, para *seek* sin recorrer
///   todo el segmento. Un segmento Active recibe `append`; al `seal` pasa a Closed (se
///   persiste el índice y se fuerza durabilidad), y deja de admitir escrituras.
///
///   Los ficheros se nombran con el offset base a 20 dígitos (orden lexicográfico =
///   orden de offset): `00000000000000000000.log` / `.index`.
/// @invariant En estado Active, `size_bytes()` == bytes escritos en el `.log`.
class Segment {
public:
    /// Estado del segmento en su ciclo de vida (§5.9).
    enum class State : std::uint8_t {
        Active,  ///< Admite `append`; es el segmento al final del log.
        Closed,  ///< Sellado (índice persistido); solo lectura.
    };

    /// @brief Crea un segmento nuevo (ficheros `.log`/`.index` vacíos) en @p dir.
    /// @param base_offset Offset del primer record que albergará (ancla del índice).
    /// @param index_interval_bytes Densidad del índice disperso (bytes entre anclas).
    [[nodiscard]] static expected<Segment> create(const std::filesystem::path& dir,
                                                  Offset base_offset,
                                                  std::size_t index_interval_bytes);

    /// @brief Abre un segmento existente (no valida CRC: eso es `recover`).
    [[nodiscard]] static expected<Segment> open(const std::filesystem::path& dir,
                                                Offset base_offset,
                                                std::size_t index_interval_bytes);

    /// @brief Añade @p batch al final del `.log` e indexa su posición.
    /// @details No fuerza durabilidad por batch (la política de `fsync` es de M5); escribe
    ///   a la caché de página. El offset base del batch debe encajar al final del log.
    /// @return El último offset del batch escrito, o error de E/S / segmento sellado.
    [[nodiscard]] expected<Offset> append(const RecordBatch& batch);

    /// @brief Sella el segmento: persiste el índice, fuerza `fsync` del log y pasa a Closed.
    [[nodiscard]] expected<void> seal();

    /// @brief ¿Ha alcanzado el segmento su tamaño máximo? (criterio de rotación, M4).
    [[nodiscard]] bool is_full(std::size_t segment_bytes) const noexcept {
        return size_bytes_ >= segment_bytes;
    }

    [[nodiscard]] Offset base_offset() const noexcept { return base_offset_; }
    [[nodiscard]] std::size_t size_bytes() const noexcept { return size_bytes_; }
    [[nodiscard]] State state() const noexcept { return state_; }

private:
    Segment(Offset base_offset, File log, SparseIndex index, std::size_t size_bytes);

    Offset base_offset_;      ///< Offset base del segmento.
    File log_;                ///< Fichero `.log` (RAII).
    SparseIndex index_;       ///< Índice disperso `.index` (RAII).
    std::size_t size_bytes_;  ///< Bytes escritos en el `.log`.
    State state_ = State::Active;
};

}  // namespace nexus
