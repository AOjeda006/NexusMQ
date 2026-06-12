/// @file   storage/partition_log.hpp
/// @brief  PartitionLog: secuencia de segmentos de una partición (rolling + recuperación).
/// @ingroup storage

#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>
#include <vector>

#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "storage/fetch_result.hpp"
#include "storage/log_config.hpp"
#include "storage/segment.hpp"

namespace nexus {

/// @brief El log append-only de una partición: una secuencia ordenada de segmentos.
/// @details Afinidad: REACTOR-LOCAL (no es thread-safe). Mantiene los segmentos ordenados
///   por offset base; el último es el **activo** (recibe `append`). Al superar el activo
///   `segment_bytes` se **rota** (se sella y se abre uno nuevo). El log es la autoridad de
///   los offsets: `append` asigna `base_offset = log_end_offset()` al batch (revisable en la
///   capa Partition/Raft, donde el líder asigna antes de replicar).
///
///   Al abrir, descubre los `.log` del directorio, abre todos los segmentos y **recupera**
///   el activo (valida CRC + trunca cola *torn*; §7.11 #2), fijando `log_end_offset()`.
/// @invariant log_start_offset() ≤ log_end_offset(); los segmentos no se solapan.
class PartitionLog {
public:
    /// @brief Abre (o crea) el log de la partición en @p dir según @p cfg.
    /// @details Crea el directorio si no existe; si está vacío, crea el primer segmento en 0.
    [[nodiscard]] static expected<PartitionLog> open(std::filesystem::path dir, LogConfig cfg);

    /// @brief Añade @p batch al final del log, rotando el segmento activo si está lleno.
    /// @details Asigna al batch el offset base autoritativo (`log_end_offset()`); el CRC no
    ///   cubre `base_offset`, así que reasignarlo no invalida la integridad.
    /// @return El último offset asignado al batch.
    [[nodiscard]] expected<Offset> append(const RecordBatch& batch);

    /// @brief Lee batches desde @p offset hasta ~@p max_bytes, **cruzando segmentos** (§7.11 #3).
    /// @details Localiza el segmento por offset (búsqueda binaria) y delega en `Segment::read`;
    ///   si no se llena @p max_bytes y quedan datos, continúa en el segmento siguiente. Lee
    ///   hasta `log_end_offset()` (todo lo escrito en este motor monohilo).
    /// @return `FetchResult` (vacío si @p offset alcanza el final), `OutOfRange` si @p offset
    ///   es anterior a `log_start_offset()`, o error de E/S.
    [[nodiscard]] expected<FetchResult> read(Offset offset, std::size_t max_bytes) const;

    /// Primer offset disponible en el log (base del primer segmento).
    [[nodiscard]] Offset log_start_offset() const noexcept { return log_start_offset_; }
    /// Offset que se asignará al próximo record (uno más que el último escrito).
    [[nodiscard]] Offset log_end_offset() const noexcept { return log_end_offset_; }
    /// Número de segmentos (observabilidad / pruebas).
    [[nodiscard]] std::size_t segment_count() const noexcept { return segments_.size(); }

private:
    PartitionLog(std::filesystem::path dir, LogConfig cfg,
                 std::vector<std::unique_ptr<Segment>> segments, Offset log_start, Offset log_end);

    [[nodiscard]] Segment* active() noexcept { return segments_.back().get(); }
    [[nodiscard]] expected<void> roll_segment();
    /// Segmento cuyo rango contiene @p offset (mayor base ≤ offset); nullptr si es menor que todos.
    [[nodiscard]] const Segment* segment_for(Offset offset) const noexcept;

    std::filesystem::path dir_;                       ///< Directorio de la partición.
    LogConfig cfg_;                                   ///< Configuración (por valor).
    std::vector<std::unique_ptr<Segment>> segments_;  ///< Segmentos por offset base.
    Offset log_start_offset_;                         ///< Primer offset del log.
    Offset log_end_offset_;                           ///< Próximo offset a asignar.
};

}  // namespace nexus
