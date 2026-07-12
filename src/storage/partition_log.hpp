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
#include "storage/retention.hpp"
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
    [[nodiscard]] static expected<PartitionLog> open(std::filesystem::path dir,
                                                     const LogConfig& cfg);

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

    /// @brief Trunca el log eliminando todo lo de offset >= @p offset (resolución de conflictos
    ///   de Raft, §7.11 #5: un seguidor descarta su cola divergente antes de aceptar la del líder).
    /// @details @p offset debe ser una **frontera de batch** (o `log_end_offset()`, que es no-op).
    ///   Borra los segmentos posteriores (ficheros incluidos), trunca el segmento que lo contiene
    ///   (que pasa a ser el activo) y retrocede `log_end_offset`/`recovery_point`. `OutOfRange` si
    ///   @p offset cae fuera de `[log_start_offset, log_end_offset]`; `InvalidArgument` si no es
    ///   frontera de batch.
    [[nodiscard]] expected<void> truncate_to(Offset offset);

    /// @brief Recorta el **prefijo** del log borrando los segmentos sellados enteros cuyo rango
    ///   queda por debajo de @p offset (compactación de Raft por snapshot, ADR-0024).
    /// @details Pieza simétrica de `enforce_retention`, pero recortando a un **offset preciso** en
    ///   vez de por política de tamaño/tiempo. Borra segmentos **completos** (nunca a media trama,
    ///   nunca el activo): tras la llamada `log_start_offset()` avanza hasta la base del primer
    ///   segmento conservado, que puede quedar **por debajo** de @p offset si ese offset cae dentro
    ///   de un segmento (la reclamación física es por segmentos enteros; la lógica del `RaftLog` es
    ///   exacta en el índice). Idempotente si @p offset ≤ `log_start_offset()`; `OutOfRange` si
    ///   @p offset > `log_end_offset()`.
    [[nodiscard]] expected<void> truncate_prefix_to(Offset offset);

    /// @brief Reinicia el log a **vacío** en la base @p offset (instalación de snapshot, ADR-0024).
    /// @details Borra **todos** los segmentos (ficheros incluidos) y crea uno nuevo, vacío, con
    ///   `base_offset == offset`; deja `log_start_offset() == log_end_offset() == offset`. Lo usa
    ///   el
    ///   **seguidor** al adoptar un `InstallSnapshot`: descarta su log divergente/obsoleto y reabre
    ///   el espacio de offsets en la base del snapshot del líder, listo para recibir la cola por
    ///   `AppendEntries`. `InvalidArgument` si @p offset es negativo.
    [[nodiscard]] expected<void> reset_to(Offset offset);

    /// @brief Fuerza la durabilidad del segmento activo (`fsync`) y avanza `recovery_point`.
    [[nodiscard]] expected<void> sync();

    /// @brief Aplica la retención: borra los segmentos sellados más antiguos elegibles por
    ///   tamaño o tiempo, **nunca el activo**; avanza `log_start_offset`.
    [[nodiscard]] expected<void> enforce_retention(const RetentionPolicy& policy);

    /// Primer offset disponible en el log (base del primer segmento).
    [[nodiscard]] Offset log_start_offset() const noexcept { return log_start_offset_; }
    /// Offset que se asignará al próximo record (uno más que el último escrito).
    [[nodiscard]] Offset log_end_offset() const noexcept { return log_end_offset_; }
    /// Offset hasta el que los datos están sincronizados a disco estable (garantía de durabilidad).
    [[nodiscard]] Offset recovery_point() const noexcept { return recovery_point_; }
    /// Número de segmentos (observabilidad / pruebas).
    [[nodiscard]] std::size_t segment_count() const noexcept { return segments_.size(); }
    /// Directorio de la partición (p. ej. para ubicar sidecars como el `raft-meta` del `RaftLog`).
    [[nodiscard]] const std::filesystem::path& dir() const noexcept { return dir_; }

private:
    PartitionLog(std::filesystem::path dir, LogConfig cfg,
                 std::vector<std::unique_ptr<Segment>> segments, Offset log_start, Offset log_end);

    [[nodiscard]] Segment* active() noexcept { return segments_.back().get(); }
    [[nodiscard]] expected<void> roll_segment();
    /// Aplica la política de `fsync` tras escribir @p appended_bytes (puede sincronizar).
    [[nodiscard]] expected<void> maybe_sync(std::size_t appended_bytes);
    /// Segmento cuyo rango contiene @p offset (mayor base ≤ offset); nullptr si es menor que todos.
    [[nodiscard]] const Segment* segment_for(Offset offset) const noexcept;

    std::filesystem::path dir_;                       ///< Directorio de la partición.
    LogConfig cfg_;                                   ///< Configuración (por valor).
    std::vector<std::unique_ptr<Segment>> segments_;  ///< Segmentos por offset base.
    Offset log_start_offset_;                         ///< Primer offset del log.
    Offset log_end_offset_;                           ///< Próximo offset a asignar.
    Offset recovery_point_;                           ///< Hasta dónde está sincronizado a disco.
    std::size_t bytes_since_sync_ = 0;                ///< Bytes escritos desde el último `fsync`.
};

}  // namespace nexus
