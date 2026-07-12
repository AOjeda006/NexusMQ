/// @file   broker/partition_txn_index.hpp
/// @brief  PartitionTxnIndex: LSO y transacciones abortadas de una partición (read_committed).
/// @ingroup broker

#pragma once

#include <cstddef>
#include <unordered_map>
#include <vector>

#include "common/control_record.hpp"
#include "common/record.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Nivel de aislamiento de una lectura (§ exactly-once). Afinidad: INMUTABLE.
/// @details `ReadUncommitted` entrega todo lo anexado hasta el *high-watermark* (incluida la data
/// de
///   transacciones aún sin decidir); `ReadCommitted` entrega solo hasta el **LSO** y **filtra** los
///   records de transacciones abortadas y los marcadores de control.
enum class IsolationLevel : std::uint8_t {
    ReadUncommitted = 0,
    ReadCommitted = 1,
};

/// @brief Transacción abortada relevante para un consumidor: su productor y su primer offset.
/// @details Espeja el `AbortedTransaction` de Kafka: el consumidor `read_committed` descarta los
///   batches transaccionales de @p producer_id desde @p first_offset hasta su marcador de abort.
struct AbortedTxn {
    ProducerId producer_id = -1;  ///< Productor de la transacción abortada.
    Offset first_offset = 0;      ///< Primer offset de datos de la transacción abortada.
    bool operator==(const AbortedTxn&) const = default;
};

/// @brief Índice transaccional de una partición: mantiene el **LSO** y las transacciones abortadas.
///   Afinidad: REACTOR-LOCAL (acompaña al `PartitionLog` de la partición).
/// @details Se alimenta del propio log (al anexar y al reconstruir): `on_data` abre la transacción
/// de
///   un productor en su primer batch transaccional; `on_marker` la cierra (un ABORT registra el
///   rango abortado). Con eso:
///   - **LSO** = `min(high_watermark, primer offset de la transacción abierta más antigua)`: la
///     frontera hasta la que **todo** está decidido (committed o aborted). Un `read_committed` no
///     entrega más allá del LSO (los datos posteriores podrían pertenecer a una transacción que aún
///     no ha confirmado).
///   - **Transacciones abortadas**: para que el consumidor **filtre** su data (los offsets se
///     conservan en el log; la visibilidad es del lector).
/// @invariant El LSO nunca supera el `high_watermark`; solo retrocede respecto a él mientras haya
///   una transacción abierta cuyos datos empiezan antes del `high_watermark`.
class PartitionTxnIndex {
public:
    /// @brief Registra el primer batch de datos transaccionales de @p producer_id en @p
    /// base_offset.
    /// @details Abre la transacción del productor en esta partición. Idempotente: solo el
    /// **primer**
    ///   batch (el de menor offset) fija el inicio; los siguientes de la misma transacción no lo
    ///   mueven.
    void on_data(ProducerId producer_id, Offset base_offset);

    /// @brief Cierra la transacción de @p producer_id al escribirse su marcador en @p
    /// marker_offset.
    /// @details `Commit` la hace visible (nada que filtrar); `Abort` registra el rango
    ///   `[first_offset, marker_offset)` como abortado para el filtrado del consumidor. Sin
    ///   transacción abierta para @p producer_id, es un no-op (idempotente ante reprocesado).
    void on_marker(ProducerId producer_id, ControlRecordType decision, Offset marker_offset);

    /// @brief LSO dado el @p high_watermark actual de la partición.
    [[nodiscard]] Offset last_stable_offset(Offset high_watermark) const;

    /// @brief Transacciones abortadas cuyo marcador cae en/tras @p fetch_offset (las que podrían
    ///   solapar un fetch que empieza ahí), ordenadas por `first_offset`.
    [[nodiscard]] std::vector<AbortedTxn> aborted_transactions(Offset fetch_offset) const;

    /// @brief Descarta el estado de transacciones abortadas cuyo marcador quedó por debajo de
    ///   @p log_start (sus datos ya no están en el log: retención/truncado de prefijo).
    void evict_below(Offset log_start);

    [[nodiscard]] bool has_open() const noexcept { return !open_.empty(); }
    [[nodiscard]] std::size_t open_count() const noexcept { return open_.size(); }
    [[nodiscard]] std::size_t aborted_count() const noexcept { return aborted_.size(); }

private:
    /// Transacción abortada con su rango completo (el marcador delimita el fin para el filtrado).
    struct AbortedRange {
        ProducerId producer_id = -1;
        Offset first_offset = 0;
        Offset marker_offset = 0;
    };

    /// `producer_id` → primer offset de su transacción abierta en esta partición.
    std::unordered_map<ProducerId, Offset> open_;
    /// Transacciones abortadas, en orden de aparición (marcador creciente).
    std::vector<AbortedRange> aborted_;
};

/// @brief Filtra @p batches para una lectura `read_committed` dadas las transacciones @p aborted.
/// @details @p batches deben venir en **orden de offset**; @p aborted es el resultado de
///   `aborted_transactions` para el offset inicial del fetch. Excluye (a) los batches de
///   **control** (los marcadores nunca se entregan al consumidor) y (b) los batches
///   **transaccionales** de un productor mientras esté dentro de una de sus transacciones abortadas
///   (entre su `first_offset` y su marcador). Los batches no transaccionales pasan siempre.
[[nodiscard]] std::vector<RecordBatch> filter_committed(const std::vector<RecordBatch>& batches,
                                                        const std::vector<AbortedTxn>& aborted);

}  // namespace nexus
