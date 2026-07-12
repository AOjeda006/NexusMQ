/// @file   broker/transaction_coordinator.hpp
/// @brief  TransactionCoordinator: FSM sin E/S del 2PC de transacciones multi-partición (ADR-0033).
/// @ingroup broker

#pragma once

#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/control_record.hpp"
#include "common/error.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Referencia a una partición concreta (topic + índice). Afinidad: INMUTABLE.
/// @details Orden total por (topic, partición) para mantener el conjunto de participantes ordenado
/// y
///   sin duplicados de forma determinista.
struct TopicPartition {
    std::string topic;          ///< Nombre del topic.
    PartitionId partition = 0;  ///< Índice de partición dentro del topic.
    friend auto operator<=>(const TopicPartition&, const TopicPartition&) = default;
};

/// @brief Estado de una transacción en el coordinador (§ exactly-once). Afinidad: INMUTABLE (dato).
/// @details Ciclo: `Ongoing` (abierta, acumulando particiones) → `PrepareCommit`/`PrepareAbort`
///   (**decisión ya registrada** en el log del coordinador; escribiendo marcadores) →
///   `CompleteCommit`/`CompleteAbort` (todos los marcadores escritos; transacción concluida). El
///   registro de la decisión **antes** de escribir marcadores es lo que hace el 2PC
///   **recuperable**: un coordinador que arranca y ve `Prepare*` sabe que debe **re-emitir** los
///   marcadores que falten (no es el 2PC en memoria bloqueante que la biblioteca prohíbe).
enum class TransactionState : std::uint8_t {
    Ongoing,         ///< Abierta; se le añaden particiones y se le anexan batches transaccionales.
    PrepareCommit,   ///< COMMIT decidido y registrado; escribiendo marcadores de commit.
    PrepareAbort,    ///< ABORT decidido y registrado; escribiendo marcadores de abort.
    CompleteCommit,  ///< Todos los marcadores de commit escritos.
    CompleteAbort,   ///< Todos los marcadores de abort escritos.
};

/// @brief Metadatos de una transacción de un productor. Afinidad: INMUTABLE (snapshot de estado).
struct TransactionMetadata {
    ProducerId producer_id = -1;                         ///< Productor dueño de la transacción.
    std::int16_t producer_epoch = -1;                    ///< Época del productor (fencing).
    TransactionState state = TransactionState::Ongoing;  ///< Estado de la FSM.
    std::vector<TopicPartition> partitions;  ///< Participantes (ordenados, sin duplicados).
    std::vector<TopicPartition>
        unacked_partitions;       ///< Participantes cuyo marcador falta por acusar.
    Epoch coordinator_epoch = 0;  ///< Época del coordinador que decidió.
    MonoTime last_update;         ///< Última mutación (para el timeout).
};

/// @brief Orden de escribir un marcador COMMIT/ABORT en una partición participante. INMUTABLE.
/// @details Salida proactiva del coordinador: el portador la transporta (escribe el marcador en la
///   partición) y luego llama a `on_marker_written`. El `coordinator_epoch` sella el marcador para
///   que la partición **descarte** los de un coordinador obsoleto (fencing en el failover).
struct MarkerWrite {
    ProducerId producer_id = -1;
    std::int16_t producer_epoch = -1;
    Epoch coordinator_epoch = 0;
    ControlRecordType decision = ControlRecordType::Abort;
    TopicPartition partition;
};

/// @brief Coordinador de transacciones: FSM síncrona **sin E/S** del 2PC multi-partición
/// (ADR-0033).
/// @details Afinidad: REACTOR-LOCAL. Al estilo de `RaftNode`/`GroupCoordinator` (ADR-0015): consume
///   *entradas* (`begin`/`add_partitions`/`commit`/`abort`/`on_marker_written`/`tick`) con el
///   instante `now` inyectado y produce *salidas* (órdenes de marcador en una cola que el portador
///   drena con `take_pending_markers`). No tiene reloj, red ni corrutinas propias. Su estado es
///   **replicable** por su propio grupo Raft (el «Raft propio» del coordinador): las transiciones
///   son deterministas y reproducibles, de modo que reproducir la secuencia reconstruye el estado.
/// @invariant Una decisión (`Prepare*`) precede siempre a la escritura de marcadores; una
///   transacción concluye (`Complete*`) solo cuando no quedan participantes por acusar
///   (`unacked_partitions` vacío).
class TransactionCoordinator {
public:
    /// Timeout por defecto de una transacción abierta antes del abort del servidor (§).
    static constexpr std::chrono::milliseconds kDefaultTxnTimeout{60'000};

    /// @param coordinator_epoch Época de liderazgo inicial (sella los marcadores que emite).
    /// @param txn_timeout Tiempo máximo que una transacción puede estar `Ongoing` sin avanzar.
    explicit TransactionCoordinator(Epoch coordinator_epoch = 0,
                                    std::chrono::milliseconds txn_timeout = kDefaultTxnTimeout);

    /// @brief Adopta una nueva época de liderazgo (failover): los marcadores que emita a partir de
    ///   ahora la llevan, de modo que las particiones fencing a los del coordinador anterior.
    void set_coordinator_epoch(Epoch epoch) noexcept { coordinator_epoch_ = epoch; }
    [[nodiscard]] Epoch coordinator_epoch() const noexcept { return coordinator_epoch_; }

    /// @brief Abre (o reabre) la transacción del productor @p producer_id con época @p epoch.
    /// @details Una época **inferior** a la registrada es `Fenced`; una **superior** (o un estado
    ///   `Complete*`, o inexistente) inicia una transacción nueva `Ongoing` fenciando a la
    ///   anterior. Reabrir sobre una transacción `Ongoing`/`Prepare*` con la **misma** época es
    ///   `InvalidArgument` (ya hay una en curso).
    [[nodiscard]] expected<void> begin(MonoTime now, ProducerId producer_id, std::int16_t epoch);

    /// @brief Registra @p partitions como participantes de la transacción `Ongoing`.
    /// @details `NotFound` si no hay transacción; `Fenced` si @p epoch es inferior;
    /// `InvalidArgument`
    ///   si @p epoch no coincide o el estado no es `Ongoing`. Idempotente: fusiona sin duplicar.
    [[nodiscard]] expected<void> add_partitions(MonoTime now, ProducerId producer_id,
                                                std::int16_t epoch,
                                                const std::vector<TopicPartition>& partitions);

    /// @brief Decide **COMMIT**: `Ongoing → PrepareCommit` y encola un marcador por participante.
    /// @details Sin participantes, concluye de inmediato (`CompleteCommit`). Errores como
    ///   `add_partitions`.
    [[nodiscard]] expected<void> commit(MonoTime now, ProducerId producer_id, std::int16_t epoch);

    /// @brief Decide **ABORT**: `Ongoing → PrepareAbort` y encola un marcador por participante.
    [[nodiscard]] expected<void> abort(MonoTime now, ProducerId producer_id, std::int16_t epoch);

    /// @brief Acusa la escritura del marcador de @p partition para @p producer_id / @p epoch.
    /// @details Ignora acks obsoletos (época distinta o estado no `Prepare*`). Cuando no quedan
    ///   marcadores, la transacción pasa a `Complete*`.
    void on_marker_written(ProducerId producer_id, std::int16_t epoch,
                           const TopicPartition& partition);

    /// @brief Aborta por timeout las transacciones `Ongoing` que llevan demasiado sin avanzar.
    void tick(MonoTime now);

    /// @brief Re-emite los marcadores pendientes de todas las transacciones en `Prepare*`.
    /// @details Se llama tras un failover (el nuevo coordinador reanuda el 2PC de las transacciones
    ///   que encontró a medias en su log). Los marcadores re-emitidos llevan la época actual.
    void resume_pending();

    /// @brief Drena las órdenes de marcador acumuladas (el portador las transporta).
    [[nodiscard]] std::vector<MarkerWrite> take_pending_markers();

    /// Metadatos de la transacción de @p producer_id, o `nullptr` si no existe (observabilidad).
    [[nodiscard]] const TransactionMetadata* find(ProducerId producer_id) const;
    [[nodiscard]] std::size_t transaction_count() const noexcept { return txns_.size(); }

private:
    /// Localiza la transacción de @p producer_id validando @p epoch (fencing). El puntero es válido
    /// hasta la próxima mutación del mapa.
    [[nodiscard]] expected<TransactionMetadata*> require(ProducerId producer_id,
                                                         std::int16_t epoch);
    /// Transiciona @p txn a `Prepare*` según @p decision y encola sus marcadores (o concluye si no
    /// hay participantes).
    void enter_prepare(TransactionMetadata& txn, ControlRecordType decision, MonoTime now);
    /// Encola un `MarkerWrite` por cada participante de @p txn.
    void enqueue_markers(const TransactionMetadata& txn, ControlRecordType decision);

    Epoch coordinator_epoch_;
    std::chrono::milliseconds txn_timeout_;
    std::unordered_map<ProducerId, TransactionMetadata> txns_;
    std::vector<MarkerWrite> pending_markers_;
};

}  // namespace nexus
