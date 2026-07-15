/// @file   broker/partition_base.hpp
/// @brief  PartitionBase: interfaz común de las particiones servibles del broker.
/// @ingroup broker

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>

#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "storage/fetch_result.hpp"
#include "storage/partition_log.hpp"
#include "storage/retention.hpp"

namespace nexus {

/// @brief Protocolo de wire que **posee** la codificación de records de una partición (P2,
///   [ADR-0030](../adr/adr-0030-particion-mono-protocolo.md)). Afinidad: INMUTABLE.
/// @details El plano nativo guarda los records en su formato propio; el subconjunto Kafka envuelve
///   un `RecordBatch` v2 **opaco**. Los dos formatos son **incompatibles** dentro de una misma
///   partición: leer con el protocolo equivocado devolvería bytes ilegibles. Por eso la primera
///   escritura *reclama* el protocolo de la partición y las de otro protocolo se rechazan.
enum class WireProtocol : std::uint8_t {
    Unset = 0,  ///< Sin escrituras aún: la primera reclama el protocolo.
    Native,     ///< Protocolo binario nativo de NexusMQ (records nativos).
    Kafka,      ///< Subconjunto Kafka (F7f): un `RecordBatch` v2 opaco envuelto.
};

/// @brief Superficie común de una partición servible por el broker. Afinidad: REACTOR-LOCAL.
/// @details Unifica la `Partition` (mono-nodo, *ack* local) y la `ReplicatedPartition` (respaldada
///   por Raft, ADR-0016) tras una única interfaz de hot-path, de modo que el `Topic` y el
///   `RequestRouter` sirven una partición **sin conocer** su clase concreta: el tipo se elige al
///   crearla según `replication_factor` (ADR-0026). Es polimorfismo en el **borde** del hot-path
///   —una indirección virtual por `produce`/`fetch`, despreciable frente a la E/S del log— que
///   mantiene a los llamantes cerrados a modificación al añadir tipos de partición (OCP).
/// @invariant `high_watermark()` ≤ `log().log_end_offset()` (lo visible nunca supera lo escrito).
/// @note Base polimórfica: copia/movimiento **protegidos** (sin *slicing* desde el llamante; las
///   derivadas siguen siendo movibles) y destructor virtual público.
class PartitionBase {
public:
    PartitionBase() = default;
    virtual ~PartitionBase() = default;

    /// @brief Anexa (o propone, en réplica) @p batch tras validar idempotencia. HOT PATH (§7.11
    /// #1).
    /// @return El último offset asignado al batch, o un error.
    [[nodiscard]] virtual expected<Offset> produce(const RecordBatch& batch) = 0;

    /// @brief Lee batches desde @p offset hasta ~@p max_bytes; el llamante respeta el
    /// `high_watermark`.
    [[nodiscard]] virtual expected<FetchResult> fetch(Offset offset,
                                                      std::size_t max_bytes) const = 0;

    /// @brief Frontera visible para los consumidores (ack local en `Partition`; offset del
    ///   `commit_index` en `ReplicatedPartition`).
    [[nodiscard]] virtual Offset high_watermark() const = 0;

    /// @brief ¿Es esta réplica el líder que acepta escrituras?
    [[nodiscard]] virtual bool is_leader() const noexcept = 0;

    /// @brief Época de liderazgo vigente (metadata / cierre de productores).
    [[nodiscard]] virtual Epoch leader_epoch() const noexcept = 0;

    /// @brief ¿Está respaldada por Raft (`ReplicatedPartition`)? Por defecto no (`Partition`).
    /// @details Lo usa el camino de produce para decidir si registrar la latencia de confirmación a
    ///   quórum (solo aplica a réplicas); evita el coste de ese registro en particiones mono-nodo.
    [[nodiscard]] virtual bool is_replicated() const noexcept { return false; }

    /// @brief Acceso de solo lectura al log subyacente (offsets de inicio/fin, lecturas).
    [[nodiscard]] virtual const PartitionLog& log() const noexcept = 0;

    /// @brief Aplica la retención al log de esta partición: reclama segmentos sellados por
    ///   tamaño/tiempo (nunca el activo). **No-op por defecto**: las particiones replicadas
    ///   gestionan su prefijo por **compactación de Raft** ([ADR-0024], sobre el `commit_index`),
    ///   no por retención directa —borrar segmentos bajo el `RaftLog` rompería sus invariantes—,
    ///   así que la retención por política solo la aplica `Partition` (mono-nodo).
    /// @param policy Política de retención derivada de la config **actual** del topic.
    /// @param now Instante de referencia para la retención por tiempo (se inyecta; ver
    ///   `PartitionLog::enforce_retention`). REACTOR-LOCAL: se llama en el hilo dueño de la
    ///   partición.
    [[nodiscard]] virtual expected<void> enforce_retention(
        const RetentionPolicy& /*policy*/, std::filesystem::file_time_type /*now*/) {
        return {};
    }

    /// @brief Reclama @p proto como protocolo de esta partición si aún no tiene dueño, o confirma
    ///   que ya es el suyo. Lo invoca el camino de `produce` **antes** de anexar (P2, ADR-0030).
    /// @return Vacío si la partición queda (o ya estaba) en @p proto; `InvalidArgument` si ya la
    ///   posee el **otro** protocolo (produce cruzado nativo/Kafka: formatos de record
    ///   incompatibles). El borde lo traduce a `InvalidRequest` en ambos wires.
    [[nodiscard]] expected<void> claim_protocol(WireProtocol proto) {
        if (protocol_ == WireProtocol::Unset || protocol_ == proto) {
            protocol_ = proto;
            return {};
        }
        return make_error(ErrorCode::InvalidArgument,
                          "partición con records de otro protocolo de wire (cruce nativo/Kafka)");
    }

    /// @brief Protocolo que posee la codificación de records; `Unset` si aún no se ha escrito.
    /// @details Lo consulta el camino de `fetch` para **rechazar** una lectura de otro protocolo en
    ///   vez de devolver bytes ilegibles (o cero records) en silencio.
    [[nodiscard]] WireProtocol protocol() const noexcept { return protocol_; }

protected:
    PartitionBase(const PartitionBase&) = default;
    PartitionBase& operator=(const PartitionBase&) = default;
    PartitionBase(PartitionBase&&) = default;
    PartitionBase& operator=(PartitionBase&&) = default;

private:
    /// Protocolo dueño de la codificación de records (P2). Lo fija la primera escritura; `fetch` y
    /// las escrituras posteriores lo consultan para rechazar cruces. REACTOR-LOCAL (sin
    /// sincronizar).
    WireProtocol protocol_{WireProtocol::Unset};
};

}  // namespace nexus
