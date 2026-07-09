/// @file   broker/topic_manager.hpp
/// @brief  TopicManager: crea/borra topics y abre sus particiones (plano de control).
/// @ingroup broker

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "broker/topic.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "consensus/deferred_message_sink.hpp"
#include "consensus/raft_carrier.hpp"
#include "consensus/raft_node.hpp"
#include "protocol/messages.hpp"

namespace nexus {

class RaftCarrier;
class RaftMessageSink;
class MetricsRegistry;

/// @brief Estado por réplica de una partición replicada (Raft): su portador, almacén durable y
///   sumidero. Definido en el `.cpp` (la cabecera solo lo declara). Afinidad: REACTOR-LOCAL.
struct ReplicaContext;

/// @brief Gestiona el ciclo de vida de los topics del nodo (plano de control). Afinidad:
///   THREAD-SAFE (un mutex protege el mapa; las creaciones/borrados son poco frecuentes).
/// @details Cada topic vive en `data_dir/<nombre>/<partición>/` (un `PartitionLog` por partición).
///   **Sharding por núcleo (ADR-0026):** una instancia atiende un único núcleo y abre **solo** las
///   particiones que le tocan (`partition % num_cores == owner_core`); con `num_cores == 1`
///   (por defecto) abre todas (equivalente al mono-reactor). Los **metadatos** del topic (nombre,
///   `partition_count`) se registran completos en cada instancia, de modo que `describe`/`Metadata`
///   y la validación son locales sin cross-core. `get` devuelve un puntero válido mientras el topic
///   no se borre (los topics se crean al arrancar, antes de servir).
class TopicManager {
public:
    /// @param data_dir Raíz de los logs de partición.
    /// @param num_cores Número de núcleos del nodo (>= 1; valores < 1 se tratan como 1).
    /// @param owner_core Núcleo que atiende esta instancia (se acota a `[0, num_cores)`).
    /// @param node_id Identidad de este nodo (votante de las particiones replicadas).
    /// @param raft_config Parámetros de Raft (timeouts, semilla) de las particiones replicadas.
    /// @param voter_peers Los **demás** votantes del grupo Raft de cada partición replicada (los
    /// node
    ///   ids del resto del clúster). Vacío = votante único (se autoconfirma). Simplificación de
    ///   esta fase: replicación total (cada partición replicada vive en todos los nodos del
    ///   directorio).
    /// @note Definido fuera de línea: `replicas_` es `vector<unique_ptr<ReplicaContext>>` con
    ///   `ReplicaContext` incompleto en la cabecera (pimpl).
    /// @param compaction Política de compactación del log de Raft (umbral de entradas aplicadas)
    ///   que se pasa a cada `RaftCarrier`; por defecto desactivada (umbral 0).
    explicit TopicManager(std::filesystem::path data_dir, int num_cores = 1, int owner_core = 0,
                          NodeId node_id = 0, RaftConfig raft_config = {},
                          std::vector<NodeId> voter_peers = {},
                          CompactionPolicy compaction = {}) noexcept;
    TopicManager(const TopicManager&) = delete;
    TopicManager& operator=(const TopicManager&) = delete;
    TopicManager(TopicManager&&) = delete;
    TopicManager& operator=(TopicManager&&) = delete;
    ~TopicManager();

    /// Longitud máxima (en bytes) de un nombre de topic; alineada con el límite de Kafka.
    static constexpr std::size_t kMaxTopicNameLength = 249;

    /// @brief Valida un nombre de topic. **Fuente única** de las reglas de nombrado, reutilizada
    /// por
    ///   todas las superficies (protocolo nativo vía `create_topic`, y REST vía `AdminApi`), de
    ///   modo que ninguna acepte lo que otra rechaza.
    /// @details Reglas: no vacío; longitud <= `kMaxTopicNameLength`; ni `.` ni `..`; y solo
    ///   caracteres del juego seguro `[A-Za-z0-9._-]`. Esto excluye espacios, separadores de ruta
    ///   (`/`, `\`) y bytes de control, que de otro modo crearían ficheros fuera del árbol previsto
    ///   del topic (p. ej. un nombre vacío materializaba `data_dir/<partición>` en la raíz).
    /// @return `void` si es válido, o `InvalidArgument` con el motivo.
    [[nodiscard]] static expected<void> validate_topic_name(std::string_view name);

    /// @brief Crea un topic con @p partition_count particiones (abre un log por cada una).
    /// @details Si @p replication_factor > 1, las particiones propias se crean como
    ///   `ReplicatedPartition` (respaldadas por Raft) y se les asocia un `RaftCarrier` que las
    ///   conduce; si es 1, son `Partition` (mono-nodo, *ack* local). En esta fase (pre-D3.5, sin
    ///   transporte inter-nodo) el grupo Raft lo forma **solo** el nodo local (sin peers): votante
    ///   único que se autoconfirma; D3.5 cableará los peers reales del clúster.
    /// @return Los metadatos del topic, o un error (`InvalidArgument` si el nombre es inválido
    ///   (`validate_topic_name`), si ya existe o si el conteo es inválido; error de E/S si no se
    ///   puede abrir algún log o su estado Raft).
    [[nodiscard]] expected<TopicMetadata> create_topic(std::string name,
                                                       std::int32_t partition_count,
                                                       TopicConfig config = {},
                                                       std::int16_t replication_factor = 1);

    /// @brief Borra un topic: lo quita del registro **y elimina sus ficheros en disco**.
    /// @details Borra los datos (`.log`/`.index` y el estado de Raft) de las particiones que este
    ///   núcleo posee (`data_dir/<nombre>/<partición>/`, sharding ADR-0026); con el fan-out
    ///   cross-core, entre todos los núcleos se eliminan todas. Suelta primero el estado en memoria
    ///   (portadores Raft y el `Topic`, cerrando los descriptores) y luego desenlaza del disco,
    ///   para no dejar descriptores sobre ficheros ya borrados. Evita la **fuga de disco** y la
    ///   **resurrección** (re-declarar el nombre arranca vacío).
    /// @return `void`; `NotFound` si el topic no existe; `IoError` si el borrado en disco de alguna
    ///   partición propia falla (el topic ya queda des-registrado en memoria).
    [[nodiscard]] expected<void> delete_topic(std::string_view name);

    /// @return El topic @p name, o `nullptr` si no existe.
    [[nodiscard]] Topic* get(std::string_view name);

    /// @brief ¿Atiende este núcleo la partición @p pid? (`pid % num_cores == owner_core`,
    /// ADR-0026).
    /// @details Misma regla que `PartitionRouter::owner_core`; el router decide local vs
    /// cross-core.
    [[nodiscard]] bool owns_partition(PartitionId pid) const noexcept {
        return static_cast<int>(static_cast<std::size_t>(pid) %
                                static_cast<std::size_t>(num_cores_)) == owner_core_;
    }

    /// Número de núcleos del nodo con el que se construyó (para el reparto de particiones).
    [[nodiscard]] int num_cores() const noexcept { return num_cores_; }
    /// Núcleo que atiende esta instancia.
    [[nodiscard]] int owner_core() const noexcept { return owner_core_; }

    /// Construye los `TopicMeta` para una `MetadataResponse` (líder = @p leader_node_id en 1b).
    [[nodiscard]] std::vector<TopicMeta> describe(NodeId leader_node_id) const;

    /// @brief Devuelve los metadatos de todos los topics (control-plane; admin/observabilidad).
    [[nodiscard]] std::vector<TopicMetadata> list_metadata() const;

    [[nodiscard]] std::size_t topic_count() const;

    /// @brief Portadores Raft de las particiones replicadas de este núcleo (para que el reactor
    ///   dueño los conduzca con `on_tick`). Vacío si no hay particiones replicadas.
    [[nodiscard]] std::vector<RaftCarrier*> carriers() const;

    /// @brief Portador Raft de la réplica @p topic / @p partition de este núcleo (para enrutarle un
    ///   `RaftEnvelope` recibido del plano inter-nodo).
    /// @return El portador, o `nullptr` si no hay una partición replicada `(topic, partition)` en
    ///   este núcleo.
    [[nodiscard]] RaftCarrier* carrier_for(std::string_view topic, PartitionId partition) const;

    /// @brief Instala el sumidero de Raft real (el transporte inter-nodo) al que enviarán todos los
    ///   portadores de este núcleo; `nullptr` lo desconecta (los sobres se descartan).
    /// @details Lo llama el *composition root* al arrancar el reactor (cuando ya existe el
    ///   `Proactor`). Antes de instalarlo, los portadores funcionan como votante único sin
    ///   transporte. Solo desde el hilo del reactor (el sumidero diferido es reactor-local).
    void set_message_sink(RaftMessageSink* sink) noexcept { raft_sink_.set_target(sink); }

    /// @brief Cablea el registro de métricas (ADR-0017) a los portadores de este núcleo: las series
    ///   de replicación de cada réplica (`commit_index`/término/rol y tráfico de Raft). Guarda el
    ///   registro y lo aplica a los portadores ya creados; los que se creen después se autocablean.
    /// @details Lo llama el *composition root* al arrancar. Las réplicas de un núcleo solo se
    ///   construyen/tocan en su propio hilo, así que llamar a esto pre-run (monohilo) o desde el
    ///   hilo del reactor dueño es seguro; el `MetricsRegistry` es THREAD-SAFE.
    /// @param[in,out] metrics Registro de métricas; vive más que este `TopicManager`.
    void set_metrics(MetricsRegistry& metrics);

private:
    std::filesystem::path data_dir_;
    int num_cores_;           ///< Núcleos del nodo (>= 1).
    int owner_core_;          ///< Núcleo que atiende esta instancia (`[0, num_cores_)`).
    NodeId node_id_;          ///< Identidad del nodo (votante de las particiones replicadas).
    RaftConfig raft_config_;  ///< Parámetros de Raft de las particiones replicadas.
    std::vector<NodeId> voter_peers_;  ///< Los demás votantes del grupo Raft (resto del clúster).
    CompactionPolicy compaction_;      ///< Política de compactación que se pasa a cada portador.
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<Topic>> topics_;
    /// Sumidero de Raft compartido por los portadores de este núcleo (reenvía al transporte real,
    /// instalado al arrancar). Declarado **antes** que `replicas_` para que se destruya **después**
    /// (los portadores lo referencian).
    DeferredMessageSink raft_sink_;
    /// Registro de métricas (no propietario; cableado por `set_metrics`). `nullptr` = sin métricas;
    /// si no lo es, cada portador nuevo se cablea al crearse. Solo lo toca el hilo de este núcleo.
    MetricsRegistry* metrics_ = nullptr;
    /// Portadores/almacén de las particiones replicadas. Declarado **tras** `topics_` y
    /// `raft_sink_` para destruirse **antes** (un portador referencia el `RaftNode` de su partición
    /// en `topics_` y el `raft_sink_`).
    std::vector<std::unique_ptr<ReplicaContext>> replicas_;
};

}  // namespace nexus
