/// @file   broker/topic_catalog.hpp
/// @brief  TopicCatalog: posee un TopicManager por núcleo (sharding ADR-0026) y crea topics
///         replicándolos a todos los núcleos en el arranque.
/// @ingroup broker

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "broker/topic.hpp"
#include "broker/topic_manager.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "consensus/raft_node.hpp"

namespace nexus {

/// @brief Posee un `TopicManager` por núcleo (sharding por núcleo, ADR-0026) y coordina la creación
///   replicada de topics. Afinidad: el catálogo se construye y se usa en el *composition root*
///   (`Server`) **antes de servir** (monohilo); cada `TopicManager` que contiene es luego
///   REACTOR-LOCAL de su núcleo.
/// @details El plano de datos se reparte: la partición `p` la sirve el núcleo `p % N` (ADR-0026),
///   así que cada `TopicManager` abre solo sus particiones pero registra los metadatos completos de
///   cada topic. El catálogo materializa ese reparto: crea N managers (uno por núcleo) y, al crear
///   un topic, lo replica a todos. La propagación **en caliente** de un cambio de metadatos
///   (CreateTopic/DeleteTopic por wire) la hace el `RequestRouter` por paso de mensajes
///   (`call_on`); este `create_topic` es para el **arranque** (pre-run, monohilo), cuando no hay
///   reactores corriendo y el reparto directo es seguro.
/// @invariant `core_count() >= 1`; los managers no cambian de identidad tras construir.
class TopicCatalog {
public:
    /// @param data_dir Raíz de los logs de partición.
    /// @param num_cores Número de núcleos del nodo (>= 1; valores < 1 se tratan como 1).
    /// @param node_id Identidad del nodo (votante de las particiones replicadas).
    /// @param raft_config Parámetros de Raft de las particiones replicadas.
    /// @param voter_peers Los demás votantes del grupo Raft (node ids del resto del clúster); vacío
    ///   = votante único. Se propaga a cada `TopicManager`.
    /// @param compaction Política de compactación del log de Raft; se propaga a cada `TopicManager`
    ///   (y de ahí a cada `RaftCarrier`). Por defecto desactivada (umbral 0).
    /// @param encryption_key KEK de cifrado en reposo (ADR-0031); se propaga a cada `TopicManager`
    ///   (y de ahí al `LogConfig` de cada partición). `nullptr` = logs en claro (por defecto).
    /// @param tier Almacén por niveles (ADR-0032); puntero **no-propietario** compartido por el
    ///   nodo. Se propaga a cada `TopicManager`. `nullptr` = sin tiering (por defecto).
    explicit TopicCatalog(const std::filesystem::path& data_dir, int num_cores = 1,
                          NodeId node_id = 0, RaftConfig raft_config = {},
                          const std::vector<NodeId>& voter_peers = {},
                          CompactionPolicy compaction = {},
                          const std::shared_ptr<const EncryptionKey>& encryption_key = nullptr,
                          StorageTier* tier = nullptr);
    TopicCatalog(const TopicCatalog&) = delete;
    TopicCatalog& operator=(const TopicCatalog&) = delete;
    TopicCatalog(TopicCatalog&&) = delete;
    TopicCatalog& operator=(TopicCatalog&&) = delete;
    ~TopicCatalog() = default;

    /// Número de núcleos (= número de `TopicManager`).
    [[nodiscard]] int core_count() const noexcept { return static_cast<int>(managers_.size()); }

    /// @brief `TopicManager` del núcleo @p core (p. ej. el del núcleo 0, que atiende las
    /// conexiones).
    /// @pre `0 <= core < core_count()`.
    [[nodiscard]] TopicManager& manager(int core) noexcept {
        return *managers_[static_cast<std::size_t>(core)];
    }

    /// Punteros a todos los managers, indexados por `core_id` (para `RequestRouter::bind_cluster`).
    [[nodiscard]] std::vector<TopicManager*> managers() const;

    /// @brief Crea @p name (con @p partition_count particiones) en **todos** los núcleos: cada
    ///   manager abre solo sus particiones; los metadatos se registran completos.
    /// @return Los metadatos del topic (del núcleo 0, autoritativos), o el primer error con
    ///   **rollback** de los núcleos ya creados (garantía fuerte).
    /// @note Uso **pre-run** (monohilo). En caliente, la propagación de un cambio de metadatos a
    ///   todos los núcleos la hace el `RequestRouter` por paso de mensajes (ADR-0026), no aquí.
    [[nodiscard]] expected<TopicMetadata> create_topic(const std::string& name,
                                                       std::int32_t partition_count,
                                                       TopicConfig config = {},
                                                       std::int16_t replication_factor = 1);

private:
    std::vector<std::unique_ptr<TopicManager>>
        managers_;  ///< Uno por núcleo (indexado por core_id).
};

}  // namespace nexus
