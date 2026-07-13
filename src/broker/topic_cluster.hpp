/// @file   broker/topic_cluster.hpp
/// @brief  Fan-out cross-core de crear/borrar topic a todos los núcleos (message-passing,
/// ADR-0026).
/// @ingroup broker

#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>

#include "broker/topic.hpp"
#include "common/error.hpp"
#include "common/task.hpp"

namespace nexus {

class Reactor;
class PartitionRouter;
class TopicManager;

/// @brief Crea @p name (con @p partition_count particiones) en **todos** los núcleos vía `call_on`
///   (paso de mensajes, ADR-0026): cada reactor crea el topic en su propio `TopicManager`
///   —tocándolo solo desde su hilo— y abre únicamente las particiones que le tocan; los metadatos
///   se registran completos en cada núcleo.
/// @param self Reactor donde corre el llamante (atiende la petición; p. ej. el núcleo 0).
/// @param partitions Router de particiones del nodo (da el reactor de cada núcleo).
/// @param topics_by_core `TopicManager` de cada núcleo, indexado por `core_id`.
/// @param name Nombre del topic (se copia al *frame* de la corrutina: vive durante el fan-out).
/// @return Los metadatos del topic (los del núcleo 0, autoritativos), o el primer error con
///   **rollback** de los núcleos ya creados (garantía fuerte).
/// @pre `topics_by_core.size() == partitions.core_count()`; ambos viven durante toda la corrutina.
[[nodiscard]] task<expected<TopicMetadata>> create_topic_on_cluster(
    Reactor& self, PartitionRouter& partitions, std::span<TopicManager* const> topics_by_core,
    std::string name, std::int32_t partition_count, TopicConfig config = {});

/// @brief Borra @p name de **todos** los núcleos vía `call_on` (paso de mensajes, ADR-0026).
/// @return El resultado del núcleo 0 (autoritativo); el borrado es idempotente en los demás.
/// @pre `topics_by_core.size() == partitions.core_count()`; ambos viven durante toda la corrutina.
[[nodiscard]] task<expected<void>> delete_topic_on_cluster(
    Reactor& self, PartitionRouter& partitions, std::span<TopicManager* const> topics_by_core,
    std::string name);

/// @brief Publica la config **mutable** de @p name (retención) en **todos** los núcleos vía
///   `call_on` (paso de mensajes, ADR-0037), de modo que el barrido de retención de cada núcleo lea
///   el valor nuevo. Idempotente por núcleo; sin recursos que deshacer (no hace falta rollback).
/// @return Los metadatos actualizados del núcleo 0 (autoritativo), o `NotFound` si no existe.
/// @pre `topics_by_core.size() == partitions.core_count()`; ambos viven durante toda la corrutina.
[[nodiscard]] task<expected<TopicMetadata>> update_topic_config_on_cluster(
    Reactor& self, PartitionRouter& partitions, std::span<TopicManager* const> topics_by_core,
    std::string name, std::optional<std::int64_t> retention_ms,
    std::optional<std::int64_t> retention_bytes);

}  // namespace nexus
