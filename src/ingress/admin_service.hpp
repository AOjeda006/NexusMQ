/// @file   ingress/admin_service.hpp
/// @brief  Puerto `AdminService` + DTOs del REST admin (ADR-0018): contrato de administración.
/// @ingroup ingress

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/error.hpp"
#include "common/task.hpp"
#include "ingress/pagination.hpp"

namespace nexus {

/// @brief Especificación para crear un topic (DTO del REST admin). Afinidad: INMUTABLE (valor).
/// @details Datos planos, **sin dependencia del broker**: el adaptador (`AdminApi`, ADR-0018) los
///   traduce a los tipos internos. `0`/`-1` significan «valor por defecto del broker».
struct CreateTopicSpec {
    std::string name;                     ///< Nombre del topic (no vacío).
    std::int32_t partition_count = 1;     ///< Número de particiones (> 0).
    std::int16_t replication_factor = 1;  ///< Factor de réplica (1 en Fase 1b).
    std::int64_t segment_bytes = 0;       ///< Tamaño de segmento (0 = por defecto).
    std::int64_t retention_ms = -1;       ///< Retención por tiempo (-1 = sin límite).
    std::int64_t retention_bytes = -1;    ///< Retención por tamaño (-1 = sin límite).
};

/// @brief Resumen de un topic (DTO de listados/creación). Afinidad: INMUTABLE (valor).
struct TopicSummary {
    std::string name;
    std::int32_t partition_count = 0;
    std::int16_t replication_factor = 1;
    std::int64_t created_at_ms = 0;
};

/// @brief Estado de una partición (DTO de `describe`). Afinidad: INMUTABLE (valor).
struct PartitionInfo {
    std::int32_t id = 0;
    std::int32_t leader = 0;          ///< Nodo líder (este nodo en Fase 1b).
    std::int64_t high_watermark = 0;  ///< Frontera visible a consumidores.
    std::int64_t leader_epoch = 0;
};

/// @brief Descripción detallada de un topic (DTO). Afinidad: INMUTABLE (valor).
struct TopicDescription {
    TopicSummary summary;
    std::vector<PartitionInfo> partitions;
};

/// @brief Resumen de un grupo de consumidores (DTO de listados). Afinidad: INMUTABLE (valor).
struct GroupSummary {
    std::string group_id;
    std::string state;  ///< Estado de la FSM del grupo (texto).
    std::int32_t generation = 0;
    std::int64_t member_count = 0;
};

/// @brief Miembro de un grupo de consumidores (DTO de `describe`). Afinidad: INMUTABLE (valor).
/// @details La **asignación de particiones** por miembro es un *blob* opaco del cliente (contrato
///   Kafka: el líder del grupo la codifica), así que **no se decodifica** aquí; se expone solo lo
///   estructurado (id, tamaño de la suscripción). `subscription_bytes` es el tamaño de los
///   metadatos de suscripción opacos.
struct GroupMemberInfo {
    std::string member_id;
    std::int64_t subscription_bytes = 0;  ///< Tamaño de la suscripción opaca (no se decodifica).
};

/// @brief Offset confirmado de un grupo en una partición, con su *lag* (DTO). Afinidad: INMUTABLE.
struct GroupPartitionOffset {
    std::string topic;
    std::int32_t partition = 0;
    std::int64_t committed_offset = 0;  ///< Último offset confirmado por el grupo.
    std::int64_t high_watermark = 0;    ///< Frontera visible de la partición (offset del dueño).
    std::int64_t lag = 0;               ///< `high_watermark - committed_offset` (acotado a >= 0).
};

/// @brief Descripción detallada de un grupo (DTO de `describe`). Afinidad: INMUTABLE (valor).
struct GroupDescription {
    std::string group_id;
    std::string state;  ///< Estado de la FSM del grupo (texto).
    std::int32_t generation = 0;
    std::string leader_id;  ///< Miembro líder de la generación (vacío si el grupo no es estable).
    std::vector<GroupMemberInfo> members;
    std::vector<GroupPartitionOffset> offsets;
};

/// @brief Un nodo del clúster (DTO de `describe_cluster`). Afinidad: INMUTABLE (valor).
struct NodeInfo {
    std::int32_t node_id = 0;
    bool is_self = false;  ///< ¿Es este nodo?
};

/// @brief Progreso de replicación de un seguidor visto por el líder (DTO). Afinidad: INMUTABLE.
struct FollowerProgress {
    std::int32_t node = 0;         ///< NodeId del seguidor.
    std::int64_t match_index = 0;  ///< Mayor índice replicado confirmado por el seguidor.
    std::int64_t lag = 0;          ///< `last_log_index - match_index` (acotado a >= 0).
};

/// @brief Estado Raft de una partición replicada (DTO de `describe_cluster`). Afinidad: INMUTABLE.
struct PartitionRaftInfo {
    std::string topic;
    std::int32_t partition = 0;
    std::int32_t leader = -1;         ///< NodeId del líder conocido, o `-1` si se desconoce.
    std::string role;                 ///< Rol de esta réplica (`follower`/`candidate`/`leader`…).
    std::int64_t term = 0;
    std::int64_t commit_index = 0;    ///< High-watermark de la réplica (entradas aplicadas).
    std::int64_t last_log_index = 0;  ///< Último índice del log local.
    std::int64_t leader_epoch = 0;
    std::vector<FollowerProgress> followers;  ///< Progreso por seguidor (solo si esta réplica lidera).
};

/// @brief Estado del clúster/Raft (DTO de `describe_cluster`). Afinidad: INMUTABLE (valor).
struct ClusterInfo {
    std::int32_t node_id = 0;  ///< Este nodo.
    std::vector<NodeInfo> nodes;
    std::vector<PartitionRaftInfo> partitions;  ///< Réplicas de Raft de este nodo (por partición).
};

/// @brief Puerto de administración del REST admin (ADR-0018). Afinidad: THREAD-SAFE (contrato).
/// @details Interfaz que el `RestGateway` (ingress) usa para administrar el broker, sin acoplarse
/// al
///   broker ni al server (inversión de dependencias). El adaptador concreto `AdminApi` (en
///   `nexus-server`) la implementa sobre `TopicManager`/`GroupCoordinator`. Devuelve `expected` con
///   `Error` del núcleo (ADR-0009); el borde REST lo traduce a RFC 7807.
class AdminService {
public:
    AdminService() = default;
    AdminService(const AdminService&) = delete;
    AdminService& operator=(const AdminService&) = delete;
    AdminService(AdminService&&) = delete;
    AdminService& operator=(AdminService&&) = delete;
    virtual ~AdminService() = default;

    /// @brief Crea un topic según @p spec. `InvalidArgument` si ya existe o los parámetros no son
    ///   válidos.
    /// @details Corrutina: la creación puede propagarse a varios núcleos por paso de mensajes
    ///   (`call_on`, ADR-0026), por eso devuelve `task` (el borde REST la `co_await`ea).
    [[nodiscard]] virtual task<expected<TopicSummary>> create_topic(
        const CreateTopicSpec& spec) = 0;

    /// @brief Borra el topic @p name. `NotFound` si no existe. Corrutina (fan-out cross-core).
    [[nodiscard]] virtual task<expected<void>> delete_topic(std::string_view name) = 0;

    /// @brief Describe el topic @p name (resumen + particiones). `NotFound` si no existe.
    /// @details Corrutina: los *high-watermark*/epoch de cada partición viven en su núcleo dueño,
    /// así
    ///   que se agregan por paso de mensajes (`call_on`, ADR-0026); el borde REST la `co_await`ea.
    [[nodiscard]] virtual task<expected<TopicDescription>> describe_topic(
        std::string_view name) = 0;

    /// Lista los topics (ordenados por nombre) paginados según @p page.
    [[nodiscard]] virtual std::vector<TopicSummary> list_topics(Page page) const = 0;

    /// @brief Lista los grupos de consumidores (ordenados por id) paginados según @p page.
    /// @details Corrutina: cada grupo se coordina en su núcleo (`hash(group_id) % N`, ADR-0026),
    /// así
    ///   que el listado **agrega** con `call_on` sobre todos los núcleos.
    [[nodiscard]] virtual task<std::vector<GroupSummary>> list_groups(Page page) = 0;

    /// @brief Describe el grupo @p group_id (miembros, offsets confirmados y *lag*). `NotFound` si
    ///   no existe.
    /// @details Corrutina **reactor-local**: el grupo vive en un **único** núcleo coordinador
    ///   (`hash(group_id) % N`, ADR-0026), así que se lee ahí (más barato que agregar). El *lag* de
    ///   cada partición se calcula con su *high-watermark*, que vive en el núcleo dueño de la
    ///   partición (`partition % N`) y se lee por `call_on`.
    [[nodiscard]] virtual task<expected<GroupDescription>> describe_group(
        std::string_view group_id) = 0;

    /// @brief Estado del clúster: nodos conocidos y estado Raft de las particiones replicadas de
    ///   este nodo. Nunca falla en operación normal (devuelve `expected` por uniformidad del
    ///   contrato).
    /// @details Corrutina: las réplicas de Raft viven en el núcleo dueño de su partición
    ///   (`partition % N`, ADR-0026), así que el estado se **agrega** con `call_on` sobre todos los
    ///   núcleos. Sin particiones replicadas (`replication_factor == 1`), `partitions` va vacío.
    [[nodiscard]] virtual task<expected<ClusterInfo>> describe_cluster() = 0;
};

}  // namespace nexus
