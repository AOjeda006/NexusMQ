/// @file   ingress/admin_service.hpp
/// @brief  Puerto `AdminService` + DTOs del REST admin (ADR-0018): contrato de administración.
/// @ingroup ingress

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/error.hpp"
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

    /// Crea un topic según @p spec. `InvalidArgument` si ya existe o los parámetros no son válidos.
    [[nodiscard]] virtual expected<TopicSummary> create_topic(const CreateTopicSpec& spec) = 0;

    /// Borra el topic @p name. `NotFound` si no existe.
    [[nodiscard]] virtual expected<void> delete_topic(std::string_view name) = 0;

    /// Describe el topic @p name (resumen + particiones). `NotFound` si no existe.
    [[nodiscard]] virtual expected<TopicDescription> describe_topic(
        std::string_view name) const = 0;

    /// Lista los topics (ordenados por nombre) paginados según @p page.
    [[nodiscard]] virtual std::vector<TopicSummary> list_topics(Page page) const = 0;

    /// Lista los grupos de consumidores (ordenados por id) paginados según @p page.
    [[nodiscard]] virtual std::vector<GroupSummary> list_groups(Page page) const = 0;
};

}  // namespace nexus
