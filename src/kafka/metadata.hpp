/// @file   kafka/metadata.hpp
/// @brief  API Metadata del subconjunto Kafka (petición/respuesta, versión flexible v9) — F7c.
/// @ingroup kafka

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/error.hpp"
#include "kafka/codec.hpp"

namespace nexus::kafka {

/// @brief Petición **Metadata** (forma flexible, v9). Afinidad: INMUTABLE.
/// @details `topics` nulo (`nullopt`) pide **todos** los topics; una lista vacía no pide ninguno.
struct MetadataRequest {
    std::optional<std::vector<std::string>> topics;  ///< `nullopt` = todos los topics.
    bool allow_auto_topic_creation = false;
    bool include_cluster_authorized_operations = false;
    bool include_topic_authorized_operations = false;
};

/// Un broker anunciado en la respuesta Metadata.
struct MetadataBroker {
    std::int32_t node_id = 0;
    std::string host;
    std::int32_t port = 0;
    std::optional<std::string> rack;
};

/// Una partición dentro de un topic en la respuesta Metadata.
struct MetadataPartition {
    std::int16_t error_code = 0;
    std::int32_t partition_index = 0;
    std::int32_t leader_id = 0;
    std::int32_t leader_epoch = -1;
    std::vector<std::int32_t> replica_nodes;
    std::vector<std::int32_t> isr_nodes;
    std::vector<std::int32_t> offline_replicas;
};

/// Un topic en la respuesta Metadata.
struct MetadataTopic {
    std::int16_t error_code = 0;
    std::string name;
    bool is_internal = false;
    std::vector<MetadataPartition> partitions;
    std::int32_t topic_authorized_operations = -2147483648;  // INT32_MIN: «no calculado»
};

/// @brief Respuesta **Metadata** (forma flexible, v9). Afinidad: INMUTABLE.
struct MetadataResponse {
    std::int32_t throttle_time_ms = 0;
    std::vector<MetadataBroker> brokers;
    std::optional<std::string> cluster_id;
    std::int32_t controller_id = -1;
    std::vector<MetadataTopic> topics;
    std::int32_t cluster_authorized_operations = -2147483648;  // INT32_MIN: «no calculado»
};

/// @brief Decodifica el **cuerpo** de una petición Metadata v9 desde @p dec (tras la cabecera).
[[nodiscard]] expected<MetadataRequest> decode_metadata_request(Decoder& dec);

/// @brief Serializa el **cuerpo** de una respuesta Metadata v9 (flexible) en @p enc.
void encode_metadata_response(Encoder& enc, const MetadataResponse& resp);

}  // namespace nexus::kafka
