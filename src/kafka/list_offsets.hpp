/// @file   kafka/list_offsets.hpp
/// @brief  API ListOffsets del subconjunto Kafka (petición/respuesta, v7 flexible) — F7f.
/// @ingroup kafka

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/error.hpp"
#include "kafka/codec.hpp"

namespace nexus::kafka {

/// Marca de tiempo especial: pide el offset **más antiguo** (log start) — `kcat -o beginning`.
inline constexpr std::int64_t kListOffsetsEarliest = -2;
/// Marca de tiempo especial: pide el offset **más reciente** (high-watermark) — `kcat -o end`.
inline constexpr std::int64_t kListOffsetsLatest = -1;

/// Partición pedida en una petición ListOffsets.
struct ListOffsetPartition {
    std::int32_t partition_index = 0;
    std::int32_t current_leader_epoch = -1;
    std::int64_t timestamp = kListOffsetsLatest;  ///< -2 = earliest, -1 = latest, o epoch ms.
};

/// Topic pedido en una petición ListOffsets.
struct ListOffsetTopic {
    std::string name;
    std::vector<ListOffsetPartition> partitions;
};

/// @brief Petición **ListOffsets** (v7 flexible). Afinidad: INMUTABLE.
/// @details La usan los consumidores (`kcat -C`) para resolver el offset de inicio/fin antes del
///   primer Fetch; sin ella, librdkafka no sabe por dónde empezar.
struct ListOffsetsRequest {
    std::int32_t replica_id = -1;
    std::int8_t isolation_level = 0;
    std::vector<ListOffsetTopic> topics;
};

/// Respuesta de una partición en ListOffsets.
struct ListOffsetPartitionResponse {
    std::int32_t partition_index = 0;
    std::int16_t error_code = 0;
    std::int64_t timestamp = -1;  ///< Marca del offset devuelto (-1 si no aplica).
    std::int64_t offset = -1;     ///< Offset resuelto (log start o high-watermark).
    std::int32_t leader_epoch = -1;
};

/// Respuesta de un topic en ListOffsets.
struct ListOffsetTopicResponse {
    std::string name;
    std::vector<ListOffsetPartitionResponse> partitions;
};

/// @brief Respuesta **ListOffsets** (v7 flexible). Afinidad: INMUTABLE.
struct ListOffsetsResponse {
    std::int32_t throttle_time_ms = 0;
    std::vector<ListOffsetTopicResponse> topics;
};

/// @brief Decodifica el **cuerpo** de una petición ListOffsets v7 desde @p dec (tras la cabecera).
[[nodiscard]] expected<ListOffsetsRequest> decode_list_offsets_request(Decoder& dec);

/// @brief Serializa el **cuerpo** de una respuesta ListOffsets v7 (flexible) en @p enc.
void encode_list_offsets_response(Encoder& enc, const ListOffsetsResponse& resp);

}  // namespace nexus::kafka
