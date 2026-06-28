/// @file   kafka/fetch.hpp
/// @brief  API Fetch del subconjunto Kafka (petición/respuesta, v12 flexible) — F7d.
/// @ingroup kafka

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "kafka/codec.hpp"

namespace nexus::kafka {

/// Partición pedida en una petición Fetch.
struct FetchPartitionRequest {
    std::int32_t partition = 0;
    std::int32_t current_leader_epoch = -1;
    std::int64_t fetch_offset = 0;
    std::int32_t last_fetched_epoch = -1;
    std::int64_t log_start_offset = -1;
    std::int32_t partition_max_bytes = 0;
};

/// Topic pedido en una petición Fetch.
struct FetchTopicRequest {
    std::string topic;
    std::vector<FetchPartitionRequest> partitions;
};

/// @brief Petición **Fetch** (v12 flexible). Afinidad: INMUTABLE.
struct FetchRequest {
    std::int32_t replica_id = -1;
    std::int32_t max_wait_ms = 0;
    std::int32_t min_bytes = 0;
    std::int32_t max_bytes = 0;
    std::int8_t isolation_level = 0;
    std::int32_t session_id = 0;
    std::int32_t session_epoch = -1;
    std::vector<FetchTopicRequest> topics;
    std::string rack_id;
};

/// Respuesta de una partición en Fetch (el blob de records es opaco al codec).
struct FetchPartitionResponse {
    std::int32_t partition_index = 0;
    std::int16_t error_code = 0;
    std::int64_t high_watermark = 0;
    std::int64_t last_stable_offset = -1;
    std::int64_t log_start_offset = 0;
    std::int32_t preferred_read_replica = -1;
    std::vector<std::byte> records;  ///< RecordBatch(es) crudos; vacío = sin records.
};

/// Respuesta de un topic en Fetch.
struct FetchTopicResponse {
    std::string topic;
    std::vector<FetchPartitionResponse> partitions;
};

/// @brief Respuesta **Fetch** (v12 flexible). Afinidad: INMUTABLE.
struct FetchResponse {
    std::int32_t throttle_time_ms = 0;
    std::int16_t error_code = 0;
    std::int32_t session_id = 0;
    std::vector<FetchTopicResponse> topics;
};

/// @brief Decodifica el **cuerpo** de una petición Fetch desde @p dec (tras la cabecera).
/// @param api_version Versión negociada: < 12 usa el formato **clásico** (longitudes
/// `INT16`/`INT32`,
///   sin *tagged fields*); >= 12, el **flexible** (compacto + tagged). Gobierna además qué campos
///   están presentes (p. ej. `current_leader_epoch` desde v9, `last_fetched_epoch` desde v12).
[[nodiscard]] expected<FetchRequest> decode_fetch_request(Decoder& dec, std::int16_t api_version);

/// @brief Serializa el **cuerpo** de una respuesta Fetch en @p enc según @p api_version (clásica o
///   flexible; gates de `throttle`/`error`/`last_stable_offset`/`log_start_offset`/réplica
///   preferida).
void encode_fetch_response(Encoder& enc, const FetchResponse& resp, std::int16_t api_version);

}  // namespace nexus::kafka
