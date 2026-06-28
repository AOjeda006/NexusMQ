/// @file   kafka/produce.hpp
/// @brief  API Produce del subconjunto Kafka (petición/respuesta, v9 flexible) — F7d.
/// @ingroup kafka

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "kafka/codec.hpp"

namespace nexus::kafka {

/// Datos de una partición en una petición Produce: el blob de records (RecordBatch) sin
/// interpretar.
struct ProducePartitionData {
    std::int32_t index = 0;
    std::vector<std::byte> records;  ///< RecordBatch(es) crudos (opaco para el codec).
};

/// Datos de un topic en una petición Produce.
struct ProduceTopicData {
    std::string name;
    std::vector<ProducePartitionData> partitions;
};

/// @brief Petición **Produce** (v9 flexible). Afinidad: INMUTABLE.
struct ProduceRequest {
    std::optional<std::string> transactional_id;
    std::int16_t acks = 0;
    std::int32_t timeout_ms = 0;
    std::vector<ProduceTopicData> topics;
};

/// Respuesta de una partición en Produce.
struct ProducePartitionResponse {
    std::int32_t index = 0;
    std::int16_t error_code = 0;
    std::int64_t base_offset = 0;
    std::int64_t log_append_time_ms = -1;
    std::int64_t log_start_offset = 0;
    std::optional<std::string> error_message;
};

/// Respuesta de un topic en Produce.
struct ProduceTopicResponse {
    std::string name;
    std::vector<ProducePartitionResponse> partitions;
};

/// @brief Respuesta **Produce** (v9 flexible). Afinidad: INMUTABLE.
struct ProduceResponse {
    std::vector<ProduceTopicResponse> topics;
    std::int32_t throttle_time_ms = 0;
};

/// @brief Decodifica el **cuerpo** de una petición Produce desde @p dec (tras la cabecera).
/// @param api_version Versión negociada: < 9 usa el formato **clásico** (longitudes
/// `INT16`/`INT32`,
///   sin *tagged fields*); >= 9, el **flexible** (compacto + tagged). Gobierna además qué campos
///   están presentes (p. ej. `transactional_id` desde v3).
[[nodiscard]] expected<ProduceRequest> decode_produce_request(Decoder& dec,
                                                              std::int16_t api_version);

/// @brief Serializa el **cuerpo** de una respuesta Produce en @p enc según @p api_version (clásica
///   o flexible; gates de `log_append_time`/`log_start_offset`/`record_errors`/`throttle`).
void encode_produce_response(Encoder& enc, const ProduceResponse& resp, std::int16_t api_version);

}  // namespace nexus::kafka
