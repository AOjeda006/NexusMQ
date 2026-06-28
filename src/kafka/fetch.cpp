/// @file   kafka/fetch.cpp
/// @brief  Implementación de la API Fetch del subconjunto Kafka (clásica v4..v11 y flexible v12).
/// @ingroup kafka

#include "kafka/fetch.hpp"

#include <string_view>
#include <utility>

#include "kafka/messages.hpp"

namespace nexus::kafka {
namespace {

/// Versiones a partir de las cuales aparece cada campo de Fetch (gates del protocolo).
constexpr std::int16_t kFetchMaxBytesSince = 3;
constexpr std::int16_t kFetchIsolationSince = 4;
constexpr std::int16_t kFetchSessionSince = 7;
constexpr std::int16_t kFetchForgottenSince = 7;
constexpr std::int16_t kFetchLeaderEpochSince = 9;
constexpr std::int16_t kFetchLogStartSince = 5;
constexpr std::int16_t kFetchLastFetchedEpochSince = 12;
constexpr std::int16_t kFetchRackSince = 11;
constexpr std::int16_t kFetchThrottleSince = 1;
constexpr std::int16_t kFetchTopLevelErrorSince = 7;
constexpr std::int16_t kFetchLastStableSince = 4;
constexpr std::int16_t kFetchPreferredReplicaSince = 11;

/// Decodifica y descarta `forgotten_topics_data` (no usamos sesiones con estado).
[[nodiscard]] expected<void> skip_forgotten_topics(Decoder& dec, bool flexible) {
    const expected<std::int32_t> count =
        flexible ? dec.get_compact_array_len() : dec.get_array_len();
    if (!count) {
        return std::unexpected(count.error());
    }
    for (std::int32_t i = 0; i < *count; ++i) {
        const expected<std::string> topic = flexible ? dec.get_compact_string() : dec.get_string();
        if (!topic) {
            return std::unexpected(topic.error());
        }
        const expected<std::int32_t> parts =
            flexible ? dec.get_compact_array_len() : dec.get_array_len();
        if (!parts) {
            return std::unexpected(parts.error());
        }
        for (std::int32_t p = 0; p < *parts; ++p) {
            const expected<std::int32_t> idx = dec.get_i32();
            if (!idx) {
                return std::unexpected(idx.error());
            }
        }
        if (flexible) {
            const expected<void> tags = dec.skip_tagged_fields();
            if (!tags) {
                return std::unexpected(tags.error());
            }
        }
    }
    return {};
}

[[nodiscard]] expected<FetchPartitionRequest> decode_partition(Decoder& dec, std::int16_t version,
                                                               bool flexible) {
    FetchPartitionRequest part;
    const expected<std::int32_t> partition = dec.get_i32();
    if (!partition) {
        return std::unexpected(partition.error());
    }
    part.partition = *partition;
    if (version >= kFetchLeaderEpochSince) {
        const expected<std::int32_t> leader_epoch = dec.get_i32();
        if (!leader_epoch) {
            return std::unexpected(leader_epoch.error());
        }
        part.current_leader_epoch = *leader_epoch;
    }
    const expected<std::int64_t> fetch_offset = dec.get_i64();
    if (!fetch_offset) {
        return std::unexpected(fetch_offset.error());
    }
    part.fetch_offset = *fetch_offset;
    if (version >= kFetchLastFetchedEpochSince) {
        const expected<std::int32_t> last_fetched = dec.get_i32();
        if (!last_fetched) {
            return std::unexpected(last_fetched.error());
        }
        part.last_fetched_epoch = *last_fetched;
    }
    if (version >= kFetchLogStartSince) {
        const expected<std::int64_t> log_start = dec.get_i64();
        if (!log_start) {
            return std::unexpected(log_start.error());
        }
        part.log_start_offset = *log_start;
    }
    const expected<std::int32_t> max_part_bytes = dec.get_i32();
    if (!max_part_bytes) {
        return std::unexpected(max_part_bytes.error());
    }
    part.partition_max_bytes = *max_part_bytes;
    if (flexible) {
        const expected<void> tags = dec.skip_tagged_fields();
        if (!tags) {
            return std::unexpected(tags.error());
        }
    }
    return part;
}

/// Decodifica la cabecera de Fetch (replica/wait/min_bytes y los campos con gate por versión:
/// max_bytes v3+, isolation_level v4+, session_id/epoch v7+) sobre @p req.
[[nodiscard]] expected<void> decode_fetch_header(Decoder& dec, std::int16_t version,
                                                 FetchRequest& req) {
    const auto replica_id = dec.get_i32();
    const auto max_wait = dec.get_i32();
    const auto min_bytes = dec.get_i32();
    if (!replica_id || !max_wait || !min_bytes) {
        return make_error(ErrorCode::InvalidArgument, "Fetch request truncada (cabecera)");
    }
    req.replica_id = *replica_id;
    req.max_wait_ms = *max_wait;
    req.min_bytes = *min_bytes;
    if (version >= kFetchMaxBytesSince) {
        const auto max_bytes = dec.get_i32();
        if (!max_bytes) {
            return std::unexpected(max_bytes.error());
        }
        req.max_bytes = *max_bytes;
    }
    if (version >= kFetchIsolationSince) {
        const auto isolation = dec.get_i8();
        if (!isolation) {
            return std::unexpected(isolation.error());
        }
        req.isolation_level = *isolation;
    }
    if (version >= kFetchSessionSince) {
        const auto session_id = dec.get_i32();
        const auto session_epoch = dec.get_i32();
        if (!session_id || !session_epoch) {
            return std::unexpected(session_id ? session_epoch.error() : session_id.error());
        }
        req.session_id = *session_id;
        req.session_epoch = *session_epoch;
    }
    return {};
}

/// Decodifica un topic de Fetch (nombre + array de particiones).
[[nodiscard]] expected<FetchTopicRequest> decode_topic(Decoder& dec, std::int16_t version,
                                                       bool flexible) {
    FetchTopicRequest topic;
    expected<std::string> name = flexible ? dec.get_compact_string() : dec.get_string();
    if (!name) {
        return std::unexpected(name.error());
    }
    topic.topic = std::move(*name);

    const expected<std::int32_t> part_count =
        flexible ? dec.get_compact_array_len() : dec.get_array_len();
    if (!part_count) {
        return std::unexpected(part_count.error());
    }
    for (std::int32_t p = 0; p < *part_count; ++p) {
        expected<FetchPartitionRequest> part = decode_partition(dec, version, flexible);
        if (!part) {
            return std::unexpected(part.error());
        }
        topic.partitions.push_back(*part);
    }
    if (flexible) {
        const expected<void> tags = dec.skip_tagged_fields();
        if (!tags) {
            return std::unexpected(tags.error());
        }
    }
    return topic;
}

/// Decodifica el bloque final de Fetch: forgotten_topics (v7+), rack_id (v11+) y tagged fields.
[[nodiscard]] expected<void> decode_fetch_trailer(Decoder& dec, std::int16_t version, bool flexible,
                                                  FetchRequest& req) {
    if (version >= kFetchForgottenSince) {
        const expected<void> forgotten = skip_forgotten_topics(dec, flexible);
        if (!forgotten) {
            return std::unexpected(forgotten.error());
        }
    }
    if (version >= kFetchRackSince) {
        expected<std::string> rack = flexible ? dec.get_compact_string() : dec.get_string();
        if (!rack) {
            return std::unexpected(rack.error());
        }
        req.rack_id = std::move(*rack);
    }
    if (flexible) {
        const expected<void> tags = dec.skip_tagged_fields();  // tagged fields del cuerpo
        if (!tags) {
            return std::unexpected(tags.error());
        }
    }
    return {};
}

}  // namespace

expected<FetchRequest> decode_fetch_request(Decoder& dec, std::int16_t api_version) {
    const bool flexible = is_flexible(ApiKey::Fetch, api_version);
    FetchRequest req;
    if (const expected<void> header = decode_fetch_header(dec, api_version, req); !header) {
        return std::unexpected(header.error());
    }

    const expected<std::int32_t> topic_count =
        flexible ? dec.get_compact_array_len() : dec.get_array_len();
    if (!topic_count) {
        return std::unexpected(topic_count.error());
    }
    for (std::int32_t t = 0; t < *topic_count; ++t) {
        expected<FetchTopicRequest> topic = decode_topic(dec, api_version, flexible);
        if (!topic) {
            return std::unexpected(topic.error());
        }
        req.topics.push_back(std::move(*topic));
    }

    if (const expected<void> trailer = decode_fetch_trailer(dec, api_version, flexible, req);
        !trailer) {
        return std::unexpected(trailer.error());
    }
    return req;
}

namespace {

/// Serializa una partición de la respuesta Fetch según @p version y el modo @p flexible.
void encode_partition_response(Encoder& enc, const FetchPartitionResponse& part,
                               std::int16_t version, bool flexible) {
    enc.put_i32(part.partition_index);
    enc.put_i16(part.error_code);
    enc.put_i64(part.high_watermark);
    if (version >= kFetchLastStableSince) {
        enc.put_i64(part.last_stable_offset);
    }
    if (version >= kFetchLogStartSince) {
        enc.put_i64(part.log_start_offset);
    }
    if (version >= kFetchLastStableSince) {
        flexible ? enc.put_compact_array_len(0)
                 : enc.put_array_len(0);  // aborted_transactions (vacío); presente desde v4
    }
    if (version >= kFetchPreferredReplicaSince) {
        enc.put_i32(part.preferred_read_replica);
    }
    // Records SIEMPRE presentes (nunca `null`): cuando no hay datos —el consumidor alcanzó el
    // high-watermark— se envía un MessageSet **vacío** (longitud 0). librdkafka rechaza un
    // `MessageSetSize` de -1 como trama corrupta.
    const std::optional<ByteSpan> records{ByteSpan{part.records}};
    flexible ? enc.put_compact_nullable_bytes(records) : enc.put_nullable_bytes(records);
    if (flexible) {
        enc.put_empty_tagged_fields();
    }
}

}  // namespace

void encode_fetch_response(Encoder& enc, const FetchResponse& resp, std::int16_t api_version) {
    const bool flexible = is_flexible(ApiKey::Fetch, api_version);
    const auto array_len = [&](std::int32_t count) {
        flexible ? enc.put_compact_array_len(count) : enc.put_array_len(count);
    };

    if (api_version >= kFetchThrottleSince) {
        enc.put_i32(resp.throttle_time_ms);
    }
    if (api_version >= kFetchTopLevelErrorSince) {
        enc.put_i16(resp.error_code);
        enc.put_i32(resp.session_id);
    }

    array_len(static_cast<std::int32_t>(resp.topics.size()));
    for (const FetchTopicResponse& topic : resp.topics) {
        flexible ? enc.put_compact_string(topic.topic) : enc.put_string(topic.topic);
        array_len(static_cast<std::int32_t>(topic.partitions.size()));
        for (const FetchPartitionResponse& part : topic.partitions) {
            encode_partition_response(enc, part, api_version, flexible);
        }
        if (flexible) {
            enc.put_empty_tagged_fields();
        }
    }
    if (flexible) {
        enc.put_empty_tagged_fields();
    }
}

}  // namespace nexus::kafka
