/// @file   kafka/fetch.cpp
/// @brief  Implementación de la API Fetch del subconjunto Kafka (v12 flexible) — F7d.
/// @ingroup kafka

#include "kafka/fetch.hpp"

#include <utility>

namespace nexus::kafka {
namespace {

/// Decodifica y descarta `forgotten_topics_data` (no lo usamos en esta sesión sin estado).
[[nodiscard]] expected<void> skip_forgotten_topics(Decoder& dec) {
    const expected<std::int32_t> count = dec.get_compact_array_len();
    if (!count) {
        return std::unexpected(count.error());
    }
    for (std::int32_t i = 0; i < *count; ++i) {
        const expected<std::string> topic = dec.get_compact_string();
        if (!topic) {
            return std::unexpected(topic.error());
        }
        const expected<std::int32_t> parts = dec.get_compact_array_len();
        if (!parts) {
            return std::unexpected(parts.error());
        }
        for (std::int32_t p = 0; p < *parts; ++p) {
            const expected<std::int32_t> idx = dec.get_i32();
            if (!idx) {
                return std::unexpected(idx.error());
            }
        }
        const expected<void> tags = dec.skip_tagged_fields();
        if (!tags) {
            return std::unexpected(tags.error());
        }
    }
    return {};
}

}  // namespace

expected<FetchRequest> decode_fetch_request(Decoder& dec) {
    FetchRequest req;
    const auto replica_id = dec.get_i32();
    const auto max_wait = dec.get_i32();
    const auto min_bytes = dec.get_i32();
    const auto max_bytes = dec.get_i32();
    const auto isolation = dec.get_i8();
    const auto session_id = dec.get_i32();
    const auto session_epoch = dec.get_i32();
    if (!replica_id || !max_wait || !min_bytes || !max_bytes || !isolation || !session_id ||
        !session_epoch) {
        return make_error(ErrorCode::InvalidArgument,
                          "Fetch request truncada (cabecera de campos)");
    }
    req.replica_id = *replica_id;
    req.max_wait_ms = *max_wait;
    req.min_bytes = *min_bytes;
    req.max_bytes = *max_bytes;
    req.isolation_level = *isolation;
    req.session_id = *session_id;
    req.session_epoch = *session_epoch;

    const expected<std::int32_t> topic_count = dec.get_compact_array_len();
    if (!topic_count) {
        return std::unexpected(topic_count.error());
    }
    for (std::int32_t t = 0; t < *topic_count; ++t) {
        FetchTopicRequest topic;
        expected<std::string> name = dec.get_compact_string();
        if (!name) {
            return std::unexpected(name.error());
        }
        topic.topic = std::move(*name);

        const expected<std::int32_t> part_count = dec.get_compact_array_len();
        if (!part_count) {
            return std::unexpected(part_count.error());
        }
        for (std::int32_t p = 0; p < *part_count; ++p) {
            FetchPartitionRequest part;
            const auto partition = dec.get_i32();
            const auto leader_epoch = dec.get_i32();
            const auto fetch_offset = dec.get_i64();
            const auto last_fetched = dec.get_i32();
            const auto log_start = dec.get_i64();
            const auto max_part_bytes = dec.get_i32();
            if (!partition || !leader_epoch || !fetch_offset || !last_fetched || !log_start ||
                !max_part_bytes) {
                return make_error(ErrorCode::InvalidArgument, "Fetch partition truncada");
            }
            part.partition = *partition;
            part.current_leader_epoch = *leader_epoch;
            part.fetch_offset = *fetch_offset;
            part.last_fetched_epoch = *last_fetched;
            part.log_start_offset = *log_start;
            part.partition_max_bytes = *max_part_bytes;
            const expected<void> tags = dec.skip_tagged_fields();
            if (!tags) {
                return std::unexpected(tags.error());
            }
            topic.partitions.push_back(part);
        }
        const expected<void> tags = dec.skip_tagged_fields();
        if (!tags) {
            return std::unexpected(tags.error());
        }
        req.topics.push_back(std::move(topic));
    }

    const expected<void> forgotten = skip_forgotten_topics(dec);
    if (!forgotten) {
        return std::unexpected(forgotten.error());
    }
    expected<std::string> rack = dec.get_compact_string();
    if (!rack) {
        return std::unexpected(rack.error());
    }
    req.rack_id = std::move(*rack);
    const expected<void> tags = dec.skip_tagged_fields();  // tagged fields del cuerpo
    if (!tags) {
        return std::unexpected(tags.error());
    }
    return req;
}

void encode_fetch_response(Encoder& enc, const FetchResponse& resp) {
    enc.put_i32(resp.throttle_time_ms);
    enc.put_i16(resp.error_code);
    enc.put_i32(resp.session_id);

    enc.put_compact_array_len(static_cast<std::int32_t>(resp.topics.size()));
    for (const FetchTopicResponse& topic : resp.topics) {
        enc.put_compact_string(topic.topic);
        enc.put_compact_array_len(static_cast<std::int32_t>(topic.partitions.size()));
        for (const FetchPartitionResponse& part : topic.partitions) {
            enc.put_i32(part.partition_index);
            enc.put_i16(part.error_code);
            enc.put_i64(part.high_watermark);
            enc.put_i64(part.last_stable_offset);
            enc.put_i64(part.log_start_offset);
            enc.put_compact_array_len(0);  // aborted_transactions (vacío)
            enc.put_i32(part.preferred_read_replica);
            if (part.records.empty()) {
                enc.put_compact_nullable_bytes(std::nullopt);
            } else {
                enc.put_compact_nullable_bytes(std::optional<ByteSpan>{ByteSpan{part.records}});
            }
            enc.put_empty_tagged_fields();
        }
        enc.put_empty_tagged_fields();
    }
    enc.put_empty_tagged_fields();
}

}  // namespace nexus::kafka
