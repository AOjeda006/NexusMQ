/// @file   kafka/produce.cpp
/// @brief  Implementación de la API Produce del subconjunto Kafka (v9 flexible) — F7d.
/// @ingroup kafka

#include "kafka/produce.hpp"

#include <utility>

namespace nexus::kafka {
namespace {

/// Decodifica una partición de Produce (índice + records `COMPACT_RECORDS` + tagged fields).
[[nodiscard]] expected<ProducePartitionData> decode_partition(Decoder& dec) {
    ProducePartitionData part;
    const expected<std::int32_t> index = dec.get_i32();
    if (!index) {
        return std::unexpected(index.error());
    }
    part.index = *index;
    // records: COMPACT_RECORDS (= COMPACT_NULLABLE_BYTES); el blob es opaco al codec.
    const expected<std::optional<ByteSpan>> blob = dec.get_compact_nullable_bytes();
    if (!blob) {
        return std::unexpected(blob.error());
    }
    if (*blob) {
        part.records.assign((*blob)->begin(), (*blob)->end());
    }
    const expected<void> tags = dec.skip_tagged_fields();
    if (!tags) {
        return std::unexpected(tags.error());
    }
    return part;
}

/// Decodifica un topic de Produce (nombre + array de particiones + tagged fields).
[[nodiscard]] expected<ProduceTopicData> decode_topic(Decoder& dec) {
    ProduceTopicData topic;
    expected<std::string> name = dec.get_compact_string();
    if (!name) {
        return std::unexpected(name.error());
    }
    topic.name = std::move(*name);

    const expected<std::int32_t> part_count = dec.get_compact_array_len();
    if (!part_count) {
        return std::unexpected(part_count.error());
    }
    for (std::int32_t p = 0; p < *part_count; ++p) {
        expected<ProducePartitionData> part = decode_partition(dec);
        if (!part) {
            return std::unexpected(part.error());
        }
        topic.partitions.push_back(std::move(*part));
    }
    const expected<void> tags = dec.skip_tagged_fields();
    if (!tags) {
        return std::unexpected(tags.error());
    }
    return topic;
}

}  // namespace

expected<ProduceRequest> decode_produce_request(Decoder& dec) {
    ProduceRequest req;

    expected<std::optional<std::string>> txn_id = dec.get_compact_nullable_string();
    if (!txn_id) {
        return std::unexpected(txn_id.error());
    }
    req.transactional_id = std::move(*txn_id);

    const expected<std::int16_t> acks = dec.get_i16();
    if (!acks) {
        return std::unexpected(acks.error());
    }
    const expected<std::int32_t> timeout = dec.get_i32();
    if (!timeout) {
        return std::unexpected(timeout.error());
    }
    req.acks = *acks;
    req.timeout_ms = *timeout;

    const expected<std::int32_t> topic_count = dec.get_compact_array_len();
    if (!topic_count) {
        return std::unexpected(topic_count.error());
    }
    for (std::int32_t t = 0; t < *topic_count; ++t) {
        expected<ProduceTopicData> topic = decode_topic(dec);
        if (!topic) {
            return std::unexpected(topic.error());
        }
        req.topics.push_back(std::move(*topic));
    }
    const expected<void> tags = dec.skip_tagged_fields();  // tagged fields del cuerpo
    if (!tags) {
        return std::unexpected(tags.error());
    }
    return req;
}

void encode_produce_response(Encoder& enc, const ProduceResponse& resp) {
    enc.put_compact_array_len(static_cast<std::int32_t>(resp.topics.size()));
    for (const ProduceTopicResponse& topic : resp.topics) {
        enc.put_compact_string(topic.name);
        enc.put_compact_array_len(static_cast<std::int32_t>(topic.partitions.size()));
        for (const ProducePartitionResponse& part : topic.partitions) {
            enc.put_i32(part.index);
            enc.put_i16(part.error_code);
            enc.put_i64(part.base_offset);
            enc.put_i64(part.log_append_time_ms);
            enc.put_i64(part.log_start_offset);
            enc.put_compact_array_len(0);  // record_errors (vacío)
            enc.put_compact_nullable_string(part.error_message);
            enc.put_empty_tagged_fields();
        }
        enc.put_empty_tagged_fields();
    }
    enc.put_i32(resp.throttle_time_ms);
    enc.put_empty_tagged_fields();
}

}  // namespace nexus::kafka
