/// @file   kafka/produce.cpp
/// @brief  Implementación de la API Produce del subconjunto Kafka (clásica v3..v8 y flexible v9).
/// @ingroup kafka

#include "kafka/produce.hpp"

#include <string_view>
#include <utility>

#include "kafka/messages.hpp"

namespace nexus::kafka {
namespace {

/// Versión a partir de la cual Produce incluye `transactional_id` (v3), `log_append_time` (v2),
/// `log_start_offset` (v5) y `record_errors`/`error_message` (v8). Los gates espejan el protocolo.
constexpr std::int16_t kProduceTxnSince = 3;
constexpr std::int16_t kProduceLogAppendSince = 2;
constexpr std::int16_t kProduceLogStartSince = 5;
constexpr std::int16_t kProduceRecordErrorsSince = 8;
constexpr std::int16_t kProduceThrottleSince = 1;

/// Decodifica una partición de Produce (índice + records; `RECORDS` clásico o `COMPACT_RECORDS`).
[[nodiscard]] expected<ProducePartitionData> decode_partition(Decoder& dec, bool flexible) {
    ProducePartitionData part;
    const expected<std::int32_t> index = dec.get_i32();
    if (!index) {
        return std::unexpected(index.error());
    }
    part.index = *index;
    const expected<std::optional<ByteSpan>> blob =
        flexible ? dec.get_compact_nullable_bytes() : dec.get_nullable_bytes();
    if (!blob) {
        return std::unexpected(blob.error());
    }
    const std::optional<ByteSpan>& maybe = *blob;  // *blob: expected validado arriba.
    if (maybe.has_value()) {
        const ByteSpan span = *maybe;
        part.records.assign(span.begin(), span.end());
    }
    if (flexible) {
        const expected<void> tags = dec.skip_tagged_fields();
        if (!tags) {
            return std::unexpected(tags.error());
        }
    }
    return part;
}

/// Decodifica un topic de Produce (nombre + array de particiones).
[[nodiscard]] expected<ProduceTopicData> decode_topic(Decoder& dec, bool flexible) {
    ProduceTopicData topic;
    expected<std::string> name = flexible ? dec.get_compact_string() : dec.get_string();
    if (!name) {
        return std::unexpected(name.error());
    }
    topic.name = std::move(*name);

    const expected<std::int32_t> part_count =
        flexible ? dec.get_compact_array_len() : dec.get_array_len();
    if (!part_count) {
        return std::unexpected(part_count.error());
    }
    for (std::int32_t p = 0; p < *part_count; ++p) {
        expected<ProducePartitionData> part = decode_partition(dec, flexible);
        if (!part) {
            return std::unexpected(part.error());
        }
        topic.partitions.push_back(std::move(*part));
    }
    if (flexible) {
        const expected<void> tags = dec.skip_tagged_fields();
        if (!tags) {
            return std::unexpected(tags.error());
        }
    }
    return topic;
}

/// Serializa una partición de la respuesta Produce según @p api_version y el modo @p flexible.
void encode_partition_response(Encoder& enc, const ProducePartitionResponse& part,
                               std::int16_t api_version, bool flexible) {
    enc.put_i32(part.index);
    enc.put_i16(part.error_code);
    enc.put_i64(part.base_offset);
    if (api_version >= kProduceLogAppendSince) {
        enc.put_i64(part.log_append_time_ms);
    }
    if (api_version >= kProduceLogStartSince) {
        enc.put_i64(part.log_start_offset);
    }
    if (api_version >= kProduceRecordErrorsSince) {
        flexible ? enc.put_compact_array_len(0) : enc.put_array_len(0);  // record_errors (vacío)
        flexible ? enc.put_compact_nullable_string(part.error_message)
                 : enc.put_nullable_string(part.error_message);
    }
    if (flexible) {
        enc.put_empty_tagged_fields();
    }
}

}  // namespace

expected<ProduceRequest> decode_produce_request(Decoder& dec, std::int16_t api_version) {
    const bool flexible = is_flexible(ApiKey::Produce, api_version);
    ProduceRequest req;

    if (api_version >= kProduceTxnSince) {
        expected<std::optional<std::string>> txn_id =
            flexible ? dec.get_compact_nullable_string() : dec.get_nullable_string();
        if (!txn_id) {
            return std::unexpected(txn_id.error());
        }
        req.transactional_id = std::move(*txn_id);
    }

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

    const expected<std::int32_t> topic_count =
        flexible ? dec.get_compact_array_len() : dec.get_array_len();
    if (!topic_count) {
        return std::unexpected(topic_count.error());
    }
    for (std::int32_t t = 0; t < *topic_count; ++t) {
        expected<ProduceTopicData> topic = decode_topic(dec, flexible);
        if (!topic) {
            return std::unexpected(topic.error());
        }
        req.topics.push_back(std::move(*topic));
    }
    if (flexible) {
        const expected<void> tags = dec.skip_tagged_fields();  // tagged fields del cuerpo
        if (!tags) {
            return std::unexpected(tags.error());
        }
    }
    return req;
}

void encode_produce_response(Encoder& enc, const ProduceResponse& resp, std::int16_t api_version) {
    const bool flexible = is_flexible(ApiKey::Produce, api_version);
    const auto array_len = [&](std::int32_t count) {
        flexible ? enc.put_compact_array_len(count) : enc.put_array_len(count);
    };

    array_len(static_cast<std::int32_t>(resp.topics.size()));
    for (const ProduceTopicResponse& topic : resp.topics) {
        flexible ? enc.put_compact_string(topic.name) : enc.put_string(topic.name);
        array_len(static_cast<std::int32_t>(topic.partitions.size()));
        for (const ProducePartitionResponse& part : topic.partitions) {
            encode_partition_response(enc, part, api_version, flexible);
        }
        if (flexible) {
            enc.put_empty_tagged_fields();
        }
    }
    if (api_version >= kProduceThrottleSince) {
        enc.put_i32(resp.throttle_time_ms);
    }
    if (flexible) {
        enc.put_empty_tagged_fields();
    }
}

}  // namespace nexus::kafka
