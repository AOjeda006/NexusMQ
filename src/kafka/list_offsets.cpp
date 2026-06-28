/// @file   kafka/list_offsets.cpp
/// @brief  Implementación de la API ListOffsets del subconjunto Kafka (v7 flexible) — F7f.
/// @ingroup kafka

#include "kafka/list_offsets.hpp"

#include <utility>

namespace nexus::kafka {

expected<ListOffsetsRequest> decode_list_offsets_request(Decoder& dec) {
    ListOffsetsRequest req;

    const expected<std::int32_t> replica_id = dec.get_i32();
    if (!replica_id) {
        return std::unexpected(replica_id.error());
    }
    req.replica_id = *replica_id;

    const expected<std::int8_t> isolation_level = dec.get_i8();
    if (!isolation_level) {
        return std::unexpected(isolation_level.error());
    }
    req.isolation_level = *isolation_level;

    const expected<std::int32_t> topic_count = dec.get_compact_array_len();
    if (!topic_count) {
        return std::unexpected(topic_count.error());
    }
    for (std::int32_t t = 0; t < *topic_count; ++t) {
        ListOffsetTopic topic;
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
            ListOffsetPartition part;
            const expected<std::int32_t> index = dec.get_i32();
            if (!index) {
                return std::unexpected(index.error());
            }
            const expected<std::int32_t> leader_epoch = dec.get_i32();
            if (!leader_epoch) {
                return std::unexpected(leader_epoch.error());
            }
            const expected<std::int64_t> timestamp = dec.get_i64();
            if (!timestamp) {
                return std::unexpected(timestamp.error());
            }
            part.partition_index = *index;
            part.current_leader_epoch = *leader_epoch;
            part.timestamp = *timestamp;
            const expected<void> part_tags = dec.skip_tagged_fields();
            if (!part_tags) {
                return std::unexpected(part_tags.error());
            }
            topic.partitions.push_back(part);
        }
        const expected<void> topic_tags = dec.skip_tagged_fields();
        if (!topic_tags) {
            return std::unexpected(topic_tags.error());
        }
        req.topics.push_back(std::move(topic));
    }

    const expected<void> body_tags = dec.skip_tagged_fields();
    if (!body_tags) {
        return std::unexpected(body_tags.error());
    }
    return req;
}

void encode_list_offsets_response(Encoder& enc, const ListOffsetsResponse& resp) {
    enc.put_i32(resp.throttle_time_ms);
    enc.put_compact_array_len(static_cast<std::int32_t>(resp.topics.size()));
    for (const ListOffsetTopicResponse& topic : resp.topics) {
        enc.put_compact_string(topic.name);
        enc.put_compact_array_len(static_cast<std::int32_t>(topic.partitions.size()));
        for (const ListOffsetPartitionResponse& part : topic.partitions) {
            enc.put_i32(part.partition_index);
            enc.put_i16(part.error_code);
            enc.put_i64(part.timestamp);
            enc.put_i64(part.offset);
            enc.put_i32(part.leader_epoch);
            enc.put_empty_tagged_fields();
        }
        enc.put_empty_tagged_fields();
    }
    enc.put_empty_tagged_fields();
}

}  // namespace nexus::kafka
