/// @file   kafka/metadata.cpp
/// @brief  Implementación de la API Metadata del subconjunto Kafka (v9 flexible) — F7c.
/// @ingroup kafka

#include "kafka/metadata.hpp"

#include <utility>

namespace nexus::kafka {
namespace {

/// Escribe un `COMPACT_ARRAY` de INT32 (réplicas/ISR/offline) en @p enc.
void put_int32_array(Encoder& enc, const std::vector<std::int32_t>& values) {
    enc.put_compact_array_len(static_cast<std::int32_t>(values.size()));
    for (const std::int32_t value : values) {
        enc.put_i32(value);
    }
}

}  // namespace

expected<MetadataRequest> decode_metadata_request(Decoder& dec) {
    MetadataRequest req;

    const expected<std::int32_t> topic_count = dec.get_compact_array_len();
    if (!topic_count) {
        return std::unexpected(topic_count.error());
    }
    if (*topic_count >= 0) {
        std::vector<std::string> topics;
        topics.reserve(static_cast<std::size_t>(*topic_count));
        for (std::int32_t i = 0; i < *topic_count; ++i) {
            expected<std::string> name = dec.get_compact_string();
            if (!name) {
                return std::unexpected(name.error());
            }
            topics.push_back(std::move(*name));
            const expected<void> tags = dec.skip_tagged_fields();  // tagged fields por topic
            if (!tags) {
                return std::unexpected(tags.error());
            }
        }
        req.topics = std::move(topics);
    }

    const expected<bool> auto_create = dec.get_bool();
    if (!auto_create) {
        return std::unexpected(auto_create.error());
    }
    const expected<bool> include_cluster = dec.get_bool();
    if (!include_cluster) {
        return std::unexpected(include_cluster.error());
    }
    const expected<bool> include_topic = dec.get_bool();
    if (!include_topic) {
        return std::unexpected(include_topic.error());
    }
    req.allow_auto_topic_creation = *auto_create;
    req.include_cluster_authorized_operations = *include_cluster;
    req.include_topic_authorized_operations = *include_topic;

    const expected<void> tags = dec.skip_tagged_fields();  // tagged fields del cuerpo
    if (!tags) {
        return std::unexpected(tags.error());
    }
    return req;
}

void encode_metadata_response(Encoder& enc, const MetadataResponse& resp) {
    enc.put_i32(resp.throttle_time_ms);

    enc.put_compact_array_len(static_cast<std::int32_t>(resp.brokers.size()));
    for (const MetadataBroker& broker : resp.brokers) {
        enc.put_i32(broker.node_id);
        enc.put_compact_string(broker.host);
        enc.put_i32(broker.port);
        enc.put_compact_nullable_string(broker.rack);
        enc.put_empty_tagged_fields();
    }

    enc.put_compact_nullable_string(resp.cluster_id);
    enc.put_i32(resp.controller_id);

    enc.put_compact_array_len(static_cast<std::int32_t>(resp.topics.size()));
    for (const MetadataTopic& topic : resp.topics) {
        enc.put_i16(topic.error_code);
        enc.put_compact_string(topic.name);
        enc.put_bool(topic.is_internal);
        enc.put_compact_array_len(static_cast<std::int32_t>(topic.partitions.size()));
        for (const MetadataPartition& part : topic.partitions) {
            enc.put_i16(part.error_code);
            enc.put_i32(part.partition_index);
            enc.put_i32(part.leader_id);
            enc.put_i32(part.leader_epoch);
            put_int32_array(enc, part.replica_nodes);
            put_int32_array(enc, part.isr_nodes);
            put_int32_array(enc, part.offline_replicas);
            enc.put_empty_tagged_fields();
        }
        enc.put_i32(topic.topic_authorized_operations);
        enc.put_empty_tagged_fields();
    }

    enc.put_i32(resp.cluster_authorized_operations);
    enc.put_empty_tagged_fields();
}

}  // namespace nexus::kafka
