#include "protocol/messages.hpp"

#include <cstddef>
#include <string>

#include "protocol/codec.hpp"
#include "protocol/frame.hpp"

namespace nexus {
namespace {

// Lee un contador de elementos y lo acota: no puede haber más elementos que bytes restantes
// (cada elemento ocupa >= 1 byte), evitando reservar memoria por un contador malicioso.
[[nodiscard]] expected<std::size_t> get_count(Decoder& dec) {
    auto count = dec.get_varint();
    if (!count) {
        return std::unexpected(count.error());
    }
    if (*count > dec.remaining()) {
        return make_error(ErrorCode::InvalidArgument, "contador de elementos excede el buffer");
    }
    return static_cast<std::size_t>(*count);
}

}  // namespace

void ApiVersionsRequest::encode(Encoder& enc) const {
    enc.put_u16(client_version);
}

expected<ApiVersionsRequest> ApiVersionsRequest::decode(Decoder& dec) {
    auto client_version = dec.get_u16();
    if (!client_version) {
        return std::unexpected(client_version.error());
    }
    return ApiVersionsRequest{.client_version = *client_version};
}

void ApiVersionsResponse::encode(Encoder& enc) const {
    enc.put_varint(ranges.size());
    for (const ApiVersionRange& range : ranges) {
        enc.put_u16(static_cast<std::uint16_t>(range.key));
        enc.put_u16(range.min);
        enc.put_u16(range.max);
    }
}

expected<ApiVersionsResponse> ApiVersionsResponse::decode(Decoder& dec) {
    auto count = get_count(dec);
    if (!count) {
        return std::unexpected(count.error());
    }
    ApiVersionsResponse response;
    response.ranges.reserve(*count);
    for (std::size_t i = 0; i < *count; ++i) {
        auto key = dec.get_u16();
        if (!key) {
            return std::unexpected(key.error());
        }
        if (*key > static_cast<std::uint16_t>(ApiKey::DeleteTopic)) {
            return make_error(ErrorCode::InvalidArgument, "api_key desconocido en ApiVersions");
        }
        auto min = dec.get_u16();
        if (!min) {
            return std::unexpected(min.error());
        }
        auto max = dec.get_u16();
        if (!max) {
            return std::unexpected(max.error());
        }
        response.ranges.push_back(
            ApiVersionRange{.key = static_cast<ApiKey>(*key), .min = *min, .max = *max});
    }
    return response;
}

void BrokerMeta::encode(Encoder& enc) const {
    enc.put_i32(node_id);
    enc.put_string(host);
    enc.put_u16(port);
}

expected<BrokerMeta> BrokerMeta::decode(Decoder& dec) {
    auto node_id = dec.get_i32();
    if (!node_id) {
        return std::unexpected(node_id.error());
    }
    auto host = dec.get_string();
    if (!host) {
        return std::unexpected(host.error());
    }
    auto port = dec.get_u16();
    if (!port) {
        return std::unexpected(port.error());
    }
    return BrokerMeta{.node_id = *node_id, .host = std::string{*host}, .port = *port};
}

void PartitionMeta::encode(Encoder& enc) const {
    enc.put_i32(id);
    enc.put_i32(leader_node_id);
    enc.put_varint(replicas.size());
    for (const NodeId replica : replicas) {
        enc.put_i32(replica);
    }
    enc.put_i32(leader_epoch);
}

expected<PartitionMeta> PartitionMeta::decode(Decoder& dec) {
    auto id = dec.get_i32();
    if (!id) {
        return std::unexpected(id.error());
    }
    auto leader = dec.get_i32();
    if (!leader) {
        return std::unexpected(leader.error());
    }
    auto count = get_count(dec);
    if (!count) {
        return std::unexpected(count.error());
    }
    PartitionMeta meta;
    meta.id = *id;
    meta.leader_node_id = *leader;
    meta.replicas.reserve(*count);
    for (std::size_t i = 0; i < *count; ++i) {
        auto replica = dec.get_i32();
        if (!replica) {
            return std::unexpected(replica.error());
        }
        meta.replicas.push_back(*replica);
    }
    auto leader_epoch = dec.get_i32();
    if (!leader_epoch) {
        return std::unexpected(leader_epoch.error());
    }
    meta.leader_epoch = *leader_epoch;
    return meta;
}

void TopicMeta::encode(Encoder& enc) const {
    enc.put_string(name);
    enc.put_i16(static_cast<std::int16_t>(error));
    enc.put_varint(partitions.size());
    for (const PartitionMeta& partition : partitions) {
        partition.encode(enc);
    }
}

expected<TopicMeta> TopicMeta::decode(Decoder& dec) {
    auto name = dec.get_string();
    if (!name) {
        return std::unexpected(name.error());
    }
    auto error = dec.get_i16();
    if (!error) {
        return std::unexpected(error.error());
    }
    auto count = get_count(dec);
    if (!count) {
        return std::unexpected(count.error());
    }
    TopicMeta meta;
    meta.name = std::string{*name};
    meta.error = static_cast<WireError>(*error);
    meta.partitions.reserve(*count);
    for (std::size_t i = 0; i < *count; ++i) {
        auto partition = PartitionMeta::decode(dec);
        if (!partition) {
            return std::unexpected(partition.error());
        }
        meta.partitions.push_back(*partition);
    }
    return meta;
}

void MetadataRequest::encode(Encoder& enc) const {
    enc.put_varint(topics.size());
    for (const std::string& topic : topics) {
        enc.put_string(topic);
    }
}

expected<MetadataRequest> MetadataRequest::decode(Decoder& dec) {
    auto count = get_count(dec);
    if (!count) {
        return std::unexpected(count.error());
    }
    MetadataRequest request;
    request.topics.reserve(*count);
    for (std::size_t i = 0; i < *count; ++i) {
        auto topic = dec.get_string();
        if (!topic) {
            return std::unexpected(topic.error());
        }
        request.topics.emplace_back(*topic);
    }
    return request;
}

void MetadataResponse::encode(Encoder& enc) const {
    enc.put_varint(brokers.size());
    for (const BrokerMeta& broker : brokers) {
        broker.encode(enc);
    }
    enc.put_varint(topics.size());
    for (const TopicMeta& topic : topics) {
        topic.encode(enc);
    }
}

expected<MetadataResponse> MetadataResponse::decode(Decoder& dec) {
    auto broker_count = get_count(dec);
    if (!broker_count) {
        return std::unexpected(broker_count.error());
    }
    MetadataResponse response;
    response.brokers.reserve(*broker_count);
    for (std::size_t i = 0; i < *broker_count; ++i) {
        auto broker = BrokerMeta::decode(dec);
        if (!broker) {
            return std::unexpected(broker.error());
        }
        response.brokers.push_back(*broker);
    }
    auto topic_count = get_count(dec);
    if (!topic_count) {
        return std::unexpected(topic_count.error());
    }
    response.topics.reserve(*topic_count);
    for (std::size_t i = 0; i < *topic_count; ++i) {
        auto topic = TopicMeta::decode(dec);
        if (!topic) {
            return std::unexpected(topic.error());
        }
        response.topics.push_back(*topic);
    }
    return response;
}

void ProduceRequest::encode(Encoder& enc) const {
    enc.put_string(topic);
    enc.put_i32(partition);
    enc.put_u8(static_cast<std::uint8_t>(acks));
    enc.put_bytes(batch);
}

expected<ProduceRequest> ProduceRequest::decode(Decoder& dec) {
    auto topic = dec.get_string();
    if (!topic) {
        return std::unexpected(topic.error());
    }
    auto partition = dec.get_i32();
    if (!partition) {
        return std::unexpected(partition.error());
    }
    auto acks = dec.get_u8();
    if (!acks) {
        return std::unexpected(acks.error());
    }
    if (*acks > static_cast<std::uint8_t>(Acks::Quorum)) {
        return make_error(ErrorCode::InvalidArgument, "valor de acks inválido");
    }
    auto batch = dec.get_bytes();
    if (!batch) {
        return std::unexpected(batch.error());
    }
    ProduceRequest request;
    request.topic = std::string{*topic};
    request.partition = *partition;
    request.acks = static_cast<Acks>(*acks);
    request.batch = *batch;
    return request;
}

void ProduceResponse::encode(Encoder& enc) const {
    enc.put_i64(base_offset);
    enc.put_i16(static_cast<std::int16_t>(error_code));
    enc.put_i32(throttle_ms);
}

expected<ProduceResponse> ProduceResponse::decode(Decoder& dec) {
    auto base_offset = dec.get_i64();
    if (!base_offset) {
        return std::unexpected(base_offset.error());
    }
    auto error_code = dec.get_i16();
    if (!error_code) {
        return std::unexpected(error_code.error());
    }
    auto throttle_ms = dec.get_i32();
    if (!throttle_ms) {
        return std::unexpected(throttle_ms.error());
    }
    return ProduceResponse{.base_offset = *base_offset,
                           .error_code = static_cast<WireError>(*error_code),
                           .throttle_ms = *throttle_ms};
}

void FetchRequest::encode(Encoder& enc) const {
    enc.put_string(topic);
    enc.put_i32(partition);
    enc.put_i64(fetch_offset);
    enc.put_i32(max_bytes);
    enc.put_i32(min_bytes);
    enc.put_i32(max_wait_ms);
}

expected<FetchRequest> FetchRequest::decode(Decoder& dec) {
    auto topic = dec.get_string();
    if (!topic) {
        return std::unexpected(topic.error());
    }
    auto partition = dec.get_i32();
    if (!partition) {
        return std::unexpected(partition.error());
    }
    auto fetch_offset = dec.get_i64();
    if (!fetch_offset) {
        return std::unexpected(fetch_offset.error());
    }
    auto max_bytes = dec.get_i32();
    if (!max_bytes) {
        return std::unexpected(max_bytes.error());
    }
    auto min_bytes = dec.get_i32();
    if (!min_bytes) {
        return std::unexpected(min_bytes.error());
    }
    auto max_wait_ms = dec.get_i32();
    if (!max_wait_ms) {
        return std::unexpected(max_wait_ms.error());
    }
    FetchRequest request;
    request.topic = std::string{*topic};
    request.partition = *partition;
    request.fetch_offset = *fetch_offset;
    request.max_bytes = *max_bytes;
    request.min_bytes = *min_bytes;
    request.max_wait_ms = *max_wait_ms;
    return request;
}

void FetchResponse::encode(Encoder& enc) const {
    enc.put_bytes(batches);
    enc.put_i64(high_watermark);
    enc.put_i64(log_start_offset);
    enc.put_i16(static_cast<std::int16_t>(error_code));
}

expected<FetchResponse> FetchResponse::decode(Decoder& dec) {
    auto batches = dec.get_bytes();
    if (!batches) {
        return std::unexpected(batches.error());
    }
    auto high_watermark = dec.get_i64();
    if (!high_watermark) {
        return std::unexpected(high_watermark.error());
    }
    auto log_start_offset = dec.get_i64();
    if (!log_start_offset) {
        return std::unexpected(log_start_offset.error());
    }
    auto error_code = dec.get_i16();
    if (!error_code) {
        return std::unexpected(error_code.error());
    }
    FetchResponse response;
    response.batches = *batches;
    response.high_watermark = *high_watermark;
    response.log_start_offset = *log_start_offset;
    response.error_code = static_cast<WireError>(*error_code);
    return response;
}

void CreateTopicRequest::encode(Encoder& enc) const {
    enc.put_string(name);
    enc.put_i32(partition_count);
    enc.put_i16(replication_factor);
}

expected<CreateTopicRequest> CreateTopicRequest::decode(Decoder& dec) {
    auto name = dec.get_string();
    if (!name) {
        return std::unexpected(name.error());
    }
    auto partition_count = dec.get_i32();
    if (!partition_count) {
        return std::unexpected(partition_count.error());
    }
    auto replication_factor = dec.get_i16();
    if (!replication_factor) {
        return std::unexpected(replication_factor.error());
    }
    CreateTopicRequest request;
    request.name = std::string{*name};
    request.partition_count = *partition_count;
    request.replication_factor = *replication_factor;
    return request;
}

void CreateTopicResponse::encode(Encoder& enc) const {
    enc.put_i16(static_cast<std::int16_t>(error_code));
}

expected<CreateTopicResponse> CreateTopicResponse::decode(Decoder& dec) {
    auto error_code = dec.get_i16();
    if (!error_code) {
        return std::unexpected(error_code.error());
    }
    return CreateTopicResponse{.error_code = static_cast<WireError>(*error_code)};
}

void DeleteTopicRequest::encode(Encoder& enc) const {
    enc.put_string(name);
}

expected<DeleteTopicRequest> DeleteTopicRequest::decode(Decoder& dec) {
    auto name = dec.get_string();
    if (!name) {
        return std::unexpected(name.error());
    }
    return DeleteTopicRequest{.name = std::string{*name}};
}

void DeleteTopicResponse::encode(Encoder& enc) const {
    enc.put_i16(static_cast<std::int16_t>(error_code));
}

expected<DeleteTopicResponse> DeleteTopicResponse::decode(Decoder& dec) {
    auto error_code = dec.get_i16();
    if (!error_code) {
        return std::unexpected(error_code.error());
    }
    return DeleteTopicResponse{.error_code = static_cast<WireError>(*error_code)};
}

void OffsetCommitRequest::encode(Encoder& enc) const {
    enc.put_string(group);
    enc.put_string(topic);
    enc.put_i32(partition);
    enc.put_i64(offset);
    enc.put_string(metadata);
}

expected<OffsetCommitRequest> OffsetCommitRequest::decode(Decoder& dec) {
    auto group = dec.get_string();
    if (!group) {
        return std::unexpected(group.error());
    }
    auto topic = dec.get_string();
    if (!topic) {
        return std::unexpected(topic.error());
    }
    auto partition = dec.get_i32();
    if (!partition) {
        return std::unexpected(partition.error());
    }
    auto offset = dec.get_i64();
    if (!offset) {
        return std::unexpected(offset.error());
    }
    auto metadata = dec.get_string();
    if (!metadata) {
        return std::unexpected(metadata.error());
    }
    OffsetCommitRequest request;
    request.group = std::string{*group};
    request.topic = std::string{*topic};
    request.partition = *partition;
    request.offset = *offset;
    request.metadata = std::string{*metadata};
    return request;
}

void OffsetCommitResponse::encode(Encoder& enc) const {
    enc.put_i16(static_cast<std::int16_t>(error_code));
}

expected<OffsetCommitResponse> OffsetCommitResponse::decode(Decoder& dec) {
    auto error_code = dec.get_i16();
    if (!error_code) {
        return std::unexpected(error_code.error());
    }
    return OffsetCommitResponse{.error_code = static_cast<WireError>(*error_code)};
}

void OffsetFetchRequest::encode(Encoder& enc) const {
    enc.put_string(group);
    enc.put_string(topic);
    enc.put_i32(partition);
}

expected<OffsetFetchRequest> OffsetFetchRequest::decode(Decoder& dec) {
    auto group = dec.get_string();
    if (!group) {
        return std::unexpected(group.error());
    }
    auto topic = dec.get_string();
    if (!topic) {
        return std::unexpected(topic.error());
    }
    auto partition = dec.get_i32();
    if (!partition) {
        return std::unexpected(partition.error());
    }
    OffsetFetchRequest request;
    request.group = std::string{*group};
    request.topic = std::string{*topic};
    request.partition = *partition;
    return request;
}

void OffsetFetchResponse::encode(Encoder& enc) const {
    enc.put_i64(offset);
    enc.put_i16(static_cast<std::int16_t>(error_code));
}

expected<OffsetFetchResponse> OffsetFetchResponse::decode(Decoder& dec) {
    auto offset = dec.get_i64();
    if (!offset) {
        return std::unexpected(offset.error());
    }
    auto error_code = dec.get_i16();
    if (!error_code) {
        return std::unexpected(error_code.error());
    }
    return OffsetFetchResponse{.offset = *offset,
                               .error_code = static_cast<WireError>(*error_code)};
}

}  // namespace nexus
