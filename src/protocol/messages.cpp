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

}  // namespace nexus
