// Pruebas de la API Metadata del subconjunto Kafka (v9 flexible) — F7c.
#include "kafka/metadata.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "kafka/codec.hpp"

namespace {

using nexus::Buffer;
using nexus::kafka::Decoder;
using nexus::kafka::Encoder;
using nexus::kafka::MetadataBroker;
using nexus::kafka::MetadataPartition;
using nexus::kafka::MetadataResponse;
using nexus::kafka::MetadataTopic;

TEST(KafkaMetadata, DecodeRequest_TopicsNulo_PideTodos) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_compact_array_len(-1);  // topics = null → todos
    enc.put_bool(false);            // allow_auto_topic_creation
    enc.put_bool(false);            // include_cluster_authorized_operations
    enc.put_bool(true);             // include_topic_authorized_operations
    enc.put_empty_tagged_fields();

    Decoder dec{buf.as_span()};
    const auto req = nexus::kafka::decode_metadata_request(dec);
    ASSERT_TRUE(req.has_value());
    EXPECT_FALSE(req->topics.has_value());
    EXPECT_TRUE(req->include_topic_authorized_operations);
    EXPECT_TRUE(dec.empty());
}

TEST(KafkaMetadata, DecodeRequest_ListaDeTopics) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_compact_array_len(2);
    enc.put_compact_string("orders");
    enc.put_empty_tagged_fields();
    enc.put_compact_string("payments");
    enc.put_empty_tagged_fields();
    enc.put_bool(true);
    enc.put_bool(false);
    enc.put_bool(false);
    enc.put_empty_tagged_fields();

    Decoder dec{buf.as_span()};
    const auto req = nexus::kafka::decode_metadata_request(dec);
    ASSERT_TRUE(req.has_value());
    ASSERT_TRUE(req->topics.has_value());
    ASSERT_EQ(req->topics->size(), 2U);
    EXPECT_EQ((*req->topics)[0], "orders");
    EXPECT_EQ((*req->topics)[1], "payments");
    EXPECT_TRUE(req->allow_auto_topic_creation);
}

TEST(KafkaMetadata, EncodeResponse_RoundTrip) {
    MetadataResponse resp;
    resp.throttle_time_ms = 0;
    resp.brokers.push_back(MetadataBroker{
        .node_id = 1, .host = "broker-1", .port = 9092, .rack = std::optional<std::string>{"r1"}});
    resp.cluster_id = std::optional<std::string>{"nexusmq-cluster"};
    resp.controller_id = 1;

    MetadataPartition part;
    part.partition_index = 0;
    part.leader_id = 1;
    part.leader_epoch = 5;
    part.replica_nodes = {1};
    part.isr_nodes = {1};
    MetadataTopic topic;
    topic.name = "orders";
    topic.partitions.push_back(part);
    resp.topics.push_back(topic);

    Buffer buf;
    Encoder enc{buf};
    nexus::kafka::encode_metadata_response(enc, resp);

    // Decodifica manualmente el cuerpo v9 y verifica la estructura.
    Decoder dec{buf.as_span()};
    EXPECT_EQ(dec.get_i32().value_or(-1), 0);  // throttle_time_ms
    const auto broker_count = dec.get_compact_array_len();
    ASSERT_TRUE(broker_count.has_value());
    ASSERT_EQ(*broker_count, 1);
    EXPECT_EQ(dec.get_i32().value_or(-1), 1);  // node_id
    EXPECT_EQ(dec.get_compact_string().value_or(""), "broker-1");
    EXPECT_EQ(dec.get_i32().value_or(-1), 9092);  // port
    const auto rack = dec.get_compact_nullable_string();
    ASSERT_TRUE(rack.has_value());
    ASSERT_TRUE(rack->has_value());
    EXPECT_EQ(**rack, "r1");
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());  // broker tagged fields

    const auto cluster_id = dec.get_compact_nullable_string();
    ASSERT_TRUE(cluster_id.has_value() && cluster_id->has_value());
    EXPECT_EQ(**cluster_id, "nexusmq-cluster");
    EXPECT_EQ(dec.get_i32().value_or(-1), 1);  // controller_id

    const auto topic_count = dec.get_compact_array_len();
    ASSERT_TRUE(topic_count.has_value());
    ASSERT_EQ(*topic_count, 1);
    EXPECT_EQ(dec.get_i16().value_or(-1), 0);  // topic error_code
    EXPECT_EQ(dec.get_compact_string().value_or(""), "orders");
    EXPECT_FALSE(dec.get_bool().value_or(true));  // is_internal
    const auto part_count = dec.get_compact_array_len();
    ASSERT_TRUE(part_count.has_value());
    ASSERT_EQ(*part_count, 1);
    EXPECT_EQ(dec.get_i16().value_or(-1), 0);                // partition error_code
    EXPECT_EQ(dec.get_i32().value_or(-1), 0);                // partition_index
    EXPECT_EQ(dec.get_i32().value_or(-1), 1);                // leader_id
    EXPECT_EQ(dec.get_i32().value_or(-1), 5);                // leader_epoch
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 1);  // replica_nodes len
    EXPECT_EQ(dec.get_i32().value_or(-1), 1);
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 1);  // isr_nodes len
    EXPECT_EQ(dec.get_i32().value_or(-1), 1);
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 0);  // offline_replicas len
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());       // partition tagged fields
    EXPECT_EQ(dec.get_i32().value_or(0), topic.topic_authorized_operations);
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());  // topic tagged fields

    EXPECT_EQ(dec.get_i32().value_or(0), resp.cluster_authorized_operations);
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());  // body tagged fields
    EXPECT_TRUE(dec.empty()) << "el cuerpo Metadata debe quedar consumido por completo";
}

}  // namespace
