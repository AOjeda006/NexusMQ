// Pruebas del dispatcher Kafka (KafkaGateway): cabecera → API → respuesta — F7e.
#include "kafka/gateway.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "kafka/codec.hpp"
#include "kafka/fetch.hpp"
#include "kafka/messages.hpp"
#include "kafka/metadata.hpp"
#include "kafka/produce.hpp"

namespace {

using nexus::Buffer;
using nexus::ByteSpan;
using nexus::kafka::ApiKey;
using nexus::kafka::Decoder;
using nexus::kafka::Encoder;

/// Doble de test del puerto al broker: registra la última petición y devuelve respuestas fijas.
class FakeBroker : public nexus::kafka::KafkaBroker {
public:
    nexus::kafka::MetadataRequest last_metadata;
    nexus::kafka::ProduceRequest last_produce;
    nexus::kafka::FetchRequest last_fetch;

    nexus::kafka::MetadataResponse metadata(const nexus::kafka::MetadataRequest& req) override {
        last_metadata = req;
        nexus::kafka::MetadataResponse resp;
        resp.controller_id = 1;
        nexus::kafka::MetadataBroker node;
        node.node_id = 1;
        node.host = "localhost";
        node.port = 9092;
        resp.brokers.push_back(node);
        nexus::kafka::MetadataTopic topic;
        topic.name = "orders";
        resp.topics.push_back(topic);
        return resp;
    }

    nexus::kafka::ProduceResponse produce(const nexus::kafka::ProduceRequest& req) override {
        last_produce = req;
        nexus::kafka::ProduceResponse resp;
        nexus::kafka::ProduceTopicResponse topic;
        topic.name = "orders";
        nexus::kafka::ProducePartitionResponse part;
        part.base_offset = 99;
        topic.partitions.push_back(part);
        resp.topics.push_back(topic);
        return resp;
    }

    nexus::kafka::FetchResponse fetch(const nexus::kafka::FetchRequest& req) override {
        last_fetch = req;
        nexus::kafka::FetchResponse resp;
        resp.session_id = 5;
        return resp;
    }
};

/// Escribe una cabecera de petición Kafka (sin el prefijo `Size`) acorde a su versión de cabecera.
void put_request_header(Encoder& enc, ApiKey api_key, std::int16_t api_version,
                        std::int32_t correlation_id) {
    enc.put_i16(static_cast<std::int16_t>(api_key));
    enc.put_i16(api_version);
    enc.put_i32(correlation_id);
    const std::int16_t header_version = nexus::kafka::request_header_version(api_key, api_version);
    if (header_version >= 1) {
        enc.put_nullable_string(std::optional<std::string_view>{"kcat"});  // client_id
    }
    if (header_version >= 2) {
        enc.put_empty_tagged_fields();
    }
}

TEST(KafkaGateway, ApiVersions_EchoesCorrelationAndListsApis) {
    FakeBroker broker;
    nexus::kafka::KafkaGateway gateway{broker};

    Buffer req;
    Encoder enc{req};
    put_request_header(enc, ApiKey::ApiVersions, 3, 777);
    // El cuerpo de la petición ApiVersions no lo interpreta el gateway.

    const auto resp = gateway.handle_request(req.as_span());
    ASSERT_TRUE(resp.has_value());

    Decoder dec{resp->as_span()};
    EXPECT_EQ(dec.get_i32().value_or(-1), 777);  // correlation_id (cabecera de respuesta v0)
    EXPECT_EQ(dec.get_i16().value_or(-1), 0);    // error_code
    EXPECT_GT(dec.get_compact_array_len().value_or(-1), 0);  // al menos una API anunciada
}

TEST(KafkaGateway, Metadata_DelegatesToBrokerAndEncodes) {
    FakeBroker broker;
    nexus::kafka::KafkaGateway gateway{broker};

    Buffer req;
    Encoder enc{req};
    put_request_header(enc, ApiKey::Metadata, 9, 12);
    enc.put_compact_array_len(1);  // topics
    enc.put_compact_string("orders");
    enc.put_empty_tagged_fields();  // topic tags
    enc.put_bool(false);            // allow_auto_topic_creation
    enc.put_bool(false);            // include_cluster_authorized_operations
    enc.put_bool(false);            // include_topic_authorized_operations
    enc.put_empty_tagged_fields();  // body tags

    const auto resp = gateway.handle_request(req.as_span());
    ASSERT_TRUE(resp.has_value());

    ASSERT_TRUE(broker.last_metadata.topics.has_value());
    ASSERT_EQ(broker.last_metadata.topics->size(), 1U);
    EXPECT_EQ(broker.last_metadata.topics->front(), "orders");

    Decoder dec{resp->as_span()};
    EXPECT_EQ(dec.get_i32().value_or(-1), 12);               // correlation_id
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());       // cabecera de respuesta flexible (v1)
    EXPECT_EQ(dec.get_i32().value_or(-1), 0);                // throttle_time_ms
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 1);  // brokers
}

TEST(KafkaGateway, Produce_PassesRecordsToBroker) {
    FakeBroker broker;
    nexus::kafka::KafkaGateway gateway{broker};

    const std::vector<std::byte> records{std::byte{0x10}, std::byte{0x20}};
    Buffer req;
    Encoder enc{req};
    put_request_header(enc, ApiKey::Produce, 9, 3);
    enc.put_compact_nullable_string(std::nullopt);  // transactional_id
    enc.put_i16(-1);                                // acks
    enc.put_i32(1000);                              // timeout_ms
    enc.put_compact_array_len(1);                   // topics
    enc.put_compact_string("orders");
    enc.put_compact_array_len(1);  // partitions
    enc.put_i32(0);                // index
    enc.put_compact_nullable_bytes(std::optional<ByteSpan>{ByteSpan{records}});
    enc.put_empty_tagged_fields();  // partition tags
    enc.put_empty_tagged_fields();  // topic tags
    enc.put_empty_tagged_fields();  // body tags

    const auto resp = gateway.handle_request(req.as_span());
    ASSERT_TRUE(resp.has_value());

    ASSERT_EQ(broker.last_produce.topics.size(), 1U);
    ASSERT_EQ(broker.last_produce.topics[0].partitions.size(), 1U);
    EXPECT_EQ(broker.last_produce.topics[0].partitions[0].records, records);

    Decoder dec{resp->as_span()};
    EXPECT_EQ(dec.get_i32().value_or(-1), 3);                // correlation_id
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());       // cabecera flexible
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 1);  // topics
}

TEST(KafkaGateway, Fetch_DelegatesToBroker) {
    FakeBroker broker;
    nexus::kafka::KafkaGateway gateway{broker};

    Buffer req;
    Encoder enc{req};
    put_request_header(enc, ApiKey::Fetch, 12, 8);
    enc.put_i32(-1);    // replica_id
    enc.put_i32(500);   // max_wait_ms
    enc.put_i32(1);     // min_bytes
    enc.put_i32(1024);  // max_bytes
    enc.put_i8(0);      // isolation_level
    enc.put_i32(0);     // session_id
    enc.put_i32(-1);    // session_epoch
    enc.put_compact_array_len(1);
    enc.put_compact_string("orders");
    enc.put_compact_array_len(1);
    enc.put_i32(2);                 // partition
    enc.put_i32(-1);                // current_leader_epoch
    enc.put_i64(50);                // fetch_offset
    enc.put_i32(-1);                // last_fetched_epoch
    enc.put_i64(0);                 // log_start_offset
    enc.put_i32(1048576);           // partition_max_bytes
    enc.put_empty_tagged_fields();  // partition tags
    enc.put_empty_tagged_fields();  // topic tags
    enc.put_compact_array_len(0);   // forgotten_topics_data
    enc.put_compact_string("");     // rack_id
    enc.put_empty_tagged_fields();  // body tags

    const auto resp = gateway.handle_request(req.as_span());
    ASSERT_TRUE(resp.has_value());

    ASSERT_EQ(broker.last_fetch.topics.size(), 1U);
    EXPECT_EQ(broker.last_fetch.topics[0].topic, "orders");
    ASSERT_EQ(broker.last_fetch.topics[0].partitions.size(), 1U);
    EXPECT_EQ(broker.last_fetch.topics[0].partitions[0].partition, 2);
    EXPECT_EQ(broker.last_fetch.topics[0].partitions[0].fetch_offset, 50);

    Decoder dec{resp->as_span()};
    EXPECT_EQ(dec.get_i32().value_or(-1), 8);           // correlation_id
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());  // cabecera flexible
    EXPECT_EQ(dec.get_i32().value_or(-1), 0);           // throttle_time_ms
}

TEST(KafkaGateway, UnsupportedApiKey_ReturnsError) {
    FakeBroker broker;
    nexus::kafka::KafkaGateway gateway{broker};

    Buffer req;
    Encoder enc{req};
    // api_key 42 no está en el subconjunto soportado; la cabecera se trata como no flexible.
    enc.put_i16(42);
    enc.put_i16(0);
    enc.put_i32(1);
    enc.put_nullable_string(std::optional<std::string_view>{"kcat"});

    const auto resp = gateway.handle_request(req.as_span());
    ASSERT_FALSE(resp.has_value());
    EXPECT_EQ(resp.error().code(), nexus::ErrorCode::Unsupported);
}

}  // namespace
