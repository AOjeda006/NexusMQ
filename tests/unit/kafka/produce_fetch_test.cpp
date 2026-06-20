// Pruebas de las APIs Produce y Fetch del subconjunto Kafka (v9/v12 flexible) — F7d.
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "kafka/codec.hpp"
#include "kafka/fetch.hpp"
#include "kafka/produce.hpp"

namespace {

using nexus::Buffer;
using nexus::ByteSpan;
using nexus::kafka::Decoder;
using nexus::kafka::Encoder;

std::vector<std::byte> bytes(std::initializer_list<int> vals) {
    std::vector<std::byte> out;
    out.reserve(vals.size());
    for (const int v : vals) {
        out.push_back(static_cast<std::byte>(v));
    }
    return out;
}

TEST(KafkaProduce, DecodeRequest_RoundTrip) {
    const std::vector<std::byte> records = bytes({0x01, 0x02, 0x03});
    // Construye una petición Produce v9 flexible.
    Buffer buf;
    Encoder enc{buf};
    enc.put_compact_nullable_string(std::nullopt);  // transactional_id
    enc.put_i16(-1);                                // acks
    enc.put_i32(30000);                             // timeout_ms
    enc.put_compact_array_len(1);                   // topics
    enc.put_compact_string("orders");
    enc.put_compact_array_len(1);  // partitions
    enc.put_i32(0);                // index
    enc.put_compact_nullable_bytes(std::optional<ByteSpan>{ByteSpan{records}});
    enc.put_empty_tagged_fields();  // partition tags
    enc.put_empty_tagged_fields();  // topic tags
    enc.put_empty_tagged_fields();  // body tags

    Decoder dec{buf.as_span()};
    const auto req = nexus::kafka::decode_produce_request(dec);
    ASSERT_TRUE(req.has_value());
    EXPECT_FALSE(req->transactional_id.has_value());
    EXPECT_EQ(req->acks, -1);
    EXPECT_EQ(req->timeout_ms, 30000);
    ASSERT_EQ(req->topics.size(), 1U);
    EXPECT_EQ(req->topics[0].name, "orders");
    ASSERT_EQ(req->topics[0].partitions.size(), 1U);
    EXPECT_EQ(req->topics[0].partitions[0].index, 0);
    EXPECT_EQ(req->topics[0].partitions[0].records, records);
    EXPECT_TRUE(dec.empty());
}

TEST(KafkaProduce, EncodeResponse_RoundTrip) {
    nexus::kafka::ProduceResponse resp;
    nexus::kafka::ProduceTopicResponse topic;
    topic.name = "orders";
    topic.partitions.push_back(
        nexus::kafka::ProducePartitionResponse{.index = 0,
                                               .error_code = 0,
                                               .base_offset = 42,
                                               .log_append_time_ms = -1,
                                               .log_start_offset = 0,
                                               .error_message = std::nullopt});
    resp.topics.push_back(topic);

    Buffer buf;
    Encoder enc{buf};
    nexus::kafka::encode_produce_response(enc, resp);

    Decoder dec{buf.as_span()};
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 1);
    EXPECT_EQ(dec.get_compact_string().value_or(""), "orders");
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 1);
    EXPECT_EQ(dec.get_i32().value_or(-1), 0);                // index
    EXPECT_EQ(dec.get_i16().value_or(-1), 0);                // error_code
    EXPECT_EQ(dec.get_i64().value_or(-1), 42);               // base_offset
    EXPECT_EQ(dec.get_i64().value_or(0), -1);                // log_append_time_ms
    EXPECT_EQ(dec.get_i64().value_or(-1), 0);                // log_start_offset
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 0);  // record_errors
    const auto err_msg = dec.get_compact_nullable_string();
    ASSERT_TRUE(err_msg.has_value());
    EXPECT_FALSE(err_msg->has_value());
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());  // partition tags
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());  // topic tags
    EXPECT_EQ(dec.get_i32().value_or(-1), 0);           // throttle_time_ms
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());  // body tags
    EXPECT_TRUE(dec.empty());
}

TEST(KafkaFetch, DecodeRequest_RoundTrip) {
    Buffer buf;
    Encoder enc{buf};
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
    enc.put_i32(3);                 // partition
    enc.put_i32(-1);                // current_leader_epoch
    enc.put_i64(100);               // fetch_offset
    enc.put_i32(-1);                // last_fetched_epoch
    enc.put_i64(0);                 // log_start_offset
    enc.put_i32(1048576);           // partition_max_bytes
    enc.put_empty_tagged_fields();  // partition tags
    enc.put_empty_tagged_fields();  // topic tags
    enc.put_compact_array_len(0);   // forgotten_topics_data
    enc.put_compact_string("");     // rack_id
    enc.put_empty_tagged_fields();  // body tags

    Decoder dec{buf.as_span()};
    const auto req = nexus::kafka::decode_fetch_request(dec);
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->max_wait_ms, 500);
    ASSERT_EQ(req->topics.size(), 1U);
    EXPECT_EQ(req->topics[0].topic, "orders");
    ASSERT_EQ(req->topics[0].partitions.size(), 1U);
    EXPECT_EQ(req->topics[0].partitions[0].partition, 3);
    EXPECT_EQ(req->topics[0].partitions[0].fetch_offset, 100);
    EXPECT_TRUE(dec.empty());
}

TEST(KafkaFetch, EncodeResponse_ConRecords_RoundTrip) {
    const std::vector<std::byte> records = bytes({0xAA, 0xBB});
    nexus::kafka::FetchResponse resp;
    resp.session_id = 7;
    nexus::kafka::FetchTopicResponse topic;
    topic.topic = "orders";
    nexus::kafka::FetchPartitionResponse part;
    part.partition_index = 0;
    part.high_watermark = 100;
    part.records = records;
    topic.partitions.push_back(part);
    resp.topics.push_back(topic);

    Buffer buf;
    Encoder enc{buf};
    nexus::kafka::encode_fetch_response(enc, resp);

    Decoder dec{buf.as_span()};
    EXPECT_EQ(dec.get_i32().value_or(-1), 0);  // throttle
    EXPECT_EQ(dec.get_i16().value_or(-1), 0);  // error_code
    EXPECT_EQ(dec.get_i32().value_or(-1), 7);  // session_id
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 1);
    EXPECT_EQ(dec.get_compact_string().value_or(""), "orders");
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 1);
    EXPECT_EQ(dec.get_i32().value_or(-1), 0);                // partition_index
    EXPECT_EQ(dec.get_i16().value_or(-1), 0);                // error_code
    EXPECT_EQ(dec.get_i64().value_or(-1), 100);              // high_watermark
    EXPECT_EQ(dec.get_i64().value_or(0), -1);                // last_stable_offset
    EXPECT_EQ(dec.get_i64().value_or(-1), 0);                // log_start_offset
    EXPECT_EQ(dec.get_compact_array_len().value_or(-1), 0);  // aborted_transactions
    EXPECT_EQ(dec.get_i32().value_or(0), -1);                // preferred_read_replica
    const auto blob = dec.get_compact_nullable_bytes();
    ASSERT_TRUE(blob.has_value());
    ASSERT_TRUE(blob->has_value());
    EXPECT_EQ(std::vector<std::byte>((*blob)->begin(), (*blob)->end()), records);
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());  // partition tags
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());  // topic tags
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());  // body tags
    EXPECT_TRUE(dec.empty());
}

}  // namespace
