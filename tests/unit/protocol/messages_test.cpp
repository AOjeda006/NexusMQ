#include "protocol/messages.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "protocol/codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/frame.hpp"
#include "protocol/versioning.hpp"

namespace {

// Codifica @p message y lo vuelve a decodificar (T::decode) desde un búfer fresco.
template <class T>
nexus::expected<T> round_trip(const T& message) {
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    message.encode(enc);
    nexus::Decoder dec{buf.as_span()};
    return T::decode(dec);
}

TEST(Messages, ApiVersions_RoundTrip) {
    const nexus::ApiVersionsRequest req{.client_version = 4};
    EXPECT_EQ(round_trip(req).value_or(nexus::ApiVersionsRequest{.client_version = 0}), req);

    nexus::ApiVersionsResponse resp;
    resp.ranges.push_back({.key = nexus::ApiKey::Produce, .min = 0, .max = 3});
    resp.ranges.push_back({.key = nexus::ApiKey::Fetch, .min = 1, .max = 2});
    const auto decoded = round_trip(resp);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, resp);
}

TEST(Messages, Metadata_RoundTrip_Anidado) {
    nexus::MetadataResponse resp;
    resp.brokers.push_back({.node_id = 1, .host = "node-1.local", .port = 9092});
    resp.brokers.push_back({.node_id = 2, .host = "node-2.local", .port = 9092});

    nexus::TopicMeta topic;
    topic.name = "events";
    topic.error = nexus::WireError::None;
    topic.partitions.push_back(
        {.id = 0, .leader_node_id = 1, .replicas = {1, 2}, .leader_epoch = 7});
    topic.partitions.push_back(
        {.id = 1, .leader_node_id = 2, .replicas = {2, 1}, .leader_epoch = 7});
    resp.topics.push_back(topic);

    const auto decoded = round_trip(resp);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, resp);
}

TEST(Messages, MetadataRequest_RoundTrip_VacioYConTopics) {
    EXPECT_EQ(
        round_trip(nexus::MetadataRequest{}).value_or(nexus::MetadataRequest{.topics = {"x"}}),
        nexus::MetadataRequest{});

    const nexus::MetadataRequest req{.topics = {"a", "b", "c"}};
    const auto decoded = round_trip(req);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, req);
}

TEST(Messages, Decode_Truncado_DevuelveInvalidArgument) {
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    enc.put_varint(3);  // declara 3 brokers pero no hay ninguno

    nexus::Decoder dec{buf.as_span()};
    const auto r = nexus::MetadataResponse::decode(dec);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Messages, Decode_ContadorMalicioso_DevuelveInvalidArgument) {
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    enc.put_varint(1000000);  // contador enorme con el búfer casi vacío

    nexus::Decoder dec{buf.as_span()};
    const auto r = nexus::MetadataRequest::decode(dec);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Messages, ProduceRequest_RoundTrip_PreservaBatch) {
    const std::vector<std::byte> batch{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE},
                                       std::byte{0xEF}};
    nexus::ProduceRequest req;
    req.topic = "events";
    req.partition = 2;
    req.acks = nexus::Acks::Quorum;
    req.batch = batch;

    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    req.encode(enc);
    nexus::Decoder dec{buf.as_span()};
    const auto decoded = nexus::ProduceRequest::decode(dec);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->topic, "events");
    EXPECT_EQ(decoded->partition, 2);
    EXPECT_EQ(decoded->acks, nexus::Acks::Quorum);
    EXPECT_TRUE(
        std::equal(decoded->batch.begin(), decoded->batch.end(), batch.begin(), batch.end()));
}

TEST(Messages, ProduceRequest_AcksInvalido_DevuelveInvalidArgument) {
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    enc.put_string("t");
    enc.put_i32(0);
    enc.put_u8(9);  // acks fuera de rango (>2)
    enc.put_bytes(nexus::ByteSpan{});

    nexus::Decoder dec{buf.as_span()};
    const auto r = nexus::ProduceRequest::decode(dec);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Messages, ProduceResponse_RoundTrip) {
    const nexus::ProduceResponse resp{
        .base_offset = 42, .error_code = nexus::WireError::None, .throttle_ms = 5};
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    resp.encode(enc);
    nexus::Decoder dec{buf.as_span()};
    const auto decoded = nexus::ProduceResponse::decode(dec);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, resp);
}

TEST(Messages, FetchRequest_RoundTrip) {
    const nexus::FetchRequest req{.topic = "events",
                                  .partition = 1,
                                  .fetch_offset = 100,
                                  .max_bytes = 1048576,
                                  .min_bytes = 1,
                                  .max_wait_ms = 500};
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    req.encode(enc);
    nexus::Decoder dec{buf.as_span()};
    const auto decoded = nexus::FetchRequest::decode(dec);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, req);
}

TEST(Messages, FetchResponse_RoundTrip_PreservaBatches) {
    const std::vector<std::byte> batches{std::byte{1}, std::byte{2}, std::byte{3}};
    nexus::FetchResponse resp;
    resp.batches = batches;
    resp.high_watermark = 99;
    resp.log_start_offset = 10;
    resp.error_code = nexus::WireError::None;

    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    resp.encode(enc);
    nexus::Decoder dec{buf.as_span()};
    const auto decoded = nexus::FetchResponse::decode(dec);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->high_watermark, 99);
    EXPECT_EQ(decoded->log_start_offset, 10);
    EXPECT_TRUE(std::equal(decoded->batches.begin(), decoded->batches.end(), batches.begin(),
                           batches.end()));
}

TEST(Messages, CreateTopic_RoundTrip) {
    const nexus::CreateTopicRequest req{
        .name = "events", .partition_count = 12, .replication_factor = 3};
    const auto decoded = round_trip(req);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, req);

    const nexus::CreateTopicResponse resp{.error_code = nexus::WireError::None};
    EXPECT_EQ(round_trip(resp).value_or(
                  nexus::CreateTopicResponse{.error_code = nexus::WireError::InvalidRequest}),
              resp);
}

TEST(Messages, DeleteTopic_RoundTrip) {
    const nexus::DeleteTopicRequest req{.name = "events"};
    const auto decoded = round_trip(req);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, req);

    const nexus::DeleteTopicResponse resp{.error_code = nexus::WireError::UnknownTopicOrPartition};
    const auto decoded_resp = round_trip(resp);
    ASSERT_TRUE(decoded_resp.has_value());
    EXPECT_EQ(*decoded_resp, resp);
}

TEST(Messages, OffsetCommit_RoundTrip) {
    const nexus::OffsetCommitRequest req{
        .group = "g1", .topic = "events", .partition = 3, .offset = 1000, .metadata = "meta"};
    const auto decoded = round_trip(req);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, req);

    const nexus::OffsetCommitResponse resp{.error_code = nexus::WireError::None};
    EXPECT_EQ(round_trip(resp).value_or(
                  nexus::OffsetCommitResponse{.error_code = nexus::WireError::InvalidRequest}),
              resp);
}

TEST(Messages, OffsetFetch_RoundTrip) {
    const nexus::OffsetFetchRequest req{.group = "g1", .topic = "events", .partition = 3};
    const auto decoded = round_trip(req);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, req);

    const nexus::OffsetFetchResponse resp{.offset = 500, .error_code = nexus::WireError::None};
    const auto decoded_resp = round_trip(resp);
    ASSERT_TRUE(decoded_resp.has_value());
    EXPECT_EQ(*decoded_resp, resp);
}

}  // namespace
