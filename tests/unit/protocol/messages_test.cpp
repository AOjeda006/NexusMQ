#include "protocol/messages.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
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

}  // namespace
