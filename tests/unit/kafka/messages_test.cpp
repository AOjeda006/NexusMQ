// Pruebas de cabeceras y ApiVersions del subconjunto Kafka (F7b).
#include "kafka/messages.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "common/bytes.hpp"
#include "kafka/codec.hpp"

namespace {

using nexus::Buffer;
using nexus::kafka::ApiKey;
using nexus::kafka::ApiVersionRange;
using nexus::kafka::Decoder;
using nexus::kafka::Encoder;

TEST(KafkaMessages, Flexibilidad_PorApiYVersion) {
    EXPECT_FALSE(nexus::kafka::is_flexible(ApiKey::ApiVersions, 0));
    EXPECT_TRUE(nexus::kafka::is_flexible(ApiKey::ApiVersions, 3));
    EXPECT_FALSE(nexus::kafka::is_flexible(ApiKey::Produce, 8));
    EXPECT_TRUE(nexus::kafka::is_flexible(ApiKey::Produce, 9));
    EXPECT_TRUE(nexus::kafka::is_flexible(ApiKey::Fetch, 12));
}

TEST(KafkaMessages, CabeceraRespuesta_ApiVersionsEsSiempreV0) {
    // ApiVersions v3 es flexible en el cuerpo, pero su cabecera de respuesta es v0 (caso especial).
    EXPECT_EQ(nexus::kafka::response_header_version(ApiKey::ApiVersions, 3), 0);
    EXPECT_EQ(nexus::kafka::response_header_version(ApiKey::Metadata, 9), 1);
    EXPECT_EQ(nexus::kafka::response_header_version(ApiKey::Metadata, 8), 0);
}

TEST(KafkaMessages, CabeceraPeticion_Version) {
    EXPECT_EQ(nexus::kafka::request_header_version(ApiKey::ApiVersions, 0), 1);
    EXPECT_EQ(nexus::kafka::request_header_version(ApiKey::ApiVersions, 3), 2);
}

TEST(KafkaMessages, DecodeRequestHeader_NoFlexible) {
    // Construye una cabecera de petición Metadata v1 (no flexible): client_id presente, sin tags.
    Buffer buf;
    Encoder enc{buf};
    enc.put_i16(static_cast<std::int16_t>(ApiKey::Metadata));
    enc.put_i16(1);     // api_version
    enc.put_i32(7777);  // correlation_id
    enc.put_nullable_string(std::optional<std::string_view>{"kcat"});

    Decoder dec{buf.as_span()};
    const auto header = nexus::kafka::decode_request_header(dec);
    ASSERT_TRUE(header.has_value());
    EXPECT_EQ(header->api_key, static_cast<std::int16_t>(ApiKey::Metadata));
    EXPECT_EQ(header->api_version, 1);
    EXPECT_EQ(header->correlation_id, 7777);
    ASSERT_TRUE(header->client_id.has_value());
    EXPECT_EQ(*header->client_id, "kcat");
    EXPECT_TRUE(dec.empty());
}

TEST(KafkaMessages, DecodeRequestHeader_Flexible_SaltaTaggedFields) {
    // ApiVersions v3 (flexible): cabecera v2 con client_id NULLABLE_STRING + tagged fields.
    Buffer buf;
    Encoder enc{buf};
    enc.put_i16(static_cast<std::int16_t>(ApiKey::ApiVersions));
    enc.put_i16(3);
    enc.put_i32(1);
    enc.put_nullable_string(std::optional<std::string_view>{"librdkafka"});
    enc.put_empty_tagged_fields();
    enc.put_i32(0xCAFE);  // dato posterior, para verificar que el cursor quedó donde debe

    Decoder dec{buf.as_span()};
    const auto header = nexus::kafka::decode_request_header(dec);
    ASSERT_TRUE(header.has_value());
    EXPECT_EQ(header->api_version, 3);
    ASSERT_TRUE(header->client_id.has_value());
    EXPECT_EQ(*header->client_id, "librdkafka");
    const auto trailing = dec.get_i32();
    ASSERT_TRUE(trailing.has_value());
    EXPECT_EQ(*trailing, 0xCAFE);
}

TEST(KafkaMessages, EncodeResponseHeader_V0YV1) {
    {
        Buffer buf;
        Encoder enc{buf};
        nexus::kafka::encode_response_header(enc, 42, /*header_version=*/0);
        EXPECT_EQ(buf.size(), 4U);  // solo correlation_id (INT32)
    }
    {
        Buffer buf;
        Encoder enc{buf};
        nexus::kafka::encode_response_header(enc, 42, /*header_version=*/1);
        EXPECT_EQ(buf.size(), 5U);  // correlation_id + tagged fields vacíos (1 byte)
    }
}

TEST(KafkaMessages, ApiVersionsResponse_RoundTrip) {
    const auto resp = nexus::kafka::make_api_versions_response();
    ASSERT_FALSE(resp.api_keys.empty());

    Buffer buf;
    Encoder enc{buf};
    nexus::kafka::encode_api_versions_response(enc, resp);

    // Decodifica el cuerpo flexible v3 y verifica la estructura.
    Decoder dec{buf.as_span()};
    const auto error_code = dec.get_i16();
    ASSERT_TRUE(error_code.has_value());
    EXPECT_EQ(*error_code, 0);
    const auto count = dec.get_compact_array_len();
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(static_cast<std::size_t>(*count), resp.api_keys.size());
    for (int i = 0; i < *count; ++i) {
        const auto key = dec.get_i16();
        const auto lo = dec.get_i16();
        const auto hi = dec.get_i16();
        ASSERT_TRUE(key.has_value() && lo.has_value() && hi.has_value());
        ASSERT_TRUE(dec.skip_tagged_fields().has_value());
        EXPECT_EQ(*key, resp.api_keys[static_cast<std::size_t>(i)].api_key);
        EXPECT_EQ(*hi, resp.api_keys[static_cast<std::size_t>(i)].max_version);
    }
    const auto throttle = dec.get_i32();
    ASSERT_TRUE(throttle.has_value());
    EXPECT_EQ(*throttle, 0);
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());
    EXPECT_TRUE(dec.empty()) << "el cuerpo debe quedar consumido por completo";
}

TEST(KafkaMessages, ApiVersionsResponse_AnunciaLasCuatroApis) {
    const auto apis = nexus::kafka::supported_apis();
    bool has_produce = false;
    bool has_fetch = false;
    bool has_metadata = false;
    bool has_api_versions = false;
    for (const ApiVersionRange& api : apis) {
        has_produce |= api.api_key == static_cast<std::int16_t>(ApiKey::Produce);
        has_fetch |= api.api_key == static_cast<std::int16_t>(ApiKey::Fetch);
        has_metadata |= api.api_key == static_cast<std::int16_t>(ApiKey::Metadata);
        has_api_versions |= api.api_key == static_cast<std::int16_t>(ApiKey::ApiVersions);
    }
    EXPECT_TRUE(has_produce && has_fetch && has_metadata && has_api_versions);
}

}  // namespace
