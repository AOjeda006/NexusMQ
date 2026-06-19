// Pruebas del codec del protocolo Kafka (big-endian, tipos clásicos y compactos) — F7a.
#include "kafka/codec.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"

namespace {

using nexus::Buffer;
using nexus::ByteSpan;
using nexus::kafka::Decoder;
using nexus::kafka::Encoder;

std::vector<std::byte> bytes_of(const Buffer& buf) {
    const ByteSpan span = buf.as_span();
    return {span.begin(), span.end()};
}

std::byte b(int value) {
    return static_cast<std::byte>(value);
}

TEST(KafkaCodec, EnterosSonBigEndian) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_i16(0x0102);
    enc.put_i32(0x01020304);
    const std::vector<std::byte> raw = bytes_of(buf);
    // Big-endian: el byte más significativo primero.
    const std::vector<std::byte> expected{b(0x01), b(0x02), b(0x01), b(0x02), b(0x03), b(0x04)};
    EXPECT_EQ(raw, expected);
}

TEST(KafkaCodec, I32_I64_RoundTrip) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_i32(-12345);
    enc.put_i64(-9876543210LL);
    Decoder dec{buf.as_span()};
    const auto a = dec.get_i32();
    const auto c = dec.get_i64();
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(*a, -12345);
    EXPECT_EQ(*c, -9876543210LL);
    EXPECT_TRUE(dec.empty());
}

TEST(KafkaCodec, String_RoundTrip) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_string("topic-a");
    Decoder dec{buf.as_span()};
    const auto s = dec.get_string();
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(*s, "topic-a");
}

TEST(KafkaCodec, NullableString_NuloYNoNulo) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_nullable_string(std::nullopt);
    enc.put_nullable_string(std::optional<std::string_view>{"x"});
    Decoder dec{buf.as_span()};
    const auto a = dec.get_nullable_string();
    const auto bb = dec.get_nullable_string();
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(bb.has_value());
    EXPECT_FALSE(a->has_value());
    ASSERT_TRUE(bb->has_value());
    EXPECT_EQ(**bb, "x");
}

TEST(KafkaCodec, CompactString_PrefijoEsLongitudMasUno) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_compact_string("abc");
    const std::vector<std::byte> raw = bytes_of(buf);
    // COMPACT_STRING: unsigned_varint(len+1=4) + "abc".
    const std::vector<std::byte> expected{b(4), b('a'), b('b'), b('c')};
    EXPECT_EQ(raw, expected);

    Decoder dec{buf.as_span()};
    const auto s = dec.get_compact_string();
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(*s, "abc");
}

TEST(KafkaCodec, CompactNullableString_NuloEsCero) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_compact_nullable_string(std::nullopt);
    EXPECT_EQ(bytes_of(buf), (std::vector<std::byte>{b(0)}));
    Decoder dec{buf.as_span()};
    const auto s = dec.get_compact_nullable_string();
    ASSERT_TRUE(s.has_value());
    EXPECT_FALSE(s->has_value());
}

TEST(KafkaCodec, UnsignedVarint_RoundTripGrande) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_unsigned_varint(300);
    enc.put_unsigned_varint(0);
    enc.put_unsigned_varint(1U << 28U);
    Decoder dec{buf.as_span()};
    const auto a = dec.get_unsigned_varint();
    const auto c = dec.get_unsigned_varint();
    const auto d = dec.get_unsigned_varint();
    ASSERT_TRUE(a.has_value() && c.has_value() && d.has_value());
    EXPECT_EQ(*a, 300U);
    EXPECT_EQ(*c, 0U);
    EXPECT_EQ(*d, 1U << 28U);
}

TEST(KafkaCodec, Varint_Varlong_Zigzag) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_varint(-1);
    enc.put_varlong(-2);
    Decoder dec{buf.as_span()};
    const auto a = dec.get_varint();
    const auto c = dec.get_varlong();
    ASSERT_TRUE(a.has_value() && c.has_value());
    EXPECT_EQ(*a, -1);
    EXPECT_EQ(*c, -2);
}

TEST(KafkaCodec, Bytes_RoundTrip) {
    Buffer buf;
    Encoder enc{buf};
    const std::vector<std::byte> payload{b(0xDE), b(0xAD), b(0xBE), b(0xEF)};
    enc.put_bytes(ByteSpan{payload});
    Decoder dec{buf.as_span()};
    const auto got = dec.get_bytes();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(std::vector<std::byte>(got->begin(), got->end()), payload);
}

TEST(KafkaCodec, CompactArrayLen_RoundTrip) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_compact_array_len(3);
    enc.put_compact_array_len(-1);  // nulo
    Decoder dec{buf.as_span()};
    const auto a = dec.get_compact_array_len();
    const auto n = dec.get_compact_array_len();
    ASSERT_TRUE(a.has_value() && n.has_value());
    EXPECT_EQ(*a, 3);
    EXPECT_EQ(*n, -1);
}

TEST(KafkaCodec, ArrayLen_Clasico) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_array_len(2);
    Decoder dec{buf.as_span()};
    const auto a = dec.get_array_len();
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(*a, 2);
}

TEST(KafkaCodec, TaggedFields_VacioSeSaltaSinConsumirDeMas) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_empty_tagged_fields();
    enc.put_i32(42);  // dato tras los tagged fields
    Decoder dec{buf.as_span()};
    ASSERT_TRUE(dec.skip_tagged_fields().has_value());
    const auto v = dec.get_i32();
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 42);
}

TEST(KafkaCodec, Decodificador_FueraDeLimites_EsError) {
    const std::vector<std::byte> truncated{b(0x00)};  // 1 byte: insuficiente para un i32
    Decoder dec{ByteSpan{truncated}};
    const auto v = dec.get_i32();
    ASSERT_FALSE(v.has_value());
    EXPECT_EQ(v.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(KafkaCodec, CompactString_DeclaraNulo_EsCorrupt) {
    const std::vector<std::byte> raw{b(0)};  // unsigned_varint 0 = nulo, inválido para no-nullable
    Decoder dec{ByteSpan{raw}};
    const auto s = dec.get_compact_string();
    ASSERT_FALSE(s.has_value());
    EXPECT_EQ(s.error().code(), nexus::ErrorCode::Corrupt);
}

}  // namespace
