#include "protocol/codec.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"

namespace {

TEST(Codec, RoundTrip_TiposFijosVariableYCadenas) {
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    enc.put_u8(0xAB);
    enc.put_u16(0x1234);
    enc.put_u32(0xDEADBEEF);
    enc.put_i16(-7);
    enc.put_i32(-123456);
    enc.put_i64(-9876543210);
    enc.put_varint(300);
    enc.put_string("topic-a");
    const std::vector<std::byte> payload{std::byte{1}, std::byte{2}, std::byte{3}};
    enc.put_bytes(payload);

    nexus::Decoder dec{buf.as_span()};
    EXPECT_EQ(dec.get_u8().value_or(0), 0xABU);
    EXPECT_EQ(dec.get_u16().value_or(0), 0x1234U);
    EXPECT_EQ(dec.get_u32().value_or(0), 0xDEADBEEFU);
    EXPECT_EQ(dec.get_i16().value_or(0), -7);
    EXPECT_EQ(dec.get_i32().value_or(0), -123456);
    EXPECT_EQ(dec.get_i64().value_or(0), -9876543210);
    EXPECT_EQ(dec.get_varint().value_or(0), 300U);

    const auto text = dec.get_string();
    ASSERT_TRUE(text.has_value());
    EXPECT_EQ(*text, std::string_view{"topic-a"});

    const auto bytes = dec.get_bytes();
    ASSERT_TRUE(bytes.has_value());
    ASSERT_EQ(bytes->size(), 3U);
    EXPECT_EQ(std::to_integer<int>((*bytes)[2]), 3);

    EXPECT_TRUE(dec.empty());  // se consumió todo
}

TEST(Codec, Decoder_BufferVacio_GetU32_DevuelveInvalidArgument) {
    nexus::Decoder dec{nexus::ByteSpan{}};
    const auto r = dec.get_u32();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Codec, Decoder_LongitudExcesiva_DevuelveInvalidArgument) {
    // Un campo bytes que declara más longitud de la disponible: el decodificador no se sale.
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    enc.put_varint(1000);  // longitud declarada
    enc.put_u8(0x01);      // solo 1 byte real

    nexus::Decoder dec{buf.as_span()};
    const auto r = dec.get_bytes();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Codec, Decoder_StringVaciaYBytesVacios) {
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    enc.put_string("");
    enc.put_bytes(nexus::ByteSpan{});

    nexus::Decoder dec{buf.as_span()};
    const auto s = dec.get_string();
    ASSERT_TRUE(s.has_value());
    EXPECT_TRUE(s->empty());
    const auto b = dec.get_bytes();
    ASSERT_TRUE(b.has_value());
    EXPECT_TRUE(b->empty());
    EXPECT_TRUE(dec.empty());
}

}  // namespace
