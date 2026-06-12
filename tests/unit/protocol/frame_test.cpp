#include "protocol/frame.hpp"

#include <gtest/gtest.h>

#include <cstdint>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "protocol/codec.hpp"

namespace {

TEST(FrameHeader, EncodeDecode_RoundTrip) {
    nexus::FrameHeader header;
    header.length = nexus::FrameHeader::length_for(120);
    header.api_key = nexus::ApiKey::Produce;
    header.api_version = 3;
    header.correlation_id = 0xCAFEBABE;
    header.flags = nexus::FrameHeader::kFlagCreditUpdate;

    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    header.encode(enc);
    EXPECT_EQ(buf.size(), nexus::FrameHeader::kEncodedSize);

    nexus::Decoder dec{buf.as_span()};
    const auto decoded = nexus::FrameHeader::decode(dec);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->length, header.length);
    EXPECT_EQ(decoded->api_key, nexus::ApiKey::Produce);
    EXPECT_EQ(decoded->api_version, 3);
    EXPECT_EQ(decoded->correlation_id, 0xCAFEBABEU);
    EXPECT_TRUE(decoded->has_credit_update());
    EXPECT_TRUE(dec.empty());
}

TEST(FrameHeader, LengthFor_CuentaCabeceraTrasLengthMasPayload) {
    // 14 - 4 (length) = 10 bytes de cabecera tras length.
    EXPECT_EQ(nexus::FrameHeader::length_for(0), 10U);
    EXPECT_EQ(nexus::FrameHeader::length_for(100), 110U);
}

TEST(FrameHeader, Decode_ApiKeyDesconocido_DevuelveInvalidArgument) {
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    enc.put_u32(10);      // length
    enc.put_u16(0xFFFF);  // api_key fuera de rango
    enc.put_u16(0);       // api_version
    enc.put_u32(1);       // correlation_id
    enc.put_u16(0);       // flags

    nexus::Decoder dec{buf.as_span()};
    const auto r = nexus::FrameHeader::decode(dec);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(FrameHeader, Decode_Truncada_DevuelveInvalidArgument) {
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    enc.put_u32(10);  // solo el campo length: faltan los demás

    nexus::Decoder dec{buf.as_span()};
    const auto r = nexus::FrameHeader::decode(dec);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

}  // namespace
