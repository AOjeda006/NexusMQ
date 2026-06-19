// Pruebas de la compresión por códec (None/LZ4/Zstd) y su integración con el codec por record.
#include "common/compression.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/record_codec.hpp"
#include "common/types.hpp"

namespace {

using nexus::ByteSpan;
using nexus::Codec;
using nexus::Record;
using nexus::RecordBatch;
using nexus::RecordBatchBuilder;

// Bytes a partir de una cadena (comodidad de test).
std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> out(text.size());
    if (!text.empty()) {
        std::memcpy(out.data(), text.data(), text.size());
    }
    return out;
}

// Texto repetitivo (comprimible) de la longitud pedida.
std::vector<std::byte> repetitive(std::size_t bytes_len) {
    std::string text;
    text.reserve(bytes_len);
    while (text.size() < bytes_len) {
        text += "nexusmq-compresion-de-bloques-";
    }
    text.resize(bytes_len);
    return bytes(text);
}

// Round-trip comprimir→descomprimir para un códec; espera recuperar la entrada exacta.
void expect_round_trip(Codec codec) {
    const std::vector<std::byte> input = repetitive(8192);
    const nexus::expected<std::vector<std::byte>> packed = nexus::compress(codec, ByteSpan{input});
    ASSERT_TRUE(packed.has_value());
    const nexus::expected<std::vector<std::byte>> back =
        nexus::decompress(codec, ByteSpan{*packed}, input.size());
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(*back, input);
}

TEST(Compression, None_EsCopiaIdentica) {
    const std::vector<std::byte> input = bytes("datos sin comprimir");
    const nexus::expected<std::vector<std::byte>> packed =
        nexus::compress(Codec::None, ByteSpan{input});
    ASSERT_TRUE(packed.has_value());
    EXPECT_EQ(*packed, input);
    const nexus::expected<std::vector<std::byte>> back =
        nexus::decompress(Codec::None, ByteSpan{*packed}, input.size());
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(*back, input);
}

TEST(Compression, Lz4_RoundTrip) {
    if (!nexus::codec_available(Codec::Lz4)) {
        GTEST_SKIP() << "LZ4 no compilado en este build";
    }
    expect_round_trip(Codec::Lz4);
}

TEST(Compression, Zstd_RoundTrip) {
    if (!nexus::codec_available(Codec::Zstd)) {
        GTEST_SKIP() << "Zstd no compilado en este build";
    }
    expect_round_trip(Codec::Zstd);
}

TEST(Compression, ComprimeReduceTamano) {
    if (!nexus::codec_available(Codec::Zstd)) {
        GTEST_SKIP() << "Zstd no compilado en este build";
    }
    const std::vector<std::byte> input = repetitive(65536);
    const nexus::expected<std::vector<std::byte>> packed =
        nexus::compress(Codec::Zstd, ByteSpan{input});
    ASSERT_TRUE(packed.has_value());
    EXPECT_LT(packed->size(), input.size()) << "datos repetitivos deben comprimir";
}

TEST(Compression, AntiBomba_RechazaTamanoExcesivo) {
    if (!nexus::codec_available(Codec::Zstd)) {
        GTEST_SKIP() << "Zstd no compilado en este build";
    }
    const std::vector<std::byte> input = repetitive(4096);
    const nexus::expected<std::vector<std::byte>> packed =
        nexus::compress(Codec::Zstd, ByteSpan{input});
    ASSERT_TRUE(packed.has_value());
    // El bloque declara 4096 bytes; acotamos a 1024 => debe rechazarse antes de reservar.
    const nexus::expected<std::vector<std::byte>> back =
        nexus::decompress(Codec::Zstd, ByteSpan{*packed}, 1024);
    ASSERT_FALSE(back.has_value());
    EXPECT_EQ(back.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(Compression, BloqueTruncado_EsCorrupt) {
    if (!nexus::codec_available(Codec::Lz4)) {
        GTEST_SKIP() << "LZ4 no compilado en este build";
    }
    // Menos bytes que el prefijo de tamaño (4): truncado.
    const std::vector<std::byte> tiny = bytes("ab");
    const nexus::expected<std::vector<std::byte>> back =
        nexus::decompress(Codec::Lz4, ByteSpan{tiny}, 4096);
    ASSERT_FALSE(back.has_value());
    EXPECT_EQ(back.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(Compression, CodecNoCompilado_EsUnsupported) {
    if (nexus::codec_available(Codec::Lz4)) {
        GTEST_SKIP() << "LZ4 sí está compilado; este caso aplica al build sin LZ4";
    }
    const std::vector<std::byte> input = bytes("hola");
    const nexus::expected<std::vector<std::byte>> packed =
        nexus::compress(Codec::Lz4, ByteSpan{input});
    ASSERT_FALSE(packed.has_value());
    EXPECT_EQ(packed.error().code(), nexus::ErrorCode::Unsupported);
}

TEST(Compression, AttrsCodec_RoundTripBits) {
    EXPECT_EQ(nexus::codec_from_attrs(0), Codec::None);
    for (const Codec codec : {Codec::None, Codec::Lz4, Codec::Zstd}) {
        // Bits altos preservados (simulan otros flags futuros).
        const std::uint16_t attrs = nexus::attrs_with_codec(0xFFF0, codec);
        EXPECT_EQ(nexus::codec_from_attrs(attrs), codec);
        EXPECT_EQ(attrs & 0xFFF0, 0xFFF0) << "los bits no-códec se conservan";
    }
}

// Integración: un batch comprimido se descomprime de forma transparente al decodificar.
TEST(Compression, RecordBatch_ComprimidoRoundTrip) {
    if (!nexus::codec_available(Codec::Zstd)) {
        GTEST_SKIP() << "Zstd no compilado en este build";
    }
    RecordBatchBuilder builder;
    for (int i = 0; i < 64; ++i) {
        Record rec;
        rec.key = bytes("clave-" + std::to_string(i % 4));
        rec.value = bytes("valor-repetitivo-para-comprimir-" + std::to_string(i));
        builder.add(rec);
    }
    const RecordBatch batch = builder.build(/*header=*/{}, Codec::Zstd);
    EXPECT_EQ(nexus::codec_from_attrs(batch.header().attrs), Codec::Zstd);

    const nexus::expected<std::vector<Record>> decoded = nexus::decode_records(batch);
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->size(), 64U);
    EXPECT_EQ((*decoded)[7].value, bytes("valor-repetitivo-para-comprimir-7"));
    EXPECT_EQ((*decoded)[0].offset, 0);
    EXPECT_EQ((*decoded)[63].offset, 63);
}

TEST(Compression, RecordBatch_SinCodecQuedaNone) {
    RecordBatchBuilder builder;
    builder.add([] {
        Record rec;
        rec.value = bytes("uno");
        return rec;
    }());
    const RecordBatch batch = builder.build();
    EXPECT_EQ(nexus::codec_from_attrs(batch.header().attrs), Codec::None);
    const nexus::expected<std::vector<Record>> decoded = nexus::decode_records(batch);
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->size(), 1U);
    EXPECT_EQ((*decoded)[0].value, bytes("uno"));
}

}  // namespace
