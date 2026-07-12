// Pruebas del codec de control records (marcadores COMMIT/ABORT) y flags de batch transaccional.
#include "common/control_record.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

#include "common/bytes.hpp"
#include "common/compression.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/record_codec.hpp"

namespace {

using nexus::ByteSpan;
using nexus::ControlRecordType;
using nexus::EndTxnMarker;

// --- Flags de attrs -------------------------------------------------------

TEST(ControlAttrs, Flags_DisjuntosDelCodec) {
    // Los bits transaccional/control no pisan los 2 bits bajos del códec.
    EXPECT_EQ(nexus::kTransactionalAttr & nexus::kCodecMask, 0U);
    EXPECT_EQ(nexus::kControlAttr & nexus::kCodecMask, 0U);
    EXPECT_EQ(nexus::kTransactionalAttr & nexus::kControlAttr, 0U);
}

TEST(ControlAttrs, SetYGet_Independientes) {
    std::uint16_t attrs = nexus::attrs_with_codec(0, nexus::Codec::Zstd);
    EXPECT_FALSE(nexus::is_transactional(attrs));
    EXPECT_FALSE(nexus::is_control(attrs));

    attrs = nexus::attrs_with_transactional(attrs, true);
    EXPECT_TRUE(nexus::is_transactional(attrs));
    EXPECT_FALSE(nexus::is_control(attrs));
    // El códec sobrevive a fijar el flag transaccional.
    EXPECT_EQ(nexus::codec_from_attrs(attrs), nexus::Codec::Zstd);

    attrs = nexus::attrs_with_control(attrs, true);
    EXPECT_TRUE(nexus::is_control(attrs));
    EXPECT_TRUE(nexus::is_transactional(attrs));

    attrs = nexus::attrs_with_transactional(attrs, false);
    EXPECT_FALSE(nexus::is_transactional(attrs));
    EXPECT_TRUE(nexus::is_control(attrs));
}

// --- Round-trip clave/valor ----------------------------------------------

TEST(ControlRecord, EncodeDecode_Commit_RoundTrip) {
    const EndTxnMarker marker{ControlRecordType::Commit, 42, nexus::kControlRecordVersion};
    const auto key = nexus::encode_control_key(marker);
    const auto value = nexus::encode_control_value(marker);
    EXPECT_EQ(key.size(), nexus::kControlKeySize);
    EXPECT_EQ(value.size(), nexus::kControlValueSize);

    const auto decoded = nexus::decode_end_txn_marker(ByteSpan{key}, ByteSpan{value});
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, marker);
    EXPECT_EQ(decoded->type, ControlRecordType::Commit);
    EXPECT_EQ(decoded->coordinator_epoch, 42);
}

TEST(ControlRecord, EncodeDecode_Abort_RoundTrip) {
    const EndTxnMarker marker{ControlRecordType::Abort, 0, nexus::kControlRecordVersion};
    const auto decoded = nexus::decode_end_txn_marker(
        ByteSpan{nexus::encode_control_key(marker)}, ByteSpan{nexus::encode_control_value(marker)});
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->type, ControlRecordType::Abort);
}

// --- Decodificador defensivo ---------------------------------------------

TEST(ControlRecord, Decode_ClaveTruncada_Corrupt) {
    const EndTxnMarker marker{ControlRecordType::Commit, 1, 0};
    const auto value = nexus::encode_control_value(marker);
    const std::vector<std::byte> short_key(nexus::kControlKeySize - 1);
    const auto r = nexus::decode_end_txn_marker(ByteSpan{short_key}, ByteSpan{value});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(ControlRecord, Decode_ValorTruncado_Corrupt) {
    const EndTxnMarker marker{ControlRecordType::Commit, 1, 0};
    const auto key = nexus::encode_control_key(marker);
    const std::vector<std::byte> short_value(nexus::kControlValueSize - 1);
    const auto r = nexus::decode_end_txn_marker(ByteSpan{key}, ByteSpan{short_value});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(ControlRecord, Decode_TipoDesconocido_Corrupt) {
    // Clave con type=7 (no Abort/Commit).
    std::vector<std::byte> key(nexus::kControlKeySize);
    nexus::store_le<std::int16_t>(nexus::kControlRecordVersion, nexus::MutByteSpan{key}.subspan(0));
    nexus::store_le<std::int16_t>(7, nexus::MutByteSpan{key}.subspan(2));
    const auto value = nexus::encode_control_value({ControlRecordType::Commit, 1, 0});
    const auto r = nexus::decode_end_txn_marker(ByteSpan{key}, ByteSpan{value});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(ControlRecord, Decode_VersionNoSoportada_Unsupported) {
    std::vector<std::byte> key(nexus::kControlKeySize);
    nexus::store_le<std::int16_t>(99, nexus::MutByteSpan{key}.subspan(0));
    nexus::store_le<std::int16_t>(1, nexus::MutByteSpan{key}.subspan(2));
    std::vector<std::byte> value(nexus::kControlValueSize);
    nexus::store_le<std::int16_t>(99, nexus::MutByteSpan{value}.subspan(0));
    nexus::store_le<std::int32_t>(1, nexus::MutByteSpan{value}.subspan(2));
    const auto r = nexus::decode_end_txn_marker(ByteSpan{key}, ByteSpan{value});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::Unsupported);
}

TEST(ControlRecord, Decode_VersionesDiscordantes_Corrupt) {
    std::vector<std::byte> key(nexus::kControlKeySize);
    nexus::store_le<std::int16_t>(0, nexus::MutByteSpan{key}.subspan(0));
    nexus::store_le<std::int16_t>(1, nexus::MutByteSpan{key}.subspan(2));
    std::vector<std::byte> value(nexus::kControlValueSize);
    nexus::store_le<std::int16_t>(1, nexus::MutByteSpan{value}.subspan(0));  // versión distinta
    nexus::store_le<std::int32_t>(1, nexus::MutByteSpan{value}.subspan(2));
    const auto r = nexus::decode_end_txn_marker(ByteSpan{key}, ByteSpan{value});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::Corrupt);
}

// --- Batch de control ----------------------------------------------------

TEST(ControlRecord, BuildParse_Batch_RoundTrip) {
    const EndTxnMarker marker{ControlRecordType::Commit, 5, 0};
    const nexus::RecordBatch batch = nexus::build_control_batch(marker, /*producer_id=*/77,
                                                                /*producer_epoch=*/3);
    EXPECT_TRUE(nexus::is_control(batch.header().attrs));
    EXPECT_TRUE(nexus::is_transactional(batch.header().attrs));
    EXPECT_EQ(batch.header().record_count, 1);
    EXPECT_EQ(batch.header().producer_id, 77);
    EXPECT_EQ(batch.header().producer_epoch, 3);

    const auto parsed = nexus::parse_control_batch(batch);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(*parsed, marker);
}

TEST(ControlRecord, Parse_BatchNoDeControl_InvalidArgument) {
    // Batch de datos normal (sin bit de control).
    nexus::Record rec;
    rec.value = std::vector<std::byte>{std::byte{1}};
    nexus::RecordBatchBuilder builder;
    builder.add(std::move(rec));
    const nexus::RecordBatch batch = builder.build();
    const auto parsed = nexus::parse_control_batch(batch);
    ASSERT_FALSE(parsed.has_value());
    EXPECT_EQ(parsed.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(ControlRecord, Parse_BatchDeControlConVariosRecords_InvalidArgument) {
    nexus::Record a;
    a.key = nexus::encode_control_key({ControlRecordType::Commit, 1, 0});
    a.value = nexus::encode_control_value({ControlRecordType::Commit, 1, 0});
    nexus::Record b = a;
    nexus::RecordBatchBuilder builder;
    builder.add(std::move(a));
    builder.add(std::move(b));
    nexus::RecordBatchHeader header;
    header.attrs = nexus::attrs_with_control(0, true);
    const nexus::RecordBatch batch = builder.build(header);
    const auto parsed = nexus::parse_control_batch(batch);
    ASSERT_FALSE(parsed.has_value());
    EXPECT_EQ(parsed.error().code(), nexus::ErrorCode::InvalidArgument);
}

// --- Property-based -------------------------------------------------------

TEST(ControlRecord, Property_RoundTrip_MarcadoresAleatorios) {
    std::mt19937 rng{0xC0FFEE};
    std::uniform_int_distribution<std::int32_t> epoch_dist{-1, 1'000'000};
    std::bernoulli_distribution commit_dist{0.5};
    for (int i = 0; i < 5000; ++i) {
        const EndTxnMarker marker{
            commit_dist(rng) ? ControlRecordType::Commit : ControlRecordType::Abort,
            epoch_dist(rng), nexus::kControlRecordVersion};

        // Round-trip vía clave/valor.
        const auto via_kv =
            nexus::decode_end_txn_marker(ByteSpan{nexus::encode_control_key(marker)},
                                         ByteSpan{nexus::encode_control_value(marker)});
        ASSERT_TRUE(via_kv.has_value());
        EXPECT_EQ(*via_kv, marker);

        // Round-trip vía batch de control completo.
        const nexus::RecordBatch batch = nexus::build_control_batch(marker, /*producer_id=*/i, 0);
        const auto via_batch = nexus::parse_control_batch(batch);
        ASSERT_TRUE(via_batch.has_value());
        EXPECT_EQ(*via_batch, marker);
    }
}

}  // namespace
