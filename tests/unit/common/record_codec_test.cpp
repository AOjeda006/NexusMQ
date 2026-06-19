// Pruebas del codec por record: Record (key/value/headers anulables) + RecordBatchBuilder.
#include "common/record_codec.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
#include <optional>
#include <string_view>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"

namespace {

using nexus::ByteSpan;
using nexus::Record;
using nexus::RecordBatch;
using nexus::RecordBatchBuilder;
using nexus::RecordHeader;

// Bytes a partir de una cadena (comodidad de test).
std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> out(text.size());
    if (!text.empty()) {
        std::memcpy(out.data(), text.data(), text.size());
    }
    return out;
}

// Record solo con valor (sin clave): atajo de test.
Record value_record(std::string_view value) {
    Record rec;
    rec.value = bytes(value);
    return rec;
}

// Round-trip de un único record vía cursor.
Record round_trip(const Record& rec, std::int64_t offset_delta, nexus::Offset base_offset) {
    nexus::Buffer buf;
    nexus::encode_record(rec, offset_delta, buf);
    ByteSpan cursor = buf.as_span();
    const nexus::expected<Record> decoded = nexus::decode_record(cursor, base_offset);
    EXPECT_TRUE(decoded.has_value());
    EXPECT_TRUE(cursor.empty()) << "el cursor debe quedar consumido";
    return decoded.value_or(Record{});
}

TEST(RecordCodec, EncodeDecode_KeyYValue_RoundTrip) {
    Record rec;
    rec.key = bytes("clave");
    rec.value = bytes("valor");
    const Record back = round_trip(rec, /*offset_delta=*/0, /*base_offset=*/0);
    EXPECT_EQ(back.key, rec.key);
    EXPECT_EQ(back.value, rec.value);
    EXPECT_TRUE(back.headers.empty());
    EXPECT_EQ(back.offset, 0);
}

TEST(RecordCodec, EncodeDecode_Tombstone_ValueNulo) {
    Record rec;
    rec.key = bytes("k");
    rec.value = std::nullopt;  // tombstone
    const Record back = round_trip(rec, 0, 0);
    EXPECT_EQ(back.key, rec.key);
    EXPECT_FALSE(back.value.has_value());
}

TEST(RecordCodec, EncodeDecode_ClaveNula_RoundTrip) {
    Record rec;
    rec.key = std::nullopt;
    rec.value = bytes("solo valor");
    const Record back = round_trip(rec, 0, 0);
    EXPECT_FALSE(back.key.has_value());
    EXPECT_EQ(back.value, rec.value);
}

TEST(RecordCodec, EncodeDecode_OffsetAbsoluto_DerivaDeBaseMasDelta) {
    Record rec;
    rec.value = bytes("v");
    const Record back = round_trip(rec, /*offset_delta=*/3, /*base_offset=*/100);
    EXPECT_EQ(back.offset, 103);
}

TEST(RecordCodec, EncodeDecode_Headers_RoundTripConValorNulo) {
    Record rec;
    rec.key = bytes("k");
    rec.value = bytes("v");
    rec.headers.push_back(RecordHeader{.key = "trace-id", .value = bytes("abc123")});
    rec.headers.push_back(RecordHeader{.key = "sin-valor", .value = std::nullopt});
    const Record back = round_trip(rec, 0, 0);
    ASSERT_EQ(back.headers.size(), 2U);
    EXPECT_EQ(back.headers[0].key, "trace-id");
    EXPECT_EQ(back.headers[0].value, bytes("abc123"));
    EXPECT_EQ(back.headers[1].key, "sin-valor");
    EXPECT_FALSE(back.headers[1].value.has_value());
}

TEST(RecordBatchBuilderTest, Build_AsignaOffsetsSecuenciales) {
    RecordBatchBuilder builder;
    builder.add(value_record("a")).add(value_record("b"));
    Record c;
    c.key = bytes("ck");
    c.value = bytes("c");
    builder.add(std::move(c));

    nexus::RecordBatchHeader header;
    header.base_offset = 50;  // como si el log lo hubiera asignado
    const RecordBatch batch = builder.build(header);
    EXPECT_EQ(batch.header().record_count, 3);

    const nexus::expected<std::vector<Record>> recs = nexus::decode_records(batch);
    ASSERT_TRUE(recs.has_value());
    ASSERT_EQ(recs->size(), 3U);
    EXPECT_EQ((*recs)[0].offset, 50);
    EXPECT_EQ((*recs)[1].offset, 51);
    EXPECT_EQ((*recs)[2].offset, 52);
    EXPECT_EQ((*recs)[0].value, bytes("a"));
    EXPECT_EQ((*recs)[2].key, bytes("ck"));
}

TEST(RecordCodec, DecodeRecord_Truncado_DevuelveCorrupt) {
    Record rec;
    rec.key = bytes("clave");
    rec.value = bytes("valor larguísimo");
    nexus::Buffer buf;
    nexus::encode_record(rec, 0, buf);
    // Recorta el último byte: el cuerpo no cabe → Corrupt.
    const ByteSpan full = buf.as_span();
    ByteSpan truncated = full.subspan(0, full.size() - 1);
    const nexus::expected<Record> decoded = nexus::decode_record(truncated, 0);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(RecordCodec, DecodeRecords_CuentaMayorQueElBlob_DevuelveCorrupt) {
    RecordBatchBuilder builder;
    builder.add(value_record("uno"));
    nexus::RecordBatchHeader header = builder.build().header();
    header.record_count = 5;  // miente: dice 5 records pero el blob tiene 1
    const RecordBatch batch{header, std::vector<std::byte>{}};
    const nexus::expected<std::vector<Record>> recs = nexus::decode_records(batch);
    EXPECT_FALSE(recs.has_value());
}

}  // namespace
