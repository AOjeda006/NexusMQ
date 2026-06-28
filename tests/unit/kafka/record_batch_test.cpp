// Pruebas de la inspección/reescritura de un RecordBatch v2 de Kafka — F7f.
#include "kafka/record_batch.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "kafka/codec.hpp"

namespace {

using nexus::Buffer;
using nexus::ByteSpan;
using nexus::kafka::Encoder;
using nexus::kafka::peek_record_batch;
using nexus::kafka::set_base_offset;

/// Construye un RecordBatch v2 mínimo y bien formado (CRC ficticio: `peek` no lo valida) con
/// @p base_offset, @p record_count y @p payload como records opacos.
std::vector<std::byte> make_batch(std::int64_t base_offset, std::int32_t record_count,
                                  ByteSpan payload) {
    Buffer buf;
    Encoder enc{buf};
    enc.put_i64(base_offset);
    // batchLength = bytes tras este campo = cabecera fija restante (49) + records.
    const auto batch_length = static_cast<std::int32_t>(49 + payload.size());
    enc.put_i32(batch_length);
    enc.put_i32(0);                                        // partitionLeaderEpoch
    enc.put_i8(nexus::kafka::kRecordBatchMagicV2);         // magic = 2
    enc.put_u32(0xDEADBEEF);                               // crc (ficticio)
    enc.put_i16(0);                                        // attributes
    enc.put_i32(record_count > 0 ? record_count - 1 : 0);  // lastOffsetDelta
    enc.put_i64(0);                                        // baseTimestamp
    enc.put_i64(0);                                        // maxTimestamp
    enc.put_i64(-1);                                       // producerId
    enc.put_i16(-1);                                       // producerEpoch
    enc.put_i32(-1);                                       // baseSequence
    enc.put_i32(record_count);                             // recordCount
    enc.put_raw(payload);                                  // records (opacos)
    const ByteSpan bytes = buf.as_span();
    return std::vector<std::byte>{bytes.begin(), bytes.end()};
}

TEST(RecordBatch, Peek_ValidBatch_ReadsHeaderFields) {
    const std::vector<std::byte> payload{std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}};
    const std::vector<std::byte> batch = make_batch(42, 5, ByteSpan{payload});

    const auto info = peek_record_batch(ByteSpan{batch});
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->base_offset, 42);
    EXPECT_EQ(info->record_count, 5);
    EXPECT_EQ(info->last_offset_delta, 4);
    EXPECT_EQ(info->encoded_size, batch.size());
}

TEST(RecordBatch, Peek_MultipleBatches_EncodedSizeWalksToTheNext) {
    const std::vector<std::byte> empty;
    std::vector<std::byte> first = make_batch(0, 1, ByteSpan{empty});
    const std::vector<std::byte> second = make_batch(1, 1, ByteSpan{empty});
    first.insert(first.end(), second.begin(), second.end());

    const auto info = peek_record_batch(ByteSpan{first});
    ASSERT_TRUE(info.has_value());
    // encoded_size apunta justo al inicio del segundo batch (no consume el blob entero).
    EXPECT_LT(info->encoded_size, first.size());
    EXPECT_EQ(info->encoded_size, nexus::kafka::kRecordBatchHeaderSize);
}

TEST(RecordBatch, Peek_Truncated_ReturnsCorrupt) {
    const std::vector<std::byte> empty;
    const std::vector<std::byte> batch = make_batch(0, 1, ByteSpan{empty});
    const ByteSpan truncated{batch.data(), batch.size() - 1};  // un byte de menos.

    const auto info = peek_record_batch(truncated);
    ASSERT_FALSE(info.has_value());
    EXPECT_EQ(info.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(RecordBatch, Peek_WrongMagic_ReturnsCorrupt) {
    const std::vector<std::byte> empty;
    std::vector<std::byte> batch = make_batch(0, 1, ByteSpan{empty});
    batch[16] = std::byte{1};  // magic en el offset 16: lo degradamos a v1.

    const auto info = peek_record_batch(ByteSpan{batch});
    ASSERT_FALSE(info.has_value());
    EXPECT_EQ(info.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(RecordBatch, SetBaseOffset_RewritesFirstEightBytesBigEndian) {
    const std::vector<std::byte> empty;
    std::vector<std::byte> batch = make_batch(0, 1, ByteSpan{empty});

    set_base_offset(std::span<std::byte>{batch}, 0x0102030405060708);

    // big-endian: el byte más significativo primero.
    EXPECT_EQ(batch[0], std::byte{0x01});
    EXPECT_EQ(batch[7], std::byte{0x08});
    const auto info = peek_record_batch(ByteSpan{batch});
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->base_offset, 0x0102030405060708);
}

}  // namespace
