#include "common/record.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <random>
#include <utility>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"

namespace {

nexus::RecordBatch make_batch(std::vector<std::byte> records, std::int32_t count) {
    nexus::RecordBatchHeader header;
    header.base_offset = 100;
    header.producer_id = 7;
    header.producer_epoch = 1;
    header.base_sequence = 5;
    header.record_count = count;
    return nexus::RecordBatch{header, std::move(records)};
}

TEST(RecordBatch, EncodeDecode_RoundTrip_PreservaCabeceraYRecords) {
    const std::vector<std::byte> payload{std::byte{1}, std::byte{2}, std::byte{3}};
    const nexus::RecordBatch original = make_batch(payload, 3);
    nexus::Buffer buf;
    original.encode(buf);

    const nexus::expected<nexus::RecordBatch> decoded = nexus::RecordBatch::decode(buf.as_span());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->header().base_offset, 100);
    EXPECT_EQ(decoded->header().producer_id, 7);
    EXPECT_EQ(decoded->header().record_count, 3);
    EXPECT_EQ(decoded->last_offset(), 102);
    ASSERT_EQ(decoded->records().size(), 3U);
    EXPECT_EQ(std::to_integer<int>(decoded->records()[0]), 1);
}

TEST(RecordBatch, Decode_Truncado_DevuelveCorrupt) {
    nexus::Buffer buf;
    make_batch({std::byte{9}}, 1).encode(buf);
    const nexus::ByteSpan full = buf.as_span();
    const auto r = nexus::RecordBatch::decode(full.subspan(0, full.size() - 1));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(RecordBatch, Decode_BitVolteado_DetectadoPorCrc) {
    nexus::Buffer buf;
    make_batch({std::byte{0xAA}, std::byte{0xBB}}, 2).encode(buf);
    std::vector<std::byte> bytes{buf.as_span().begin(), buf.as_span().end()};
    bytes.back() ^= std::byte{0xFF};  // corrompe un byte del payload
    const auto r = nexus::RecordBatch::decode(nexus::ByteSpan{bytes});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(RecordBatch, RoundTrip_PayloadsAleatorios_Property) {
    std::mt19937_64 rng{0xBEEFU};
    std::uniform_int_distribution<int> byte_dist{0, 255};
    for (const std::size_t len : {0U, 1U, 13U, 100U, 1000U}) {
        std::vector<std::byte> payload(len);
        for (std::byte& b : payload) {
            b = static_cast<std::byte>(byte_dist(rng));
        }
        nexus::Buffer buf;
        make_batch(payload, static_cast<std::int32_t>(len)).encode(buf);
        const auto decoded = nexus::RecordBatch::decode(buf.as_span());
        ASSERT_TRUE(decoded.has_value()) << "len=" << len;
        const nexus::ByteSpan recs = decoded->records();
        EXPECT_TRUE(std::equal(payload.begin(), payload.end(), recs.begin(), recs.end()))
            << "len=" << len;
    }
}

}  // namespace
