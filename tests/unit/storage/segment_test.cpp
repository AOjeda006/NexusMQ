#include "storage/segment.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "io/file.hpp"

namespace {

// Crea (y limpia con RAII) un directorio temporal único para un segmento.
class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_segment_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

// Batch con 'count' records de 'payload_len' bytes (contenido 0xAB) y el base_offset dado.
nexus::RecordBatch make_batch(nexus::Offset base_offset, std::int32_t count,
                              std::size_t payload_len) {
    nexus::RecordBatchHeader header;
    header.base_offset = base_offset;
    header.record_count = count;
    return nexus::RecordBatch{header, std::vector<std::byte>(payload_len, std::byte{0xAB})};
}

// Segmento con 4 batches de 3 records y payload 10 (encoded_size 46 c/u):
// offsets b0 0..2, b1 3..5, b2 6..8, b3 9..11. Con interval 64 el índice ancla en b2 (pos 92).
nexus::Segment filled_segment(const std::filesystem::path& dir) {
    auto seg = nexus::Segment::create(dir, /*base_offset=*/0, /*interval=*/64);
    EXPECT_TRUE(seg.has_value());
    for (const nexus::Offset base : {0, 3, 6, 9}) {
        EXPECT_TRUE(seg->append(make_batch(base, 3, 10)).has_value());
    }
    return std::move(*seg);
}

TEST(Segment, Append_VariosBatches_DevuelveUltimoOffsetYCreceTamano) {
    const TempDir dir("append");
    auto seg = nexus::Segment::create(dir.path(), /*base_offset=*/0, /*interval=*/64);
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->base_offset(), 0);
    EXPECT_EQ(seg->size_bytes(), 0U);

    const auto off0 = seg->append(make_batch(0, 3, 10));  // offsets 0..2
    const auto off1 = seg->append(make_batch(3, 2, 10));  // offsets 3..4
    const auto off2 = seg->append(make_batch(5, 5, 10));  // offsets 5..9
    ASSERT_TRUE(off0.has_value() && off1.has_value() && off2.has_value());
    EXPECT_EQ(*off0, 2);
    EXPECT_EQ(*off1, 4);
    EXPECT_EQ(*off2, 9);
    EXPECT_GT(seg->size_bytes(), 0U);
}

TEST(Segment, IsFull_SuperaUmbral_DevuelveTrue) {
    const TempDir dir("full");
    auto seg = nexus::Segment::create(dir.path(), 0, 64);
    ASSERT_TRUE(seg.has_value());
    EXPECT_FALSE(seg->is_full(1024));
    ASSERT_TRUE(seg->append(make_batch(0, 1, 200)).has_value());
    EXPECT_TRUE(seg->is_full(64));  // ya supera 64 bytes
    EXPECT_FALSE(seg->is_full(100000));
}

TEST(Segment, SealYReopen_PreservaTamanoYContenidoEnDisco) {
    const TempDir dir("reopen");
    nexus::Offset off_seen = -1;
    std::size_t bytes_seen = 0;
    {
        auto seg = nexus::Segment::create(dir.path(), 0, 64);
        ASSERT_TRUE(seg.has_value());
        const auto off = seg->append(make_batch(0, 4, 16));
        ASSERT_TRUE(off.has_value());
        off_seen = *off;
        bytes_seen = seg->size_bytes();
        ASSERT_TRUE(seg->seal().has_value());
        EXPECT_EQ(seg->state(), nexus::Segment::State::Closed);
    }
    auto reopened = nexus::Segment::open(dir.path(), 0, 64);
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->size_bytes(), bytes_seen);

    // El primer batch del .log decodifica correctamente y conserva su último offset.
    auto log = nexus::File::open((dir.path() / "00000000000000000000.log").string(),
                                 nexus::File::Mode::ReadOnly);
    ASSERT_TRUE(log.has_value());
    std::vector<std::byte> raw(bytes_seen);
    ASSERT_TRUE(log->read_at(raw, 0).has_value());
    const auto decoded = nexus::RecordBatch::decode(nexus::ByteSpan{raw});
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->last_offset(), off_seen);
}

TEST(Segment, Read_DesdeOffsetMedio_DevuelveBatchDesdeEseOffset) {
    const TempDir dir("read_medio");
    const auto seg = filled_segment(dir.path());
    const auto fr = seg.read(4, 100000);
    ASSERT_TRUE(fr.has_value());
    const auto first = nexus::RecordBatch::decode(fr->batches.as_span());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->header().base_offset, 3);  // el batch que contiene el offset 4
    EXPECT_EQ(fr->next_offset, 12);             // tras el último batch (last 11)
}

TEST(Segment, Read_RespetaMaxBytes_DevuelveAlMenosUnBatch) {
    const TempDir dir("read_max");
    const auto seg = filled_segment(dir.path());
    const auto fr = seg.read(0, 46);  // encoded_size de un batch = 36 + 10
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->batches.size(), 46U);  // exactamente un batch
    EXPECT_EQ(fr->next_offset, 3);       // tras b0 (last 2)
}

TEST(Segment, Read_OffsetMasAllaDelFinal_DevuelveVacio) {
    const TempDir dir("read_fin");
    const auto seg = filled_segment(dir.path());
    const auto fr = seg.read(100, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_TRUE(fr->batches.empty());
    EXPECT_EQ(fr->next_offset, 100);
}

TEST(Segment, Read_SeekViaIndice_LocalizaBatchCorrecto) {
    const TempDir dir("read_seek");
    const auto seg = filled_segment(dir.path());
    const auto fr = seg.read(7, 100000);  // floor(7) -> ancla {6,92}; salta b0/b1
    ASSERT_TRUE(fr.has_value());
    const auto first = nexus::RecordBatch::decode(fr->batches.as_span());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->header().base_offset, 6);
    EXPECT_EQ(fr->next_offset, 12);
}

TEST(Segment, Append_EnSegmentoSellado_DevuelveInvalidArgument) {
    const TempDir dir("sellado");
    auto seg = nexus::Segment::create(dir.path(), 0, 64);
    ASSERT_TRUE(seg.has_value());
    ASSERT_TRUE(seg->seal().has_value());
    const auto off = seg->append(make_batch(0, 1, 8));
    ASSERT_FALSE(off.has_value());
    EXPECT_EQ(off.error().code(), nexus::ErrorCode::InvalidArgument);
}

}  // namespace
