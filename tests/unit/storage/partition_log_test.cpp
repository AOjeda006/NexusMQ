#include "storage/partition_log.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "common/record.hpp"
#include "common/types.hpp"
#include "storage/log_config.hpp"

namespace {

// Directorio temporal único con limpieza RAII.
class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_plog_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

// Batch de 'count' records y 'payload_len' bytes; el base_offset de entrada es indiferente
// (PartitionLog lo reasigna).
nexus::RecordBatch make_batch(std::int32_t count, std::size_t payload_len) {
    nexus::RecordBatchHeader header;
    header.base_offset = -999;  // se ignora: el log asigna el offset
    header.record_count = count;
    return nexus::RecordBatch{header, std::vector<std::byte>(payload_len, std::byte{0xCD})};
}

nexus::LogConfig small_segments() {
    nexus::LogConfig cfg;
    cfg.segment_bytes = 100;  // rota tras superar 100 bytes
    cfg.index_interval_bytes = 64;
    return cfg;
}

TEST(PartitionLog, Open_DirectorioVacio_CreaPrimerSegmento) {
    const TempDir dir("vacio");
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    EXPECT_EQ(plog->segment_count(), 1U);
    EXPECT_EQ(plog->log_start_offset(), 0);
    EXPECT_EQ(plog->log_end_offset(), 0);
}

TEST(PartitionLog, Append_AsignaOffsetsContiguos) {
    const TempDir dir("offsets");
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());

    const auto o0 = plog->append(make_batch(3, 10));  // base 0 -> last 2
    const auto o1 = plog->append(make_batch(2, 10));  // base 3 -> last 4
    const auto o2 = plog->append(make_batch(5, 10));  // base 5 -> last 9
    ASSERT_TRUE(o0.has_value() && o1.has_value() && o2.has_value());
    EXPECT_EQ(*o0, 2);
    EXPECT_EQ(*o1, 4);
    EXPECT_EQ(*o2, 9);
    EXPECT_EQ(plog->log_end_offset(), 10);
}

TEST(PartitionLog, Append_SuperaSegmentBytes_RotaSegmento) {
    const TempDir dir("rota");
    auto plog = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_TRUE(plog.has_value());
    EXPECT_EQ(plog->segment_count(), 1U);

    // Cada batch ocupa 36 + 40 = 76 bytes; el segundo supera los 100 -> rota antes del 3.º.
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    EXPECT_GT(plog->segment_count(), 1U);  // hubo al menos una rotación
    EXPECT_EQ(plog->log_end_offset(), 3);  // offsets contiguos pese a rotar
}

TEST(PartitionLog, Reopen_TrasRotacion_PreservaOffsetsYSegmentos) {
    const TempDir dir("reopen");
    std::size_t segments = 0;
    {
        auto plog = nexus::PartitionLog::open(dir.path(), small_segments());
        ASSERT_TRUE(plog.has_value());
        for (int i = 0; i < 4; ++i) {
            ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
        }
        segments = plog->segment_count();
        ASSERT_GT(segments, 1U);
    }
    auto reopened = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->segment_count(), segments);
    EXPECT_EQ(reopened->log_start_offset(), 0);
    EXPECT_EQ(reopened->log_end_offset(), 4);
}

}  // namespace
