// Pruebas de LogCompactor: compactación por clave (último por clave + tombstones).
#include "storage/log_compactor.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/record.hpp"
#include "common/record_codec.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace {

using nexus::CompactionStats;
using nexus::LogCompactor;
using nexus::Record;

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> out(text.size());
    if (!text.empty()) {
        std::memcpy(out.data(), text.data(), text.size());
    }
    return out;
}

// Record con clave y valor, con offset explícito.
Record keyed(std::string_view key, std::string_view value, nexus::Offset offset) {
    Record rec;
    rec.key = bytes(key);
    rec.value = bytes(value);
    rec.offset = offset;
    return rec;
}

// Tombstone (value nulo) con clave y offset.
Record tombstone(std::string_view key, nexus::Offset offset) {
    Record rec;
    rec.key = bytes(key);
    rec.value = std::nullopt;
    rec.offset = offset;
    return rec;
}

std::string_view text(const std::optional<std::vector<std::byte>>& field) {
    return std::string_view{
        reinterpret_cast<const char*>(field->data()),  // NOLINT(*-reinterpret-*)
        field->size()};
}

TEST(LogCompactor, Compact_ConservaUltimoPorClave) {
    const std::vector<Record> in{keyed("a", "1", 0), keyed("b", "1", 1), keyed("a", "2", 2),
                                 keyed("a", "3", 3), keyed("b", "2", 4)};
    CompactionStats stats;
    const std::vector<Record> out = LogCompactor{}.compact(in, &stats);

    ASSERT_EQ(out.size(), 2U);
    // Conserva la última 'a' (offset 3) y la última 'b' (offset 4), en orden de offset.
    EXPECT_EQ(text(out[0].key), "a");
    EXPECT_EQ(text(out[0].value), "3");
    EXPECT_EQ(out[0].offset, 3);
    EXPECT_EQ(text(out[1].key), "b");
    EXPECT_EQ(text(out[1].value), "2");
    EXPECT_EQ(out[1].offset, 4);

    EXPECT_EQ(stats.records_in, 5U);
    EXPECT_EQ(stats.records_kept, 2U);
    EXPECT_EQ(stats.records_superseded, 3U);
    EXPECT_EQ(stats.tombstones_dropped, 0U);
}

TEST(LogCompactor, Compact_TombstoneBorraLaClave) {
    const std::vector<Record> in{keyed("a", "1", 0), keyed("b", "1", 1), tombstone("a", 2)};
    CompactionStats stats;
    const std::vector<Record> out = LogCompactor{/*retain_tombstones=*/false}.compact(in, &stats);

    ASSERT_EQ(out.size(), 1U);  // 'a' desaparece (tombstone); queda 'b'.
    EXPECT_EQ(text(out[0].key), "b");
    EXPECT_EQ(stats.tombstones_dropped, 1U);
    EXPECT_EQ(stats.records_superseded, 1U);  // la 'a' inicial fue superada por el tombstone.
}

TEST(LogCompactor, Compact_RetainTombstones_ConservaElTombstone) {
    const std::vector<Record> in{keyed("a", "1", 0), tombstone("a", 1)};
    const std::vector<Record> out = LogCompactor{/*retain_tombstones=*/true}.compact(in);

    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(text(out[0].key), "a");
    EXPECT_FALSE(out[0].value.has_value());  // el tombstone se conserva.
    EXPECT_EQ(out[0].offset, 1);
}

TEST(LogCompactor, Compact_RecordsSinClave_SeConservanSiempre) {
    Record no_key;
    no_key.value = bytes("x");
    no_key.offset = 0;
    Record no_key2;
    no_key2.value = bytes("y");
    no_key2.offset = 2;
    const std::vector<Record> in{no_key, keyed("a", "1", 1), no_key2, keyed("a", "2", 3)};
    const std::vector<Record> out = LogCompactor{}.compact(in);

    ASSERT_EQ(out.size(), 3U);  // dos sin clave + la última 'a'.
    EXPECT_FALSE(out[0].key.has_value());
    EXPECT_FALSE(out[1].key.has_value());
    EXPECT_EQ(text(out[2].key), "a");
    EXPECT_EQ(text(out[2].value), "2");
}

TEST(LogCompactor, Compact_Vacio_DevuelveVacio) {
    const std::vector<Record> out = LogCompactor{}.compact({});
    EXPECT_TRUE(out.empty());
}

// --- Integración con un PartitionLog real ---

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_compact_" + std::string{tag} + "_" +
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

TEST(LogCompactor, CompactLog_LeeYCompactaUnLogReal) {
    TempDir dir{"real"};
    nexus::expected<nexus::PartitionLog> log =
        nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(log.has_value());

    // Tres batches, cada uno con un record con clave; 'a' se actualiza, 'b' se borra con tombstone.
    auto append_record = [&](const Record& rec) {
        nexus::RecordBatchBuilder builder;
        builder.add(rec);
        ASSERT_TRUE(log->append(builder.build()).has_value());
    };
    append_record(keyed("a", "v1", 0));
    append_record(keyed("b", "v1", 0));
    append_record(keyed("a", "v2", 0));  // el offset real lo asigna el log
    append_record(tombstone("b", 0));

    CompactionStats stats;
    const nexus::expected<std::vector<Record>> compacted = LogCompactor{}.compact_log(*log, &stats);
    ASSERT_TRUE(compacted.has_value());

    ASSERT_EQ(compacted->size(), 1U);  // solo la última 'a'; 'b' borrada por tombstone.
    EXPECT_EQ(text((*compacted)[0].key), "a");
    EXPECT_EQ(text((*compacted)[0].value), "v2");
    EXPECT_EQ((*compacted)[0].offset, 2);  // offset absoluto preservado
    EXPECT_EQ(stats.records_in, 4U);
    EXPECT_EQ(stats.tombstones_dropped, 1U);
}

}  // namespace
