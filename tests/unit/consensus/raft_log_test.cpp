// Pruebas de RaftLog: vista (term,index) sobre un PartitionLog (ADR-0014). El índice es el ordinal
// de entrada; cada entrada es un RecordBatch que el PartitionLog ubica en su espacio de offsets.
#include "consensus/raft_log.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "consensus/raft_state.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_raftlog_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }
    [[nodiscard]] std::string meta_path() const { return (path_ / "raft-meta").string(); }
    [[nodiscard]] std::filesystem::path log_dir() const { return path_ / "log"; }

private:
    std::filesystem::path path_;
};

// Entrada de datos cuyo payload es un RecordBatch de `records` records (base_offset 0: lo reasigna
// el PartitionLog). `index` es informativo aquí (lo asigna el ordinal al anexar).
nexus::RaftLogEntry data_entry(nexus::Term term, nexus::Index index, std::int32_t records) {
    nexus::RecordBatchHeader header;
    header.base_offset = 0;
    header.record_count = records;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(8, std::byte{0xCD})};
    nexus::Buffer buffer;
    batch.encode(buffer);
    const nexus::ByteSpan span = buffer.as_span();
    return nexus::RaftLogEntry{.term = term,
                               .index = index,
                               .type = nexus::RaftEntryType::Data,
                               .payload = std::vector<std::byte>(span.begin(), span.end())};
}

// Anexa una sola entrada (azúcar).
nexus::expected<nexus::Index> append_one(nexus::RaftLog& log, nexus::Term term, nexus::Index index,
                                         std::int32_t records) {
    const nexus::RaftLogEntry entry = data_entry(term, index, records);
    const std::vector<nexus::RaftLogEntry> batch{entry};
    return log.append(batch);
}

TEST(RaftLog, Vacio_LastIndexYLastTermEnCero) {
    const TempDir dir("vacio");
    auto plog = nexus::PartitionLog::open(dir.log_dir(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    auto rlog = nexus::RaftLog::open(*plog, dir.meta_path());
    ASSERT_TRUE(rlog.has_value());
    EXPECT_EQ(rlog->last_index(), 0);
    EXPECT_EQ(rlog->last_term(), 0);
    EXPECT_EQ(rlog->term_at(0).value(), 0);  // centinela
}

TEST(RaftLog, Append_AsignaIndicesContiguosYTerminos) {
    const TempDir dir("append");
    auto plog = nexus::PartitionLog::open(dir.log_dir(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    auto rlog = nexus::RaftLog::open(*plog, dir.meta_path());
    ASSERT_TRUE(rlog.has_value());

    ASSERT_TRUE(append_one(*rlog, 1, 1, 1).has_value());
    ASSERT_TRUE(append_one(*rlog, 1, 2, 1).has_value());
    const auto last = append_one(*rlog, 2, 3, 1);
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 3);
    EXPECT_EQ(rlog->last_index(), 3);
    EXPECT_EQ(rlog->last_term(), 2);
    EXPECT_EQ(rlog->term_at(1).value(), 1);
    EXPECT_EQ(rlog->term_at(3).value(), 2);
    EXPECT_FALSE(rlog->term_at(4).has_value());  // fuera de rango alto
}

TEST(RaftLog, OffsetsAt_MapeaAlEspacioDeOffsetsPorRecord) {
    const TempDir dir("offsets");
    auto plog = nexus::PartitionLog::open(dir.log_dir(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    auto rlog = nexus::RaftLog::open(*plog, dir.meta_path());
    ASSERT_TRUE(rlog.has_value());

    ASSERT_TRUE(append_one(*rlog, 1, 1, 3).has_value());  // offsets 0..2
    ASSERT_TRUE(append_one(*rlog, 1, 2, 2).has_value());  // offsets 3..4

    const auto first = rlog->offsets_at(1);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->first, 0);
    EXPECT_EQ(first->second, 2);
    const auto second = rlog->offsets_at(2);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->first, 3);
    EXPECT_EQ(second->second, 4);
}

TEST(RaftLog, EntriesFrom_DevuelveDesdeIndiceYRespetaMax) {
    const TempDir dir("entries");
    auto plog = nexus::PartitionLog::open(dir.log_dir(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    auto rlog = nexus::RaftLog::open(*plog, dir.meta_path());
    ASSERT_TRUE(rlog.has_value());
    ASSERT_TRUE(append_one(*rlog, 1, 1, 1).has_value());
    ASSERT_TRUE(append_one(*rlog, 2, 2, 1).has_value());
    ASSERT_TRUE(append_one(*rlog, 2, 3, 1).has_value());

    const auto from2 = rlog->entries_from(2, 10);
    ASSERT_TRUE(from2.has_value());
    ASSERT_EQ(from2->size(), 2U);
    EXPECT_EQ((*from2)[0].index, 2);
    EXPECT_EQ((*from2)[0].term, 2);
    EXPECT_EQ((*from2)[1].index, 3);
    // El payload decodifica como un RecordBatch íntegro.
    const auto batch = nexus::RecordBatch::decode(nexus::ByteSpan{(*from2)[0].payload});
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->header().record_count, 1);

    const auto limited = rlog->entries_from(1, 2);
    ASSERT_TRUE(limited.has_value());
    EXPECT_EQ(limited->size(), 2U);  // respeta max
}

TEST(RaftLog, TruncateFrom_EliminaLaCola) {
    const TempDir dir("truncate");
    auto plog = nexus::PartitionLog::open(dir.log_dir(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    auto rlog = nexus::RaftLog::open(*plog, dir.meta_path());
    ASSERT_TRUE(rlog.has_value());
    ASSERT_TRUE(append_one(*rlog, 1, 1, 1).has_value());
    ASSERT_TRUE(append_one(*rlog, 1, 2, 1).has_value());
    ASSERT_TRUE(append_one(*rlog, 2, 3, 1).has_value());

    ASSERT_TRUE(rlog->truncate_from(2).has_value());  // borra índices 2 y 3
    EXPECT_EQ(rlog->last_index(), 1);
    EXPECT_EQ(rlog->last_term(), 1);
    EXPECT_FALSE(rlog->term_at(2).has_value());
}

TEST(RaftLog, TruncateThenAppend_ReasignaIndices) {
    const TempDir dir("trunc_append");
    auto plog = nexus::PartitionLog::open(dir.log_dir(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    auto rlog = nexus::RaftLog::open(*plog, dir.meta_path());
    ASSERT_TRUE(rlog.has_value());
    ASSERT_TRUE(append_one(*rlog, 1, 1, 1).has_value());
    ASSERT_TRUE(append_one(*rlog, 1, 2, 1).has_value());
    ASSERT_TRUE(rlog->truncate_from(2).has_value());

    const auto last = append_one(*rlog, 3, 2, 1);  // reocupa el índice 2 con un término nuevo
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 2);
    EXPECT_EQ(rlog->term_at(2).value(), 3);
}

TEST(RaftLog, TruncateFrom_MasAllaDelFinal_EsNoOp) {
    const TempDir dir("trunc_noop");
    auto plog = nexus::PartitionLog::open(dir.log_dir(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    auto rlog = nexus::RaftLog::open(*plog, dir.meta_path());
    ASSERT_TRUE(rlog.has_value());
    ASSERT_TRUE(append_one(*rlog, 1, 1, 1).has_value());
    ASSERT_TRUE(rlog->truncate_from(5).has_value());
    EXPECT_EQ(rlog->last_index(), 1);
}

TEST(RaftLog, Reopen_RecuperaIndicesYTerminos) {
    const TempDir dir("reopen");
    {
        auto plog = nexus::PartitionLog::open(dir.log_dir(), nexus::LogConfig{});
        ASSERT_TRUE(plog.has_value());
        auto rlog = nexus::RaftLog::open(*plog, dir.meta_path());
        ASSERT_TRUE(rlog.has_value());
        ASSERT_TRUE(append_one(*rlog, 1, 1, 2).has_value());  // offsets 0..1
        ASSERT_TRUE(append_one(*rlog, 3, 2, 1).has_value());  // offset 2
        ASSERT_TRUE(plog->sync().has_value());
    }
    auto plog = nexus::PartitionLog::open(dir.log_dir(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    auto rlog = nexus::RaftLog::open(*plog, dir.meta_path());
    ASSERT_TRUE(rlog.has_value());
    EXPECT_EQ(rlog->last_index(), 2);
    EXPECT_EQ(rlog->last_term(), 3);
    EXPECT_EQ(rlog->term_at(1).value(), 1);
    const auto offsets = rlog->offsets_at(1);
    ASSERT_TRUE(offsets.has_value());
    EXPECT_EQ(offsets->second, 1);  // la primera entrada cubre offsets 0..1
}

}  // namespace
