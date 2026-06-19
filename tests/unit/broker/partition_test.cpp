// Pruebas de Partition: produce idempotente + fetch sobre un PartitionLog real (dir temporal).
#include "broker/partition.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace {

// Directorio temporal único con limpieza RAII.
class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_part_" + std::string{tag} + "_" +
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

nexus::Partition open_partition(const TempDir& dir) {
    nexus::expected<nexus::PartitionLog> log =
        nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    EXPECT_TRUE(log.has_value());
    return nexus::Partition{std::move(*log)};
}

// Batch con campos de idempotencia explícitos y un payload de `count*4` bytes.
nexus::RecordBatch make_batch(nexus::ProducerId producer_id, nexus::Sequence base_seq,
                              std::int32_t count, std::int16_t epoch = 0) {
    nexus::RecordBatchHeader header;
    header.producer_id = producer_id;
    header.producer_epoch = epoch;
    header.base_sequence = base_seq;
    header.record_count = count;
    return nexus::RecordBatch{
        header, std::vector<std::byte>(static_cast<std::size_t>(count) * 4, std::byte{0xAB})};
}

TEST(Partition, Produce_NoIdempotente_AnexaYAvanzaHighWatermark) {
    TempDir dir{"noidem"};
    nexus::Partition partition = open_partition(dir);

    const nexus::expected<nexus::Offset> first = partition.produce(make_batch(-1, -1, 3));
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, 2);  // base 0, count 3 → último offset 2
    EXPECT_EQ(partition.high_watermark(), 3);

    const nexus::expected<nexus::Offset> second = partition.produce(make_batch(-1, -1, 2));
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(*second, 4);
    EXPECT_EQ(partition.high_watermark(), 5);
}

TEST(Partition, Produce_Idempotente_SecuenciaContigua_Acepta) {
    TempDir dir{"idem_ok"};
    nexus::Partition partition = open_partition(dir);

    ASSERT_TRUE(partition.produce(make_batch(/*pid=*/1, /*base=*/0, /*count=*/3)).has_value());
    const nexus::expected<nexus::Offset> next = partition.produce(make_batch(1, 3, 2));
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(partition.high_watermark(), 5);
}

TEST(Partition, Produce_Idempotente_Duplicado_NoReanexa) {
    TempDir dir{"idem_dup"};
    nexus::Partition partition = open_partition(dir);

    ASSERT_TRUE(partition.produce(make_batch(1, 0, 3)).has_value());
    const nexus::Offset hw_before = partition.high_watermark();
    const nexus::expected<nexus::Offset> dup = partition.produce(make_batch(1, 0, 3));
    ASSERT_TRUE(dup.has_value());                      // se reconoce sin error
    EXPECT_EQ(partition.high_watermark(), hw_before);  // no re-anexa
}

TEST(Partition, Produce_Idempotente_Duplicado_DevuelveOffsetOriginal) {
    TempDir dir{"idem_dup_off"};
    nexus::Partition partition = open_partition(dir);

    // Dos batches del mismo productor: [0,2] → offsets [0,2]; [3,4] → offsets [3,4].
    ASSERT_TRUE(partition.produce(make_batch(1, 0, 3)).has_value());
    const nexus::expected<nexus::Offset> second = partition.produce(make_batch(1, 3, 2));
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(*second, 4);
    // Reintento del último batch: devuelve su último offset original (4), no el final del log.
    const nexus::expected<nexus::Offset> dup = partition.produce(make_batch(1, 3, 2));
    ASSERT_TRUE(dup.has_value());
    EXPECT_EQ(*dup, 4);
}

TEST(Partition, Produce_EpocaObsoleta_DevuelveFenced) {
    TempDir dir{"fenced"};
    nexus::Partition partition = open_partition(dir);

    // El productor 1 escribe con época 5.
    ASSERT_TRUE(partition.produce(make_batch(1, 0, 3, /*epoch=*/5)).has_value());
    const nexus::Offset hw_before = partition.high_watermark();
    // Un zombi con época anterior queda expulsado: no escribe aunque la secuencia continúe.
    const nexus::expected<nexus::Offset> fenced =
        partition.produce(make_batch(1, 3, 1, /*epoch=*/4));
    ASSERT_FALSE(fenced.has_value());
    EXPECT_EQ(fenced.error().code(), nexus::ErrorCode::Fenced);
    EXPECT_EQ(partition.high_watermark(), hw_before);  // nada anexado
}

TEST(Partition, Produce_NuevaEpoca_ReiniciaSecuencia) {
    TempDir dir{"epoch_bump"};
    nexus::Partition partition = open_partition(dir);

    ASSERT_TRUE(partition.produce(make_batch(1, 0, 3, /*epoch=*/0)).has_value());
    // Encarnación nueva (época 1) tras reinicio del productor: su secuencia arranca en 0 de nuevo.
    const nexus::expected<nexus::Offset> resumed =
        partition.produce(make_batch(1, 0, 2, /*epoch=*/1));
    ASSERT_TRUE(resumed.has_value());
    EXPECT_EQ(*resumed, 4);  // se anexa tras el batch previo: offsets [3,4]
    EXPECT_EQ(partition.high_watermark(), 5);
}

TEST(Partition, Produce_Idempotente_Hueco_DevuelveOutOfRange) {
    TempDir dir{"idem_gap"};
    nexus::Partition partition = open_partition(dir);

    const nexus::expected<nexus::Offset> gap =
        partition.produce(make_batch(/*pid=*/2, /*base=*/5, 1));
    ASSERT_FALSE(gap.has_value());
    EXPECT_EQ(gap.error().code(), nexus::ErrorCode::OutOfRange);
    EXPECT_EQ(partition.high_watermark(), 0);  // nada anexado
}

TEST(Partition, Fetch_TrasProduce_DevuelveBatches) {
    TempDir dir{"fetch"};
    nexus::Partition partition = open_partition(dir);
    ASSERT_TRUE(partition.produce(make_batch(-1, -1, 4)).has_value());

    const nexus::expected<nexus::FetchResult> result = partition.fetch(0, 64U * 1024U);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->batches.empty());
    EXPECT_EQ(result->next_offset, partition.high_watermark());
}

}  // namespace
