// Pruebas de Topic: contenedor de particiones + metadatos. Las particiones usan logs reales.
#include "broker/topic.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "broker/partition.hpp"
#include "broker/partition_base.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_topic_" + std::string{tag} + "_" +
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

std::unique_ptr<nexus::Partition> open_partition(const std::filesystem::path& dir) {
    nexus::expected<nexus::PartitionLog> log = nexus::PartitionLog::open(dir, nexus::LogConfig{});
    EXPECT_TRUE(log.has_value());
    return std::make_unique<nexus::Partition>(std::move(*log));
}

nexus::TopicMetadata make_meta(const char* name, std::int32_t partitions) {
    nexus::TopicMetadata meta;
    meta.name = name;
    meta.partition_count = partitions;
    return meta;
}

TEST(Topic, Meta_GuardaNombreYConfiguracion) {
    nexus::Topic topic{make_meta("eventos", 3)};
    EXPECT_EQ(topic.meta().name, "eventos");
    EXPECT_EQ(topic.meta().partition_count, 3);
    EXPECT_EQ(topic.meta().replication_factor, 1);
    EXPECT_EQ(topic.partition_count(), 0U);  // aún sin instalar particiones
}

TEST(Topic, AddPartition_QuedaAccesiblePorId) {
    TempDir dir{"add"};
    nexus::Topic topic{make_meta("t", 2)};
    topic.add_partition(0, open_partition(dir.path() / "p0"));
    topic.add_partition(1, open_partition(dir.path() / "p1"));

    EXPECT_EQ(topic.partition_count(), 2U);
    ASSERT_NE(topic.partition(0), nullptr);
    ASSERT_NE(topic.partition(1), nullptr);
    EXPECT_EQ(topic.partition(2), nullptr);  // inexistente
}

TEST(Topic, Partition_PermiteProduceYFetch) {
    TempDir dir{"use"};
    nexus::Topic topic{make_meta("t", 1)};
    topic.add_partition(0, open_partition(dir.path() / "p0"));

    // Se sirve por la interfaz base (PartitionBase), sin conocer el tipo concreto.
    nexus::PartitionBase* part = topic.partition(0);
    ASSERT_NE(part, nullptr);
    nexus::RecordBatchHeader header;
    header.record_count = 2;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(8, std::byte{0x1})};
    ASSERT_TRUE(part->produce(batch).has_value());
    EXPECT_EQ(part->high_watermark(), 2);
}

}  // namespace
