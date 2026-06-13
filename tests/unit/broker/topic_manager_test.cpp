// Pruebas de TopicManager: creación/borrado de topics y apertura de sus particiones.
#include "broker/topic_manager.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "broker/partition.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_tm_" + std::string{tag} + "_" +
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

TEST(TopicManager, CreateTopic_AbreParticionesYQuedaAccesible) {
    TempDir dir{"create"};
    nexus::TopicManager manager{dir.path()};

    const nexus::expected<nexus::TopicMetadata> meta = manager.create_topic("eventos", 3);
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(meta->name, "eventos");
    EXPECT_EQ(meta->partition_count, 3);
    EXPECT_EQ(manager.topic_count(), 1U);

    nexus::Topic* topic = manager.get("eventos");
    ASSERT_NE(topic, nullptr);
    EXPECT_EQ(topic->partition_count(), 3U);
    EXPECT_NE(topic->partition(0), nullptr);
    EXPECT_NE(topic->partition(2), nullptr);
    EXPECT_EQ(topic->partition(3), nullptr);
}

TEST(TopicManager, CreateTopic_Duplicado_DevuelveInvalidArgument) {
    TempDir dir{"dup"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 1).has_value());

    const nexus::expected<nexus::TopicMetadata> again = manager.create_topic("t", 1);
    ASSERT_FALSE(again.has_value());
    EXPECT_EQ(again.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(TopicManager, CreateTopic_ConteoInvalido_DevuelveInvalidArgument) {
    TempDir dir{"badcount"};
    nexus::TopicManager manager{dir.path()};
    const nexus::expected<nexus::TopicMetadata> meta = manager.create_topic("t", 0);
    ASSERT_FALSE(meta.has_value());
    EXPECT_EQ(meta.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(TopicManager, Get_TopicInexistente_DevuelveNullptr) {
    TempDir dir{"absent"};
    nexus::TopicManager manager{dir.path()};
    EXPECT_EQ(manager.get("nope"), nullptr);
}

TEST(TopicManager, DeleteTopic_QuitaDelRegistro) {
    TempDir dir{"del"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 1).has_value());
    ASSERT_TRUE(manager.delete_topic("t").has_value());
    EXPECT_EQ(manager.get("t"), nullptr);
    EXPECT_EQ(manager.delete_topic("t").error().code(), nexus::ErrorCode::NotFound);
}

TEST(TopicManager, Describe_ListaTopicsConParticiones) {
    TempDir dir{"desc"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 2).has_value());

    const std::vector<nexus::TopicMeta> metas = manager.describe(/*leader_node_id=*/7);
    ASSERT_EQ(metas.size(), 1U);
    EXPECT_EQ(metas[0].name, "t");
    ASSERT_EQ(metas[0].partitions.size(), 2U);
    EXPECT_EQ(metas[0].partitions[0].leader_node_id, 7);
}

TEST(TopicManager, ProduceTrasCrear_FuncionaSobreLaParticion) {
    TempDir dir{"produce"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 1).has_value());

    nexus::Partition* part = manager.get("t")->partition(0);
    ASSERT_NE(part, nullptr);
    nexus::RecordBatchHeader header;
    header.record_count = 3;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(12, std::byte{0x5})};
    ASSERT_TRUE(part->produce(batch).has_value());
    EXPECT_EQ(part->high_watermark(), 3);
}

}  // namespace
