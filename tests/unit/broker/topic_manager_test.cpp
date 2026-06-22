// Pruebas de TopicManager: creación/borrado de topics y apertura de sus particiones.
#include "broker/topic_manager.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "broker/partition.hpp"
#include "broker/partition_base.hpp"
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

    nexus::PartitionBase* part = manager.get("t")->partition(0);
    ASSERT_NE(part, nullptr);
    nexus::RecordBatchHeader header;
    header.record_count = 3;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(12, std::byte{0x5})};
    ASSERT_TRUE(part->produce(batch).has_value());
    EXPECT_EQ(part->high_watermark(), 3);
}

TEST(TopicManager, Sharding_AbreSoloLasParticionesDelNucleo) {
    TempDir dir{"shard"};
    // Núcleo 1 de 3: posee las particiones p con p % 3 == 1 → de [0,5): {1, 4}.
    nexus::TopicManager manager{dir.path(), /*num_cores=*/3, /*owner_core=*/1};
    ASSERT_TRUE(manager.create_topic("t", 5).has_value());

    nexus::Topic* topic = manager.get("t");
    ASSERT_NE(topic, nullptr);
    // Metadatos completos (las 5 particiones), pero solo 2 instaladas localmente.
    EXPECT_EQ(topic->meta().partition_count, 5);
    EXPECT_EQ(topic->partition_count(), 2U);
    EXPECT_NE(topic->partition(1), nullptr);
    EXPECT_NE(topic->partition(4), nullptr);
    EXPECT_EQ(topic->partition(0), nullptr);  // dueño = núcleo 0
    EXPECT_EQ(topic->partition(2), nullptr);  // dueño = núcleo 2
    EXPECT_EQ(topic->partition(3), nullptr);  // dueño = núcleo 0
}

TEST(TopicManager, OwnsPartition_SigueLaReglaModulo) {
    TempDir dir{"owns"};
    nexus::TopicManager manager{dir.path(), /*num_cores=*/4, /*owner_core=*/2};
    EXPECT_EQ(manager.num_cores(), 4);
    EXPECT_EQ(manager.owner_core(), 2);
    EXPECT_TRUE(manager.owns_partition(2));
    EXPECT_TRUE(manager.owns_partition(6));
    EXPECT_FALSE(manager.owns_partition(0));
    EXPECT_FALSE(manager.owns_partition(3));
}

TEST(TopicManager, Sharding_DescribeListaTodasLasParticionesAunqueNoSeanLocales) {
    TempDir dir{"shard_desc"};
    nexus::TopicManager manager{dir.path(), /*num_cores=*/2, /*owner_core=*/0};
    ASSERT_TRUE(manager.create_topic("t", 4).has_value());

    // Metadata anuncia las 4 particiones al cliente aunque este núcleo solo sirva {0, 2}.
    const std::vector<nexus::TopicMeta> metas = manager.describe(/*leader_node_id=*/1);
    ASSERT_EQ(metas.size(), 1U);
    EXPECT_EQ(metas[0].partitions.size(), 4U);
}

TEST(TopicManager, ArgumentosDeShardingInvalidos_SeAcotan) {
    TempDir dir{"clamp"};
    nexus::TopicManager manager{dir.path(), /*num_cores=*/0, /*owner_core=*/9};
    EXPECT_EQ(manager.num_cores(), 1);   // < 1 → 1
    EXPECT_EQ(manager.owner_core(), 0);  // fuera de rango → 0
    ASSERT_TRUE(manager.create_topic("t", 3).has_value());
    EXPECT_EQ(manager.get("t")->partition_count(), 3U);  // num_cores=1 → abre todas
}

}  // namespace
