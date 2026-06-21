// Pruebas de TopicCatalog: managers por núcleo (sharding ADR-0026) y creación replicada.
#include "broker/topic_catalog.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "broker/partition.hpp"
#include "broker/topic.hpp"
#include "broker/topic_manager.hpp"
#include "common/error.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_catalog_" + std::string{tag} + "_" +
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

TEST(TopicCatalog, CoreCount_RespetaElNumeroDeNucleos) {
    TempDir dir{"cores"};
    const nexus::TopicCatalog catalog{dir.path(), 3};
    EXPECT_EQ(catalog.core_count(), 3);
    EXPECT_EQ(catalog.managers().size(), 3U);
}

TEST(TopicCatalog, NumCoresInvalido_SeAcotaAUno) {
    TempDir dir{"clamp"};
    const nexus::TopicCatalog catalog{dir.path(), 0};
    EXPECT_EQ(catalog.core_count(), 1);
}

TEST(TopicCatalog, CreateTopic_ReplicaATodos_CadaNucleoAbreSusParticiones) {
    TempDir dir{"replica"};
    nexus::TopicCatalog catalog{dir.path(), 2};

    const nexus::expected<nexus::TopicMetadata> meta = catalog.create_topic("t", 2);
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(meta->partition_count, 2);

    // Ambos núcleos registran el topic completo, pero cada uno abre solo su partición (p % N).
    nexus::Topic* t0 = catalog.manager(0).get("t");
    nexus::Topic* t1 = catalog.manager(1).get("t");
    ASSERT_NE(t0, nullptr);
    ASSERT_NE(t1, nullptr);
    EXPECT_NE(t0->partition(0), nullptr);  // p0 vive en el núcleo 0
    EXPECT_EQ(t0->partition(1), nullptr);
    EXPECT_NE(t1->partition(1), nullptr);  // p1 vive en el núcleo 1
    EXPECT_EQ(t1->partition(0), nullptr);
    EXPECT_EQ(catalog.manager(0).topic_count(), 1U);
    EXPECT_EQ(catalog.manager(1).topic_count(), 1U);
}

TEST(TopicCatalog, CreateTopic_Duplicado_Rechaza) {
    TempDir dir{"dup"};
    nexus::TopicCatalog catalog{dir.path(), 2};
    ASSERT_TRUE(catalog.create_topic("t", 1).has_value());

    const nexus::expected<nexus::TopicMetadata> again = catalog.create_topic("t", 1);
    ASSERT_FALSE(again.has_value());
    EXPECT_EQ(again.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(TopicCatalog, CreateTopic_ConteoInvalido_NoDejaEstado) {
    TempDir dir{"badcount"};
    nexus::TopicCatalog catalog{dir.path(), 2};

    const nexus::expected<nexus::TopicMetadata> bad = catalog.create_topic("t", 0);
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error().code(), nexus::ErrorCode::InvalidArgument);
    EXPECT_EQ(catalog.manager(0).topic_count(), 0U);
    EXPECT_EQ(catalog.manager(1).topic_count(), 0U);
}

}  // namespace
