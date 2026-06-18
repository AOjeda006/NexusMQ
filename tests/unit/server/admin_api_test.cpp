// AdminApi (ADR-0018): adaptador de AdminService sobre TopicManager + group lister inyectado.
// Verifica creación/borrado/describe/listado de topics y el listado de grupos por la función.
#include "server/admin_api.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

#include "broker/topic_manager.hpp"
#include "common/error.hpp"
#include "ingress/pagination.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_admin_" + std::string{tag} + "_" +
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

constexpr nexus::NodeId kNodeId = 7;

TEST(AdminApi, CreateTopic_DevuelveResumen) {
    TempDir dir{"create"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};

    const nexus::CreateTopicSpec spec{.name = "orders", .partition_count = 3};
    const auto summary = admin.create_topic(spec);
    ASSERT_TRUE(summary.has_value());
    EXPECT_EQ(summary->name, "orders");
    EXPECT_EQ(summary->partition_count, 3);
    EXPECT_EQ(topics.topic_count(), 1U);
}

TEST(AdminApi, CreateTopic_NombreVacio_Rechaza) {
    TempDir dir{"empty"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};

    const auto result =
        admin.create_topic(nexus::CreateTopicSpec{.name = "", .partition_count = 1});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(AdminApi, CreateTopic_Duplicado_Rechaza) {
    TempDir dir{"dup"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};

    ASSERT_TRUE(admin.create_topic(nexus::CreateTopicSpec{.name = "t", .partition_count = 1}));
    EXPECT_FALSE(admin.create_topic(nexus::CreateTopicSpec{.name = "t", .partition_count = 1}));
}

TEST(AdminApi, DeleteTopic_QuitaElTopic) {
    TempDir dir{"del"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};
    ASSERT_TRUE(admin.create_topic(nexus::CreateTopicSpec{.name = "t", .partition_count = 1}));

    EXPECT_TRUE(admin.delete_topic("t").has_value());
    EXPECT_EQ(topics.topic_count(), 0U);
    EXPECT_FALSE(admin.delete_topic("t").has_value());  // ya no existe.
}

TEST(AdminApi, DescribeTopic_DevuelveParticiones) {
    TempDir dir{"desc"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};
    ASSERT_TRUE(admin.create_topic(nexus::CreateTopicSpec{.name = "orders", .partition_count = 2}));

    const auto description = admin.describe_topic("orders");
    ASSERT_TRUE(description.has_value());
    EXPECT_EQ(description->summary.name, "orders");
    ASSERT_EQ(description->partitions.size(), 2U);
    EXPECT_EQ(description->partitions[0].id, 0);
    EXPECT_EQ(description->partitions[0].leader, kNodeId);
    EXPECT_EQ(description->partitions[1].id, 1);
    EXPECT_EQ(description->partitions[0].high_watermark, 0);  // log vacío.
}

TEST(AdminApi, DescribeTopic_Inexistente_Rechaza) {
    TempDir dir{"desc404"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};
    const auto result = admin.describe_topic("no-existe");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::NotFound);
}

TEST(AdminApi, ListTopics_OrdenadosYPaginados) {
    TempDir dir{"list"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};
    for (const char* name : {"charlie", "alpha", "bravo", "delta"}) {
        ASSERT_TRUE(admin.create_topic(nexus::CreateTopicSpec{.name = name, .partition_count = 1}));
    }

    const auto first = admin.list_topics(nexus::Page{.number = 1, .size = 2});
    ASSERT_EQ(first.size(), 2U);
    EXPECT_EQ(first[0].name, "alpha");  // orden por nombre.
    EXPECT_EQ(first[1].name, "bravo");

    const auto second = admin.list_topics(nexus::Page{.number = 2, .size = 2});
    ASSERT_EQ(second.size(), 2U);
    EXPECT_EQ(second[0].name, "charlie");
    EXPECT_EQ(second[1].name, "delta");

    EXPECT_TRUE(admin.list_topics(nexus::Page{.number = 3, .size = 2}).empty());  // fuera de rango.
}

TEST(AdminApi, ListGroups_UsaElListerInyectado) {
    TempDir dir{"groups"};
    nexus::TopicManager topics{dir.path()};
    auto lister = [](nexus::Page /*page*/) {
        return std::vector<nexus::GroupSummary>{nexus::GroupSummary{
            .group_id = "g1", .state = "Stable", .generation = 4, .member_count = 2}};
    };
    nexus::AdminApi admin{topics, kNodeId, lister};

    const auto groups = admin.list_groups(nexus::Page{});
    ASSERT_EQ(groups.size(), 1U);
    EXPECT_EQ(groups[0].group_id, "g1");
    EXPECT_EQ(groups[0].state, "Stable");
    EXPECT_EQ(groups[0].member_count, 2);
}

TEST(AdminApi, ListGroups_SinLister_DevuelveVacio) {
    TempDir dir{"nogroups"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};
    EXPECT_TRUE(admin.list_groups(nexus::Page{}).empty());
}

}  // namespace
