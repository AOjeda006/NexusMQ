// AdminApi (ADR-0018): adaptador de AdminService sobre TopicManager + group lister inyectado.
// Verifica creación/borrado/describe/listado de topics y el listado de grupos por la función.
#include "server/admin_api.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "broker/partition.hpp"
#include "broker/topic.hpp"
#include "broker/topic_manager.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/task.hpp"
#include "ingress/admin_service.hpp"
#include "ingress/pagination.hpp"
#include "reactor/partition_router.hpp"
#include "reactor/reactor.hpp"
#include "support/fake_proactor.hpp"

namespace {

// `create_topic`/`delete_topic` son ahora corrutinas; se conducen a término con `sync_wait` (sin
// E/S real en tests). Azúcar para no repetirlo en cada caso.
nexus::expected<nexus::TopicSummary> run_create(nexus::AdminApi& admin,
                                                const nexus::CreateTopicSpec& spec) {
    return nexus::sync_wait(admin.create_topic(spec));
}
nexus::expected<void> run_delete(nexus::AdminApi& admin, std::string_view name) {
    return nexus::sync_wait(admin.delete_topic(name));
}
// `describe_topic`/`list_groups` también son corrutinas (sin cablear no se suspenden): `sync_wait`.
nexus::expected<nexus::TopicDescription> run_describe(nexus::AdminApi& admin,
                                                      std::string_view name) {
    return nexus::sync_wait(admin.describe_topic(name));
}
std::vector<nexus::GroupSummary> run_list_groups(nexus::AdminApi& admin, nexus::Page page) {
    return nexus::sync_wait(admin.list_groups(page));
}

// Conduce `create_topic` cuando puede suspenderse (fan-out cross-core): dos reactores con
// `poll_once` (no hay E/S real, solo el buzón). Marca `done` y guarda el resultado al completar.
nexus::task<void> drive_create(nexus::AdminApi& admin, nexus::CreateTopicSpec spec,
                               nexus::expected<nexus::TopicSummary>& out, bool& done) {
    out = co_await admin.create_topic(spec);
    done = true;
}

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
    const auto summary = run_create(admin, spec);
    ASSERT_TRUE(summary.has_value());
    EXPECT_EQ(summary->name, "orders");
    EXPECT_EQ(summary->partition_count, 3);
    EXPECT_EQ(topics.topic_count(), 1U);
}

TEST(AdminApi, CreateTopic_NombreVacio_Rechaza) {
    TempDir dir{"empty"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};

    const auto result = run_create(admin, nexus::CreateTopicSpec{.name = "", .partition_count = 1});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(AdminApi, CreateTopic_NombreInvalido_Rechaza) {
    TempDir dir{"badname"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};

    // Antes solo se rechazaba el nombre vacío; ahora REST reutiliza las reglas centralizadas de
    // TopicManager (P4): separadores de ruta, espacios, etc. también se rechazan sin crear
    // ficheros.
    const auto result =
        run_create(admin, nexus::CreateTopicSpec{.name = "a/b", .partition_count = 1});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::InvalidArgument);
    EXPECT_EQ(topics.topic_count(), 0U);
}

TEST(AdminApi, CreateTopic_Duplicado_Rechaza) {
    TempDir dir{"dup"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};

    ASSERT_TRUE(run_create(admin, nexus::CreateTopicSpec{.name = "t", .partition_count = 1}));
    EXPECT_FALSE(run_create(admin, nexus::CreateTopicSpec{.name = "t", .partition_count = 1}));
}

TEST(AdminApi, DeleteTopic_QuitaElTopic) {
    TempDir dir{"del"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};
    ASSERT_TRUE(run_create(admin, nexus::CreateTopicSpec{.name = "t", .partition_count = 1}));

    EXPECT_TRUE(run_delete(admin, "t").has_value());
    EXPECT_EQ(topics.topic_count(), 0U);
    EXPECT_FALSE(run_delete(admin, "t").has_value());  // ya no existe.
}

TEST(AdminApi, DescribeTopic_DevuelveParticiones) {
    TempDir dir{"desc"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};
    ASSERT_TRUE(run_create(admin, nexus::CreateTopicSpec{.name = "orders", .partition_count = 2}));

    const auto description = run_describe(admin, "orders");
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
    const auto result = run_describe(admin, "no-existe");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::NotFound);
}

TEST(AdminApi, ListTopics_OrdenadosYPaginados) {
    TempDir dir{"list"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};
    for (const char* name : {"charlie", "alpha", "bravo", "delta"}) {
        ASSERT_TRUE(run_create(admin, nexus::CreateTopicSpec{.name = name, .partition_count = 1}));
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
    auto lister = [](nexus::Page /*page*/) -> nexus::task<std::vector<nexus::GroupSummary>> {
        co_return std::vector<nexus::GroupSummary>{nexus::GroupSummary{
            .group_id = "g1", .state = "Stable", .generation = 4, .member_count = 2}};
    };
    nexus::AdminApi admin{topics, kNodeId, lister};

    const auto groups = run_list_groups(admin, nexus::Page{});
    ASSERT_EQ(groups.size(), 1U);
    EXPECT_EQ(groups[0].group_id, "g1");
    EXPECT_EQ(groups[0].state, "Stable");
    EXPECT_EQ(groups[0].member_count, 2);
}

TEST(AdminApi, ListGroups_SinLister_DevuelveVacio) {
    TempDir dir{"nogroups"};
    nexus::TopicManager topics{dir.path()};
    nexus::AdminApi admin{topics, kNodeId};
    EXPECT_TRUE(run_list_groups(admin, nexus::Page{}).empty());
}

TEST(AdminApi, CreateTopic_Cableado_FanOutATodosLosNucleos) {
    TempDir dir{"fanout"};
    // Dos reactores cableados; dos TopicManager sharded (núcleo 0 dueño de p0; núcleo 1 de p1).
    nexus::Reactor r0{0, 2, std::make_unique<nexus::FakeProactor>()};
    nexus::Reactor r1{1, 2, std::make_unique<nexus::FakeProactor>()};
    r0.connect_peers({&r0, &r1});
    r1.connect_peers({&r0, &r1});
    nexus::TopicManager t0{dir.path(), /*num_cores=*/2, /*owner_core=*/0};
    nexus::TopicManager t1{dir.path(), /*num_cores=*/2, /*owner_core=*/1};
    nexus::PartitionRouter partitions{{&r0, &r1}};
    nexus::AdminApi admin{t0, kNodeId};  // el manager del núcleo 0 es el "local" del adaptador.
    admin.bind_cluster(r0, partitions, {&t0, &t1});

    // El admin se sirve en el núcleo 0; el fan-out alcanza al núcleo 1 por el buzón.
    nexus::expected<nexus::TopicSummary> result;
    bool done = false;
    r0.spawn(drive_create(admin, nexus::CreateTopicSpec{.name = "t", .partition_count = 2}, result,
                          done));
    for (int i = 0; i < 64 && !done; ++i) {
        r0.poll_once();
        r1.poll_once();
    }
    ASSERT_TRUE(done);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->name, "t");
    // Cada núcleo registró el topic y abrió solo la partición que le toca.
    ASSERT_NE(t0.get("t"), nullptr);
    ASSERT_NE(t1.get("t"), nullptr);
    EXPECT_NE(t0.get("t")->partition(0), nullptr);
    EXPECT_EQ(t0.get("t")->partition(1), nullptr);
    EXPECT_NE(t1.get("t")->partition(1), nullptr);
    EXPECT_EQ(t1.get("t")->partition(0), nullptr);
}

TEST(AdminApi, DescribeTopic_Cableado_AgregaWatermarksDeCadaNucleo) {
    TempDir dir{"describe_xcore"};
    nexus::Reactor r0{0, 2, std::make_unique<nexus::FakeProactor>()};
    nexus::Reactor r1{1, 2, std::make_unique<nexus::FakeProactor>()};
    r0.connect_peers({&r0, &r1});
    r1.connect_peers({&r0, &r1});
    nexus::TopicManager t0{dir.path(), /*num_cores=*/2, /*owner_core=*/0};
    nexus::TopicManager t1{dir.path(), /*num_cores=*/2, /*owner_core=*/1};
    ASSERT_TRUE(t0.create_topic("t", 2).has_value());  // metadatos completos en cada núcleo;
    ASSERT_TRUE(t1.create_topic("t", 2).has_value());  // cada uno abre solo su partición.

    // Bumpea el high-watermark de la partición 1 (dueña: núcleo 1) escribiendo 3 records.
    nexus::RecordBatchHeader header;
    header.record_count = 3;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(3, std::byte{0x7})};
    ASSERT_TRUE(t1.get("t")->partition(1)->produce(batch).has_value());

    nexus::PartitionRouter partitions{{&r0, &r1}};
    nexus::AdminApi admin{t0, kNodeId};
    admin.bind_cluster(r0, partitions, {&t0, &t1});

    // describe se sirve en el núcleo 0; el watermark de p1 lo lee del núcleo 1 por el buzón.
    nexus::expected<nexus::TopicDescription> out;
    bool done = false;
    r0.spawn([](nexus::AdminApi& api, nexus::expected<nexus::TopicDescription>& result,
                bool& finished) -> nexus::task<void> {
        result = co_await api.describe_topic("t");
        finished = true;
    }(admin, out, done));
    for (int i = 0; i < 64 && !done; ++i) {
        r0.poll_once();
        r1.poll_once();
    }
    ASSERT_TRUE(done);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->partitions.size(), 2U);
    EXPECT_EQ(out->partitions[0].high_watermark, 0);  // p0 (núcleo 0): log vacío.
    EXPECT_EQ(out->partitions[1].high_watermark, 3);  // p1 (núcleo 1): leído cross-core.
}

}  // namespace
