// Pruebas de RequestRouter: puente protocolo↔dominio sin red (codifica/decodifica en memoria).
#include "broker/request_router.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "broker/group_coordinator.hpp"
#include "broker/offset_manager.hpp"
#include "broker/topic_manager.hpp"
#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/fnv1a.hpp"
#include "common/record.hpp"
#include "common/task.hpp"
#include "common/types.hpp"
#include "protocol/codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/frame.hpp"
#include "protocol/messages.hpp"
#include "reactor/partition_router.hpp"
#include "reactor/reactor.hpp"
#include "support/fake_proactor.hpp"
#include "telemetry/metrics.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_router_" + std::string{tag} + "_" +
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

// Codifica una petición en bytes (cuerpo de trama) y devuelve un Decoder sobre ellos.
template <class Request>
nexus::Buffer encode_request(const Request& req) {
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    req.encode(enc);
    return buf;
}

// Bytes de un RecordBatch con `count` records (para ProduceRequest.batch).
std::vector<std::byte> encode_batch(std::int32_t count) {
    nexus::RecordBatchHeader header;
    header.record_count = count;
    const nexus::RecordBatch batch{
        header, std::vector<std::byte>(static_cast<std::size_t>(count), std::byte{0x7})};
    nexus::Buffer buf;
    batch.encode(buf);
    const nexus::ByteSpan span = buf.as_span();
    return {span.begin(), span.end()};
}

// `dispatch` es una corrutina (`task<expected<void>>`); en los tests se conduce a término con
// `sync_wait` (no se suspende en E/S). Helper para no repetirlo en cada caso.
nexus::expected<void> run_dispatch(nexus::RequestRouter& router, nexus::ApiKey key,
                                   std::uint16_t version, nexus::Decoder& body,
                                   nexus::Buffer& out) {
    return nexus::sync_wait(router.dispatch(key, version, body, out));
}

// Conduce `dispatch` cuando puede suspenderse (enrutado cross-core): marca `done` al completar.
// Para usar con dos reactores movidos con `poll_once` (no hay E/S real, solo el buzón).
nexus::task<void> drive_dispatch(nexus::RequestRouter& router, nexus::ApiKey key,
                                 nexus::Decoder& body, nexus::Buffer& out, bool& done) {
    (void)co_await router.dispatch(key, /*version=*/0, body, out);
    done = true;
}

TEST(RequestRouter, ApiVersions_DevuelveRangosSoportados) {
    TempDir dir{"apiver"};
    nexus::TopicManager topics{dir.path()};
    nexus::RequestRouter router{topics, /*node_id=*/0, "127.0.0.1", 9092};

    const nexus::Buffer body = encode_request(nexus::ApiVersionsRequest{.client_version = 0});
    nexus::Decoder dec{body.as_span()};
    nexus::Buffer out;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::ApiVersions, 0, dec, out).has_value());

    nexus::Decoder resp_dec{out.as_span()};
    const nexus::expected<nexus::ApiVersionsResponse> resp =
        nexus::ApiVersionsResponse::decode(resp_dec);
    ASSERT_TRUE(resp.has_value());
    EXPECT_FALSE(resp->ranges.empty());
}

TEST(RequestRouter, Produce_AnexaYDevuelveBaseOffset) {
    TempDir dir{"produce"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("t", 1).has_value());
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};

    const std::vector<std::byte> batch = encode_batch(3);
    nexus::ProduceRequest req;
    req.topic = "t";
    req.partition = 0;
    req.batch = nexus::ByteSpan{batch.data(), batch.size()};
    const nexus::Buffer body = encode_request(req);

    nexus::Decoder dec{body.as_span()};
    nexus::Buffer out;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Produce, 0, dec, out).has_value());

    nexus::Decoder resp_dec{out.as_span()};
    const nexus::expected<nexus::ProduceResponse> resp = nexus::ProduceResponse::decode(resp_dec);
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->error_code, nexus::WireError::None);
    EXPECT_EQ(resp->base_offset, 0);
}

TEST(RequestRouter, Produce_TopicInexistente_DevuelveUnknownTopic) {
    TempDir dir{"notopic"};
    nexus::TopicManager topics{dir.path()};
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};

    const std::vector<std::byte> batch = encode_batch(1);
    nexus::ProduceRequest req;
    req.topic = "nope";
    req.batch = nexus::ByteSpan{batch.data(), batch.size()};
    const nexus::Buffer body = encode_request(req);

    nexus::Decoder dec{body.as_span()};
    nexus::Buffer out;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Produce, 0, dec, out).has_value());

    nexus::Decoder resp_dec{out.as_span()};
    const nexus::expected<nexus::ProduceResponse> resp = nexus::ProduceResponse::decode(resp_dec);
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->error_code, nexus::WireError::UnknownTopicOrPartition);
}

TEST(RequestRouter, ProduceLuegoFetch_DevuelveLosBytes) {
    TempDir dir{"pf"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("t", 1).has_value());
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};

    const std::vector<std::byte> batch = encode_batch(4);
    nexus::ProduceRequest preq;
    preq.topic = "t";
    preq.batch = nexus::ByteSpan{batch.data(), batch.size()};
    nexus::Buffer pbody = encode_request(preq);
    nexus::Decoder pdec{pbody.as_span()};
    nexus::Buffer pout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Produce, 0, pdec, pout).has_value());

    nexus::FetchRequest freq;
    freq.topic = "t";
    freq.partition = 0;
    freq.fetch_offset = 0;
    freq.max_bytes = 64 * 1024;
    nexus::Buffer fbody = encode_request(freq);
    nexus::Decoder fdec{fbody.as_span()};
    nexus::Buffer fout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Fetch, 0, fdec, fout).has_value());

    nexus::Decoder resp_dec{fout.as_span()};
    const nexus::expected<nexus::FetchResponse> resp = nexus::FetchResponse::decode(resp_dec);
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->error_code, nexus::WireError::None);
    EXPECT_EQ(resp->high_watermark, 4);
    EXPECT_FALSE(resp->batches.empty());
}

TEST(RequestRouter, CreateTopic_CreaYLuegoMetadataLoLista) {
    TempDir dir{"create"};
    nexus::TopicManager topics{dir.path()};
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};

    nexus::Buffer cbody = encode_request(
        nexus::CreateTopicRequest{.name = "nuevo", .partition_count = 2, .replication_factor = 1});
    nexus::Decoder cdec{cbody.as_span()};
    nexus::Buffer cout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::CreateTopic, 0, cdec, cout).has_value());
    nexus::Decoder cresp_dec{cout.as_span()};
    const nexus::expected<nexus::CreateTopicResponse> cresp =
        nexus::CreateTopicResponse::decode(cresp_dec);
    ASSERT_TRUE(cresp.has_value());
    EXPECT_EQ(cresp->error_code, nexus::WireError::None);
    EXPECT_EQ(topics.topic_count(), 1U);

    nexus::Buffer mbody = encode_request(nexus::MetadataRequest{});
    nexus::Decoder mdec{mbody.as_span()};
    nexus::Buffer mout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Metadata, 0, mdec, mout).has_value());
    nexus::Decoder mresp_dec{mout.as_span()};
    const nexus::expected<nexus::MetadataResponse> mresp =
        nexus::MetadataResponse::decode(mresp_dec);
    ASSERT_TRUE(mresp.has_value());
    ASSERT_EQ(mresp->brokers.size(), 1U);
    EXPECT_EQ(mresp->brokers[0].port, 9092);
    ASSERT_EQ(mresp->topics.size(), 1U);
    EXPECT_EQ(mresp->topics[0].partitions.size(), 2U);
}

TEST(RequestRouter, OffsetCommitLuegoFetch_DevuelveElOffset) {
    TempDir dir{"offset"};
    nexus::TopicManager topics{dir.path()};
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};

    nexus::Buffer cbody = encode_request(nexus::OffsetCommitRequest{
        .group = "g", .topic = "t", .partition = 0, .offset = 7, .metadata = ""});
    nexus::Decoder cdec{cbody.as_span()};
    nexus::Buffer cout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::OffsetCommit, 0, cdec, cout).has_value());
    nexus::Decoder cresp_dec{cout.as_span()};
    const nexus::expected<nexus::OffsetCommitResponse> cresp =
        nexus::OffsetCommitResponse::decode(cresp_dec);
    ASSERT_TRUE(cresp.has_value());
    EXPECT_EQ(cresp->error_code, nexus::WireError::None);

    nexus::Buffer fbody =
        encode_request(nexus::OffsetFetchRequest{.group = "g", .topic = "t", .partition = 0});
    nexus::Decoder fdec{fbody.as_span()};
    nexus::Buffer fout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::OffsetFetch, 0, fdec, fout).has_value());
    nexus::Decoder fresp_dec{fout.as_span()};
    const nexus::expected<nexus::OffsetFetchResponse> fresp =
        nexus::OffsetFetchResponse::decode(fresp_dec);
    ASSERT_TRUE(fresp.has_value());
    EXPECT_EQ(fresp->error_code, nexus::WireError::None);
    EXPECT_EQ(fresp->offset, 7);
}

TEST(RequestRouter, OffsetFetch_SinCommit_DevuelveMenosUno) {
    TempDir dir{"nooffset"};
    nexus::TopicManager topics{dir.path()};
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};

    nexus::Buffer fbody =
        encode_request(nexus::OffsetFetchRequest{.group = "nuevo", .topic = "t", .partition = 0});
    nexus::Decoder fdec{fbody.as_span()};
    nexus::Buffer fout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::OffsetFetch, 0, fdec, fout).has_value());
    nexus::Decoder fresp_dec{fout.as_span()};
    const nexus::expected<nexus::OffsetFetchResponse> fresp =
        nexus::OffsetFetchResponse::decode(fresp_dec);
    ASSERT_TRUE(fresp.has_value());
    EXPECT_EQ(fresp->error_code, nexus::WireError::None);
    EXPECT_EQ(fresp->offset, -1);
}

TEST(RequestRouter, JoinSyncHeartbeatLeave_FlujoDeGrupo) {
    TempDir dir{"group"};
    nexus::TopicManager topics{dir.path()};
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};

    // Join: alta de un consumidor nuevo (id vacío) → líder de la generación 1.
    nexus::Buffer jbody = encode_request(nexus::JoinGroupRequest{
        .group = "g", .member_id = "", .session_timeout_ms = 30000, .subscription = {}});
    nexus::Decoder jdec{jbody.as_span()};
    nexus::Buffer jout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::JoinGroup, 0, jdec, jout).has_value());
    nexus::Decoder jresp_dec{jout.as_span()};
    const nexus::expected<nexus::JoinGroupResponse> jresp =
        nexus::JoinGroupResponse::decode(jresp_dec);
    ASSERT_TRUE(jresp.has_value());
    EXPECT_EQ(jresp->error_code, nexus::WireError::None);
    EXPECT_EQ(jresp->generation, 1);
    EXPECT_TRUE(jresp->is_leader);
    EXPECT_EQ(jresp->member_id, "g-1");
    ASSERT_EQ(jresp->members.size(), 1U);

    // Sync: el líder se reparte una asignación a sí mismo y la recibe de vuelta.
    nexus::SyncGroupRequest sreq;
    sreq.group = "g";
    sreq.member_id = jresp->member_id;
    sreq.generation = jresp->generation;
    sreq.assignments.push_back({.member_id = jresp->member_id, .assignment = {std::byte{0x1}}});
    nexus::Buffer sbody = encode_request(sreq);
    nexus::Decoder sdec{sbody.as_span()};
    nexus::Buffer sout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::SyncGroup, 0, sdec, sout).has_value());
    nexus::Decoder sresp_dec{sout.as_span()};
    const nexus::expected<nexus::SyncGroupResponse> sresp =
        nexus::SyncGroupResponse::decode(sresp_dec);
    ASSERT_TRUE(sresp.has_value());
    EXPECT_EQ(sresp->error_code, nexus::WireError::None);
    EXPECT_EQ(sresp->assignment, (std::vector<std::byte>{std::byte{0x1}}));

    // Heartbeat: grupo estable → None.
    nexus::Buffer hbody = encode_request(nexus::HeartbeatRequest{
        .group = "g", .member_id = jresp->member_id, .generation = jresp->generation});
    nexus::Decoder hdec{hbody.as_span()};
    nexus::Buffer hout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Heartbeat, 0, hdec, hout).has_value());
    nexus::Decoder hresp_dec{hout.as_span()};
    const nexus::expected<nexus::HeartbeatResponse> hresp =
        nexus::HeartbeatResponse::decode(hresp_dec);
    ASSERT_TRUE(hresp.has_value());
    EXPECT_EQ(hresp->error_code, nexus::WireError::None);

    // Leave: baja del único miembro → None.
    nexus::Buffer lbody =
        encode_request(nexus::LeaveGroupRequest{.group = "g", .member_id = jresp->member_id});
    nexus::Decoder ldec{lbody.as_span()};
    nexus::Buffer lout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::LeaveGroup, 0, ldec, lout).has_value());
    nexus::Decoder lresp_dec{lout.as_span()};
    const nexus::expected<nexus::LeaveGroupResponse> lresp =
        nexus::LeaveGroupResponse::decode(lresp_dec);
    ASSERT_TRUE(lresp.has_value());
    EXPECT_EQ(lresp->error_code, nexus::WireError::None);
}

TEST(RequestRouter, Heartbeat_GrupoDesconocido_DevuelveError) {
    TempDir dir{"ghost"};
    nexus::TopicManager topics{dir.path()};
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};

    nexus::Buffer body = encode_request(
        nexus::HeartbeatRequest{.group = "fantasma", .member_id = "m", .generation = 1});
    nexus::Decoder dec{body.as_span()};
    nexus::Buffer out;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Heartbeat, 0, dec, out).has_value());
    nexus::Decoder resp_dec{out.as_span()};
    const nexus::expected<nexus::HeartbeatResponse> resp =
        nexus::HeartbeatResponse::decode(resp_dec);
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->error_code, nexus::WireError::UnknownTopicOrPartition);
}

TEST(RequestRouter, ApiKeyDesconocida_DevuelveError) {
    TempDir dir{"unsup"};
    nexus::TopicManager topics{dir.path()};
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};
    nexus::Buffer body;
    nexus::Decoder dec{body.as_span()};
    nexus::Buffer out;
    const auto unknown = static_cast<nexus::ApiKey>(0xFFFF);
    EXPECT_FALSE(run_dispatch(router, unknown, 0, dec, out).has_value());
}

// --- Enrutado cross-core (ADR-0026): router en el núcleo 0, partición dueña en el núcleo 1 ---

TEST(RequestRouter, ProduceLuegoFetch_EnrutadoAlNucleoDueno) {
    TempDir dir{"crosscore"};
    // Dos reactores cableados; dos TopicManager sharded (núcleo 0 dueño de p0; núcleo 1 de p1).
    nexus::Reactor r0{0, 2, std::make_unique<nexus::FakeProactor>()};
    nexus::Reactor r1{1, 2, std::make_unique<nexus::FakeProactor>()};
    r0.connect_peers({&r0, &r1});
    r1.connect_peers({&r0, &r1});
    nexus::TopicManager t0{dir.path(), /*num_cores=*/2, /*owner_core=*/0};
    nexus::TopicManager t1{dir.path(), /*num_cores=*/2, /*owner_core=*/1};
    ASSERT_TRUE(t0.create_topic("t", 2).has_value());
    ASSERT_TRUE(t1.create_topic("t", 2).has_value());
    ASSERT_EQ(t0.get("t")->partition(1), nullptr);  // p1 no vive en el núcleo 0
    ASSERT_NE(t1.get("t")->partition(1), nullptr);  // p1 vive en el núcleo 1

    // El router atiende en el núcleo 0; opera p1 en el núcleo 1 vía PartitionRouter.
    nexus::PartitionRouter partitions{{&r0, &r1}};
    nexus::GroupCoordinator g0;
    nexus::GroupCoordinator g1;
    nexus::OffsetManager o0;
    nexus::OffsetManager o1;
    nexus::RequestRouter router{t0, 0, "127.0.0.1", 9092};
    router.bind_cluster(r0, partitions, {&t0, &t1}, {&g0, &g1}, {&o0, &o1});

    auto pump = [&](bool& done) {
        for (int i = 0; i < 64 && !done; ++i) {
            r0.poll_once();
            r1.poll_once();
        }
    };

    // Produce 3 records a la partición 1 (dueña: núcleo 1).
    const std::vector<std::byte> batch = encode_batch(3);
    nexus::ProduceRequest preq;
    preq.topic = "t";
    preq.partition = 1;
    preq.batch = nexus::ByteSpan{batch.data(), batch.size()};
    nexus::Buffer pbody = encode_request(preq);
    nexus::Decoder pdec{pbody.as_span()};
    nexus::Buffer pout;
    bool pdone = false;
    r0.spawn(drive_dispatch(router, nexus::ApiKey::Produce, pdec, pout, pdone));
    pump(pdone);
    ASSERT_TRUE(pdone);

    nexus::Decoder presp_dec{pout.as_span()};
    const nexus::expected<nexus::ProduceResponse> presp = nexus::ProduceResponse::decode(presp_dec);
    ASSERT_TRUE(presp.has_value());
    EXPECT_EQ(presp->error_code, nexus::WireError::None);
    EXPECT_EQ(presp->base_offset, 0);
    // Aterrizó en la partición del núcleo 1, no en el 0.
    EXPECT_EQ(t1.get("t")->partition(1)->high_watermark(), 3);

    // Fetch desde la misma partición: vuelve por el cruce con los bytes y el high_watermark.
    nexus::FetchRequest freq;
    freq.topic = "t";
    freq.partition = 1;
    freq.fetch_offset = 0;
    freq.max_bytes = 64 * 1024;
    nexus::Buffer fbody = encode_request(freq);
    nexus::Decoder fdec{fbody.as_span()};
    nexus::Buffer fout;
    bool fdone = false;
    r0.spawn(drive_dispatch(router, nexus::ApiKey::Fetch, fdec, fout, fdone));
    pump(fdone);
    ASSERT_TRUE(fdone);

    nexus::Decoder fresp_dec{fout.as_span()};
    const nexus::expected<nexus::FetchResponse> fresp = nexus::FetchResponse::decode(fresp_dec);
    ASSERT_TRUE(fresp.has_value());
    EXPECT_EQ(fresp->error_code, nexus::WireError::None);
    EXPECT_EQ(fresp->high_watermark, 3);
    EXPECT_FALSE(fresp->batches.empty());
}

TEST(RequestRouter, CreateLuegoDeleteTopic_FanOutATodosLosNucleos) {
    TempDir dir{"fanout"};
    // Dos reactores cableados; dos TopicManager sharded (núcleo 0 dueño de p0; núcleo 1 de p1).
    nexus::Reactor r0{0, 2, std::make_unique<nexus::FakeProactor>()};
    nexus::Reactor r1{1, 2, std::make_unique<nexus::FakeProactor>()};
    r0.connect_peers({&r0, &r1});
    r1.connect_peers({&r0, &r1});
    nexus::TopicManager t0{dir.path(), /*num_cores=*/2, /*owner_core=*/0};
    nexus::TopicManager t1{dir.path(), /*num_cores=*/2, /*owner_core=*/1};
    nexus::PartitionRouter partitions{{&r0, &r1}};
    nexus::GroupCoordinator g0;
    nexus::GroupCoordinator g1;
    nexus::OffsetManager o0;
    nexus::OffsetManager o1;
    nexus::RequestRouter router{t0, 0, "127.0.0.1", 9092};
    router.bind_cluster(r0, partitions, {&t0, &t1}, {&g0, &g1}, {&o0, &o1});

    auto pump = [&](bool& done) {
        for (int i = 0; i < 64 && !done; ++i) {
            r0.poll_once();
            r1.poll_once();
        }
    };

    // CreateTopic atendido en el núcleo 0 → fan-out cross-core: ambos managers registran el topic,
    // cada uno abriendo solo la partición que le toca (ADR-0026).
    nexus::Buffer cbody = encode_request(
        nexus::CreateTopicRequest{.name = "t", .partition_count = 2, .replication_factor = 1});
    nexus::Decoder cdec{cbody.as_span()};
    nexus::Buffer cout;
    bool cdone = false;
    r0.spawn(drive_dispatch(router, nexus::ApiKey::CreateTopic, cdec, cout, cdone));
    pump(cdone);
    ASSERT_TRUE(cdone);
    nexus::Decoder cresp_dec{cout.as_span()};
    const nexus::expected<nexus::CreateTopicResponse> cresp =
        nexus::CreateTopicResponse::decode(cresp_dec);
    ASSERT_TRUE(cresp.has_value());
    EXPECT_EQ(cresp->error_code, nexus::WireError::None);
    EXPECT_EQ(t0.topic_count(), 1U);
    EXPECT_EQ(t1.topic_count(), 1U);
    ASSERT_NE(t0.get("t"), nullptr);
    ASSERT_NE(t1.get("t"), nullptr);
    EXPECT_NE(t0.get("t")->partition(0), nullptr);  // p0 vive en el núcleo 0
    EXPECT_EQ(t0.get("t")->partition(1), nullptr);
    EXPECT_NE(t1.get("t")->partition(1), nullptr);  // p1 vive en el núcleo 1
    EXPECT_EQ(t1.get("t")->partition(0), nullptr);

    // Produce a la partición 1 (núcleo 1): funciona porque el fan-out la creó allí.
    const std::vector<std::byte> batch = encode_batch(2);
    nexus::ProduceRequest preq;
    preq.topic = "t";
    preq.partition = 1;
    preq.batch = nexus::ByteSpan{batch.data(), batch.size()};
    nexus::Buffer pbody = encode_request(preq);
    nexus::Decoder pdec{pbody.as_span()};
    nexus::Buffer pout;
    bool pdone = false;
    r0.spawn(drive_dispatch(router, nexus::ApiKey::Produce, pdec, pout, pdone));
    pump(pdone);
    ASSERT_TRUE(pdone);
    nexus::Decoder presp_dec{pout.as_span()};
    const nexus::expected<nexus::ProduceResponse> presp = nexus::ProduceResponse::decode(presp_dec);
    ASSERT_TRUE(presp.has_value());
    EXPECT_EQ(presp->error_code, nexus::WireError::None);

    // DeleteTopic atendido en el núcleo 0 → fan-out: se borra de ambos managers.
    nexus::Buffer dbody = encode_request(nexus::DeleteTopicRequest{.name = "t"});
    nexus::Decoder ddec{dbody.as_span()};
    nexus::Buffer dout;
    bool ddone = false;
    r0.spawn(drive_dispatch(router, nexus::ApiKey::DeleteTopic, ddec, dout, ddone));
    pump(ddone);
    ASSERT_TRUE(ddone);
    nexus::Decoder dresp_dec{dout.as_span()};
    const nexus::expected<nexus::DeleteTopicResponse> dresp =
        nexus::DeleteTopicResponse::decode(dresp_dec);
    ASSERT_TRUE(dresp.has_value());
    EXPECT_EQ(dresp->error_code, nexus::WireError::None);
    EXPECT_EQ(t0.topic_count(), 0U);
    EXPECT_EQ(t1.topic_count(), 0U);
}

TEST(RequestRouter, GrupoYOffset_EnrutadosAlNucleoCoordinador) {
    TempDir dir{"groupshard"};
    // Dos reactores; coordinadores y offsets por núcleo (los grupos/offsets no tocan particiones).
    nexus::Reactor r0{0, 2, std::make_unique<nexus::FakeProactor>()};
    nexus::Reactor r1{1, 2, std::make_unique<nexus::FakeProactor>()};
    r0.connect_peers({&r0, &r1});
    r1.connect_peers({&r0, &r1});
    nexus::TopicManager t0{dir.path(), /*num_cores=*/2, /*owner_core=*/0};
    nexus::TopicManager t1{dir.path(), /*num_cores=*/2, /*owner_core=*/1};
    nexus::PartitionRouter partitions{{&r0, &r1}};
    nexus::GroupCoordinator g0;
    nexus::GroupCoordinator g1;
    nexus::OffsetManager o0;
    nexus::OffsetManager o1;
    nexus::RequestRouter router{t0, 0, "127.0.0.1", 9092};
    router.bind_cluster(r0, partitions, {&t0, &t1}, {&g0, &g1}, {&o0, &o1});

    auto pump = [&](bool& done) {
        for (int i = 0; i < 64 && !done; ++i) {
            r0.poll_once();
            r1.poll_once();
        }
    };

    // Elige un grupo cuyo núcleo coordinador es el 1 (`hash % 2 == 1`): así el OffsetCommit/Join
    // atendidos en el núcleo 0 cruzan de verdad al núcleo 1 (no es el fast-path local, ADR-0026).
    std::string group;
    for (int i = 0; group.empty(); ++i) {
        std::string candidate = "grp-" + std::to_string(i);
        if (nexus::fnv1a_64(candidate) % 2 == 1) {
            group = std::move(candidate);
        }
    }

    // OffsetCommit atendido en el núcleo 0 → aterriza en el OffsetManager del núcleo 1.
    nexus::Buffer cbody = encode_request(nexus::OffsetCommitRequest{
        .group = group, .topic = "t", .partition = 0, .offset = 99, .metadata = ""});
    nexus::Decoder cdec{cbody.as_span()};
    nexus::Buffer cout;
    bool cdone = false;
    r0.spawn(drive_dispatch(router, nexus::ApiKey::OffsetCommit, cdec, cout, cdone));
    pump(cdone);
    ASSERT_TRUE(cdone);
    const nexus::expected<nexus::Offset> on1 = o1.fetch(group, "t", 0);
    ASSERT_TRUE(on1.has_value());
    EXPECT_EQ(*on1, 99);
    EXPECT_FALSE(o0.fetch(group, "t", 0).has_value());  // no aterrizó en el núcleo 0.

    // JoinGroup atendido en el núcleo 0 → la membresía aterriza en el coordinador del núcleo 1.
    nexus::Buffer jbody = encode_request(nexus::JoinGroupRequest{
        .group = group, .member_id = "", .session_timeout_ms = 30000, .subscription = {}});
    nexus::Decoder jdec{jbody.as_span()};
    nexus::Buffer jout;
    bool jdone = false;
    r0.spawn(drive_dispatch(router, nexus::ApiKey::JoinGroup, jdec, jout, jdone));
    pump(jdone);
    ASSERT_TRUE(jdone);
    EXPECT_EQ(g1.group_count(), 1U);
    EXPECT_EQ(g0.group_count(), 0U);
}

TEST(RequestRouter, Metrics_ProduceConExito_RegistraTraficoBytesYLatencia) {
    TempDir dir{"m_produce"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("t", 1).has_value());
    nexus::MetricsRegistry metrics;
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};
    router.set_metrics(metrics);

    const std::vector<std::byte> batch = encode_batch(3);
    nexus::ProduceRequest req;
    req.topic = "t";
    req.partition = 0;
    req.batch = nexus::ByteSpan{batch.data(), batch.size()};
    const nexus::Buffer body = encode_request(req);
    nexus::Decoder dec{body.as_span()};
    nexus::Buffer out;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Produce, 0, dec, out).has_value());

    const nexus::Labels api{{"api", "produce"}, {"protocol", "native"}};
    EXPECT_EQ(metrics.counter("nexus_broker_requests_total", api).value(), 1U);
    EXPECT_EQ(metrics.counter("nexus_broker_request_errors_total", api).value(), 0U);
    EXPECT_EQ(metrics.counter("nexus_broker_request_bytes_total", api).value(), batch.size());
    // Un request agrupa N records: `messages` sube por el nº de records del batch, no por 1.
    EXPECT_EQ(metrics.counter("nexus_broker_messages_total", api).value(), 3U);
    EXPECT_EQ(metrics.histogram("nexus_broker_request_duration_seconds", api).count(), 1U);
}

TEST(RequestRouter, Metrics_ProduceTopicInexistente_RegistraError) {
    TempDir dir{"m_produce_err"};
    nexus::TopicManager topics{dir.path()};
    nexus::MetricsRegistry metrics;
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};
    router.set_metrics(metrics);

    const std::vector<std::byte> batch = encode_batch(1);
    nexus::ProduceRequest req;
    req.topic = "nope";
    req.batch = nexus::ByteSpan{batch.data(), batch.size()};
    const nexus::Buffer body = encode_request(req);
    nexus::Decoder dec{body.as_span()};
    nexus::Buffer out;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Produce, 0, dec, out).has_value());

    const nexus::Labels api{{"api", "produce"}, {"protocol", "native"}};
    EXPECT_EQ(metrics.counter("nexus_broker_requests_total", api).value(), 1U);
    EXPECT_EQ(metrics.counter("nexus_broker_request_errors_total", api).value(), 1U);
}

TEST(RequestRouter, Metrics_Fetch_RegistraTraficoYBytesServidos) {
    TempDir dir{"m_fetch"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("t", 1).has_value());
    nexus::MetricsRegistry metrics;
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};
    router.set_metrics(metrics);

    const std::vector<std::byte> batch = encode_batch(2);
    nexus::ProduceRequest preq;
    preq.topic = "t";
    preq.partition = 0;
    preq.batch = nexus::ByteSpan{batch.data(), batch.size()};
    const nexus::Buffer pbody = encode_request(preq);
    nexus::Decoder pdec{pbody.as_span()};
    nexus::Buffer pout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Produce, 0, pdec, pout).has_value());

    nexus::FetchRequest freq;
    freq.topic = "t";
    freq.partition = 0;
    freq.fetch_offset = 0;
    freq.max_bytes = 64 * 1024;
    const nexus::Buffer fbody = encode_request(freq);
    nexus::Decoder fdec{fbody.as_span()};
    nexus::Buffer fout;
    ASSERT_TRUE(run_dispatch(router, nexus::ApiKey::Fetch, 0, fdec, fout).has_value());

    const nexus::Labels fetch{{"api", "fetch"}, {"protocol", "native"}};
    EXPECT_EQ(metrics.counter("nexus_broker_requests_total", fetch).value(), 1U);
    EXPECT_EQ(metrics.counter("nexus_broker_request_errors_total", fetch).value(), 0U);
    EXPECT_GT(metrics.counter("nexus_broker_request_bytes_total", fetch).value(), 0U);
    // Se produjo un batch de 2 records: el fetch los sirve todos, así que `messages` sube 2.
    EXPECT_EQ(metrics.counter("nexus_broker_messages_total", fetch).value(), 2U);
}

}  // namespace
