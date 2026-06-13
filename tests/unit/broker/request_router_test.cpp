// Pruebas de RequestRouter: puente protocolo↔dominio sin red (codifica/decodifica en memoria).
#include "broker/request_router.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "broker/topic_manager.hpp"
#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "protocol/codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/frame.hpp"
#include "protocol/messages.hpp"

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

TEST(RequestRouter, ApiVersions_DevuelveRangosSoportados) {
    TempDir dir{"apiver"};
    nexus::TopicManager topics{dir.path()};
    nexus::RequestRouter router{topics, /*node_id=*/0, "127.0.0.1", 9092};

    const nexus::Buffer body = encode_request(nexus::ApiVersionsRequest{.client_version = 0});
    nexus::Decoder dec{body.as_span()};
    nexus::Buffer out;
    ASSERT_TRUE(router.dispatch(nexus::ApiKey::ApiVersions, 0, dec, out).has_value());

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
    ASSERT_TRUE(router.dispatch(nexus::ApiKey::Produce, 0, dec, out).has_value());

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
    ASSERT_TRUE(router.dispatch(nexus::ApiKey::Produce, 0, dec, out).has_value());

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
    ASSERT_TRUE(router.dispatch(nexus::ApiKey::Produce, 0, pdec, pout).has_value());

    nexus::FetchRequest freq;
    freq.topic = "t";
    freq.partition = 0;
    freq.fetch_offset = 0;
    freq.max_bytes = 64 * 1024;
    nexus::Buffer fbody = encode_request(freq);
    nexus::Decoder fdec{fbody.as_span()};
    nexus::Buffer fout;
    ASSERT_TRUE(router.dispatch(nexus::ApiKey::Fetch, 0, fdec, fout).has_value());

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
    ASSERT_TRUE(router.dispatch(nexus::ApiKey::CreateTopic, 0, cdec, cout).has_value());
    nexus::Decoder cresp_dec{cout.as_span()};
    const nexus::expected<nexus::CreateTopicResponse> cresp =
        nexus::CreateTopicResponse::decode(cresp_dec);
    ASSERT_TRUE(cresp.has_value());
    EXPECT_EQ(cresp->error_code, nexus::WireError::None);
    EXPECT_EQ(topics.topic_count(), 1U);

    nexus::Buffer mbody = encode_request(nexus::MetadataRequest{});
    nexus::Decoder mdec{mbody.as_span()};
    nexus::Buffer mout;
    ASSERT_TRUE(router.dispatch(nexus::ApiKey::Metadata, 0, mdec, mout).has_value());
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
    ASSERT_TRUE(router.dispatch(nexus::ApiKey::OffsetCommit, 0, cdec, cout).has_value());
    nexus::Decoder cresp_dec{cout.as_span()};
    const nexus::expected<nexus::OffsetCommitResponse> cresp =
        nexus::OffsetCommitResponse::decode(cresp_dec);
    ASSERT_TRUE(cresp.has_value());
    EXPECT_EQ(cresp->error_code, nexus::WireError::None);

    nexus::Buffer fbody =
        encode_request(nexus::OffsetFetchRequest{.group = "g", .topic = "t", .partition = 0});
    nexus::Decoder fdec{fbody.as_span()};
    nexus::Buffer fout;
    ASSERT_TRUE(router.dispatch(nexus::ApiKey::OffsetFetch, 0, fdec, fout).has_value());
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
    ASSERT_TRUE(router.dispatch(nexus::ApiKey::OffsetFetch, 0, fdec, fout).has_value());
    nexus::Decoder fresp_dec{fout.as_span()};
    const nexus::expected<nexus::OffsetFetchResponse> fresp =
        nexus::OffsetFetchResponse::decode(fresp_dec);
    ASSERT_TRUE(fresp.has_value());
    EXPECT_EQ(fresp->error_code, nexus::WireError::None);
    EXPECT_EQ(fresp->offset, -1);
}

TEST(RequestRouter, ApiKeyNoSoportada_DevuelveError) {
    TempDir dir{"unsup"};
    nexus::TopicManager topics{dir.path()};
    nexus::RequestRouter router{topics, 0, "127.0.0.1", 9092};
    nexus::Buffer body;
    nexus::Decoder dec{body.as_span()};
    nexus::Buffer out;
    EXPECT_FALSE(router.dispatch(nexus::ApiKey::JoinGroup, 0, dec, out).has_value());
}

}  // namespace
