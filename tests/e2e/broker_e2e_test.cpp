// E2E del broker mono-nodo: un cliente crudo (Socket::connect + framing manual por ::send/::recv)
// produce y luego consume contra un `Server` real corriendo en su propio hilo (reactor io_uring).
// Cierra el lazo protocolo↔red↔dominio que las pruebas unitarias del router no cubren.
#include <gtest/gtest.h>
#include <sys/socket.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "io/socket.hpp"
#include "protocol/codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/frame.hpp"
#include "protocol/messages.hpp"
#include "server/server.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_e2e_" + std::string{tag} + "_" +
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

// Envía todos los bytes (bloqueante); `false` si el par cerró o hubo error.
bool send_all(int fd, nexus::ByteSpan data) {
    const auto* ptr = reinterpret_cast<const char*>(data.data());  // NOLINT(*-reinterpret-cast)
    std::size_t left = data.size();
    while (left > 0) {
        const ssize_t sent = ::send(fd, ptr, left, MSG_NOSIGNAL);
        if (sent <= 0) {
            return false;
        }
        ptr += sent;
        left -= static_cast<std::size_t>(sent);
    }
    return true;
}

// Lee exactamente `buf.size()` bytes (bloqueante); `false` si el par cerró antes.
bool recv_exact(int fd, nexus::MutByteSpan buf) {
    auto* ptr = reinterpret_cast<char*>(buf.data());  // NOLINT(*-reinterpret-cast)
    std::size_t left = buf.size();
    while (left > 0) {
        const ssize_t got = ::recv(fd, ptr, left, 0);
        if (got <= 0) {
            return false;
        }
        ptr += got;
        left -= static_cast<std::size_t>(got);
    }
    return true;
}

// Construye y envía una trama: cabecera (con `length`) + cuerpo ya codificado.
bool send_frame(int fd, nexus::ApiKey key, std::uint32_t correlation_id, nexus::ByteSpan body) {
    nexus::FrameHeader header;
    header.api_key = key;
    header.api_version = 0;
    header.correlation_id = correlation_id;
    header.length = nexus::FrameHeader::length_for(body.size());
    nexus::Buffer frame;
    nexus::Encoder enc{frame};
    header.encode(enc);
    frame.append(body);
    return send_all(fd, frame.as_span());
}

// Recibe una trama completa; devuelve sus bytes (cabecera incluida) o vacío si falla.
std::vector<std::byte> recv_frame(int fd) {
    std::array<std::byte, sizeof(std::uint32_t)> len_bytes{};
    if (!recv_exact(fd, len_bytes)) {
        return {};
    }
    nexus::Decoder len_dec{nexus::ByteSpan{len_bytes.data(), len_bytes.size()}};
    const nexus::expected<std::uint32_t> length = len_dec.get_u32();
    if (!length) {
        return {};
    }
    std::vector<std::byte> frame(len_bytes.size() + *length);
    std::memcpy(frame.data(), len_bytes.data(), len_bytes.size());
    const nexus::MutByteSpan rest{frame.data() + len_bytes.size(), *length};
    if (!recv_exact(fd, rest)) {
        return {};
    }
    return frame;
}

// Bytes de un RecordBatch con `count` records (relleno opaco) para ProduceRequest.batch.
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

TEST(BrokerE2E, ProduceLuegoFetch_RoundTripPorSocket) {
    TempDir dir{"roundtrip"};
    nexus::Server::Config config;
    config.host = "127.0.0.1";
    config.port = 0;  // puerto efímero
    config.data_dir = dir.path();
    config.advertised_host = "127.0.0.1";

    std::optional<nexus::Server> server;
    try {
        server.emplace(std::move(config));
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }

    ASSERT_TRUE(server->create_topic("t", 1).has_value());
    ASSERT_TRUE(server->bind().has_value());
    const std::uint16_t port = server->port();
    ASSERT_NE(port, 0);

    std::thread server_thread{[&server] { server->run(); }};

    nexus::expected<nexus::Socket> client = nexus::Socket::connect("127.0.0.1", port);
    ASSERT_TRUE(client.has_value());
    const int fd = client->fd();

    // --- Produce: 3 records a "t"/0 ---
    const std::vector<std::byte> batch = encode_batch(3);
    nexus::ProduceRequest preq;
    preq.topic = "t";
    preq.partition = 0;
    preq.batch = nexus::ByteSpan{batch.data(), batch.size()};
    nexus::Buffer pbody;
    nexus::Encoder penc{pbody};
    preq.encode(penc);
    ASSERT_TRUE(send_frame(fd, nexus::ApiKey::Produce, /*correlation_id=*/1, pbody.as_span()));

    {
        const std::vector<std::byte> rframe = recv_frame(fd);
        ASSERT_FALSE(rframe.empty());
        nexus::Decoder dec{nexus::ByteSpan{rframe.data(), rframe.size()}};
        const nexus::expected<nexus::FrameHeader> header = nexus::FrameHeader::decode(dec);
        ASSERT_TRUE(header.has_value());
        EXPECT_EQ(header->correlation_id, 1U);
        const nexus::expected<nexus::ProduceResponse> resp = nexus::ProduceResponse::decode(dec);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp->error_code, nexus::WireError::None);
        EXPECT_EQ(resp->base_offset, 0);
    }

    // --- Fetch: desde el offset 0 ---
    nexus::FetchRequest freq;
    freq.topic = "t";
    freq.partition = 0;
    freq.fetch_offset = 0;
    freq.max_bytes = 64 * 1024;
    nexus::Buffer fbody;
    nexus::Encoder fenc{fbody};
    freq.encode(fenc);
    ASSERT_TRUE(send_frame(fd, nexus::ApiKey::Fetch, /*correlation_id=*/2, fbody.as_span()));

    {
        const std::vector<std::byte> rframe = recv_frame(fd);
        ASSERT_FALSE(rframe.empty());
        nexus::Decoder dec{nexus::ByteSpan{rframe.data(), rframe.size()}};
        const nexus::expected<nexus::FrameHeader> header = nexus::FrameHeader::decode(dec);
        ASSERT_TRUE(header.has_value());
        EXPECT_EQ(header->correlation_id, 2U);
        const nexus::expected<nexus::FetchResponse> resp = nexus::FetchResponse::decode(dec);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp->error_code, nexus::WireError::None);
        EXPECT_EQ(resp->high_watermark, 3);
        EXPECT_FALSE(resp->batches.empty());
    }

    client->close();
    server->stop();
    server_thread.join();
}

// Multi-reactor (N=2): produce/fetch a particiones de **distinto** núcleo por el mismo socket
// (enrutado cross-core, ADR-0026) y CreateTopic por protocolo con fan-out a ambos núcleos. Bajo
// TSan, además, comprueba que el camino thread-per-core no tiene carreras.
TEST(BrokerE2E, MultiReactor_ProduceFetchYCreateTopicCrossCore) {
    TempDir dir{"n2"};
    nexus::Server::Config config;
    config.host = "127.0.0.1";
    config.port = 0;
    config.data_dir = dir.path();
    config.advertised_host = "127.0.0.1";
    config.num_reactors = 2;  // p0 → núcleo 0; p1 → núcleo 1.

    std::optional<nexus::Server> server;
    try {
        server.emplace(std::move(config));
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }

    ASSERT_TRUE(server->create_topic("t", 2).has_value());
    ASSERT_TRUE(server->bind().has_value());
    const std::uint16_t port = server->port();
    ASSERT_NE(port, 0);

    std::thread server_thread{[&server] { server->run(); }};

    nexus::expected<nexus::Socket> client = nexus::Socket::connect("127.0.0.1", port);
    ASSERT_TRUE(client.has_value());
    const int fd = client->fd();

    // Produce a cada partición: la 0 la sirve el núcleo 0; la 1, el núcleo 1 (salto cross-core).
    for (std::int32_t partition = 0; partition < 2; ++partition) {
        const std::vector<std::byte> batch = encode_batch(partition + 1);
        nexus::ProduceRequest preq;
        preq.topic = "t";
        preq.partition = partition;
        preq.batch = nexus::ByteSpan{batch.data(), batch.size()};
        nexus::Buffer pbody;
        nexus::Encoder penc{pbody};
        preq.encode(penc);
        ASSERT_TRUE(send_frame(fd, nexus::ApiKey::Produce,
                               static_cast<std::uint32_t>(10 + partition), pbody.as_span()));
        const std::vector<std::byte> rframe = recv_frame(fd);
        ASSERT_FALSE(rframe.empty());
        nexus::Decoder dec{nexus::ByteSpan{rframe.data(), rframe.size()}};
        ASSERT_TRUE(nexus::FrameHeader::decode(dec).has_value());
        const nexus::expected<nexus::ProduceResponse> resp = nexus::ProduceResponse::decode(dec);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp->error_code, nexus::WireError::None);
        EXPECT_EQ(resp->base_offset, 0);
    }

    // Fetch de vuelta de ambas particiones: vuelven por el cruce con su high_watermark.
    for (std::int32_t partition = 0; partition < 2; ++partition) {
        nexus::FetchRequest freq;
        freq.topic = "t";
        freq.partition = partition;
        freq.fetch_offset = 0;
        freq.max_bytes = 64 * 1024;
        nexus::Buffer fbody;
        nexus::Encoder fenc{fbody};
        freq.encode(fenc);
        ASSERT_TRUE(send_frame(fd, nexus::ApiKey::Fetch, static_cast<std::uint32_t>(20 + partition),
                               fbody.as_span()));
        const std::vector<std::byte> rframe = recv_frame(fd);
        ASSERT_FALSE(rframe.empty());
        nexus::Decoder dec{nexus::ByteSpan{rframe.data(), rframe.size()}};
        ASSERT_TRUE(nexus::FrameHeader::decode(dec).has_value());
        const nexus::expected<nexus::FetchResponse> resp = nexus::FetchResponse::decode(dec);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp->error_code, nexus::WireError::None);
        EXPECT_EQ(resp->high_watermark, partition + 1);
        EXPECT_FALSE(resp->batches.empty());
    }

    // CreateTopic por protocolo: fan-out a ambos núcleos. Producir a su partición 1 (núcleo 1) solo
    // funciona si el fan-out la creó allí.
    nexus::CreateTopicRequest creq{.name = "u", .partition_count = 2, .replication_factor = 1};
    nexus::Buffer cbody;
    nexus::Encoder cenc{cbody};
    creq.encode(cenc);
    ASSERT_TRUE(send_frame(fd, nexus::ApiKey::CreateTopic, /*correlation_id=*/30, cbody.as_span()));
    {
        const std::vector<std::byte> rframe = recv_frame(fd);
        ASSERT_FALSE(rframe.empty());
        nexus::Decoder dec{nexus::ByteSpan{rframe.data(), rframe.size()}};
        ASSERT_TRUE(nexus::FrameHeader::decode(dec).has_value());
        const nexus::expected<nexus::CreateTopicResponse> resp =
            nexus::CreateTopicResponse::decode(dec);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp->error_code, nexus::WireError::None);
    }
    {
        const std::vector<std::byte> batch = encode_batch(5);
        nexus::ProduceRequest preq;
        preq.topic = "u";
        preq.partition = 1;  // dueña: núcleo 1.
        preq.batch = nexus::ByteSpan{batch.data(), batch.size()};
        nexus::Buffer pbody;
        nexus::Encoder penc{pbody};
        preq.encode(penc);
        ASSERT_TRUE(send_frame(fd, nexus::ApiKey::Produce, /*correlation_id=*/31, pbody.as_span()));
        const std::vector<std::byte> rframe = recv_frame(fd);
        ASSERT_FALSE(rframe.empty());
        nexus::Decoder dec{nexus::ByteSpan{rframe.data(), rframe.size()}};
        ASSERT_TRUE(nexus::FrameHeader::decode(dec).has_value());
        const nexus::expected<nexus::ProduceResponse> resp = nexus::ProduceResponse::decode(dec);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp->error_code, nexus::WireError::None);
    }

    client->close();
    server->stop();
    server_thread.join();
}

}  // namespace
