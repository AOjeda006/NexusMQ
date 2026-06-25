// E2E del modo proxy del plano de datos (ADR-0006/0027, D4-3b): dos `Server` reales —un backend con
// el topic en modo nativo y un proxy opt-in que releva al backend—. Un cliente conecta al PROXY y
// hace Produce/Fetch; el proxy enruta (consistent-hashing) y releva las tramas al backend sobre una
// conexión del UpstreamPool, devolviéndola al pool al cerrar. Cierra el lazo accept → route →
// acquire → forward → release que las unitarias de Proxy/UpstreamPool no cubren.
#include <gtest/gtest.h>

#include "io/io_uring_backend.hpp"  // arrastra NEXUS_HAVE_IOURING

#ifdef NEXUS_HAVE_IOURING

#include <sys/socket.h>
#include <sys/time.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cluster/peer_directory.hpp"
#include "common/bytes.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "io/socket.hpp"
#include "protocol/codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/frame.hpp"
#include "protocol/messages.hpp"
#include "server/server.hpp"

namespace {

using namespace std::chrono_literals;

class TempDir {
public:
    explicit TempDir(const std::string& tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_proxy_server_e2e_" + tag + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

// --- Cliente del plano de datos por socket bloqueante (igual que el e2e de clúster) ---

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

std::optional<nexus::WireError> try_produce(int fd, const std::string& topic, std::int32_t count,
                                            std::int64_t* base_offset) {
    const std::vector<std::byte> batch = encode_batch(count);
    nexus::ProduceRequest preq;
    preq.topic = topic;
    preq.partition = 0;
    preq.batch = nexus::ByteSpan{batch.data(), batch.size()};
    nexus::Buffer pbody;
    nexus::Encoder penc{pbody};
    preq.encode(penc);
    if (!send_frame(fd, nexus::ApiKey::Produce, /*correlation_id=*/1, pbody.as_span())) {
        return std::nullopt;
    }
    const std::vector<std::byte> rframe = recv_frame(fd);
    if (rframe.empty()) {
        return std::nullopt;
    }
    nexus::Decoder dec{nexus::ByteSpan{rframe.data(), rframe.size()}};
    if (!nexus::FrameHeader::decode(dec).has_value()) {
        return std::nullopt;
    }
    const nexus::expected<nexus::ProduceResponse> resp = nexus::ProduceResponse::decode(dec);
    if (!resp) {
        return std::nullopt;
    }
    if (base_offset != nullptr) {
        *base_offset = resp->base_offset;
    }
    return resp->error_code;
}

std::optional<std::int64_t> try_fetch_hwm(int fd, const std::string& topic, std::int64_t offset) {
    nexus::FetchRequest freq;
    freq.topic = topic;
    freq.partition = 0;
    freq.fetch_offset = offset;
    freq.max_bytes = 64 * 1024;
    nexus::Buffer fbody;
    nexus::Encoder fenc{fbody};
    freq.encode(fenc);
    if (!send_frame(fd, nexus::ApiKey::Fetch, /*correlation_id=*/2, fbody.as_span())) {
        return std::nullopt;
    }
    const std::vector<std::byte> rframe = recv_frame(fd);
    if (rframe.empty()) {
        return std::nullopt;
    }
    nexus::Decoder dec{nexus::ByteSpan{rframe.data(), rframe.size()}};
    if (!nexus::FrameHeader::decode(dec).has_value()) {
        return std::nullopt;
    }
    const nexus::expected<nexus::FetchResponse> resp = nexus::FetchResponse::decode(dec);
    if (!resp || resp->error_code != nexus::WireError::None) {
        return std::nullopt;
    }
    return resp->high_watermark;
}

// Config de un nodo de un reactor (RF=1: el Produce confirma de inmediato) sobre loopback.
nexus::Server::Config make_node_config(const TempDir& dir, nexus::NodeId node_id) {
    nexus::Server::Config config;
    config.host = "127.0.0.1";
    config.port = 0;
    config.data_dir = dir.path() / "data";
    config.node_id = node_id;
    config.num_reactors = 1;
    return config;
}

bool iouring_available() {
    try {
        (void)nexus::IoUringBackend{8};
        return true;
    } catch (const std::system_error&) {
        return false;
    }
}

TEST(ProxyServerE2E, RelevaProduceYFetchAlNodoAguasArriba) {
    if (!iouring_available()) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    TempDir backend_dir{"backend"};
    TempDir proxy_dir{"proxy"};

    // Backend: nodo real con el topic, modo nativo directo.
    std::optional<nexus::Server> backend;
    try {
        backend.emplace(make_node_config(backend_dir, /*node_id=*/1));
    } catch (const std::system_error&) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    ASSERT_TRUE(backend->create_topic("orders", 1, /*replication_factor=*/1).has_value());
    ASSERT_TRUE(backend->bind().has_value());
    const std::uint16_t backend_port = backend->port();
    ASSERT_GT(backend_port, 0);

    // Proxy: modo opt-in que releva al backend (único nodo aguas arriba en el anillo).
    nexus::Server::Config proxy_cfg = make_node_config(proxy_dir, /*node_id=*/2);
    proxy_cfg.proxy.upstreams.emplace(
        nexus::NodeId{1}, nexus::PeerAddress{.host = "127.0.0.1", .port = backend_port});
    std::optional<nexus::Server> proxy;
    try {
        proxy.emplace(std::move(proxy_cfg));
    } catch (const std::system_error&) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    ASSERT_TRUE(proxy->bind().has_value());
    const std::uint16_t proxy_port = proxy->port();
    ASSERT_GT(proxy_port, 0);

    std::thread backend_thread{[srv = &*backend] { srv->run(); }};
    std::thread proxy_thread{[srv = &*proxy] { srv->run(); }};

    // Espera a que el relevo esté operativo: conecta al PROXY (no al backend) y produce hasta
    // confirmar (ambos servidores arrancando en paralelo). Un timeout de recv evita colgarse.
    std::optional<nexus::Socket> client;
    std::int64_t base_offset = -1;
    std::optional<nexus::WireError> produce_err;
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline) {
        nexus::expected<nexus::Socket> connected = nexus::Socket::connect("127.0.0.1", proxy_port);
        if (connected) {
            client = std::move(*connected);
            const timeval timeout{.tv_sec = 2, .tv_usec = 0};
            ::setsockopt(client->fd(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            produce_err = try_produce(client->fd(), "orders", 5, &base_offset);
            if (produce_err == nexus::WireError::None) {
                break;
            }
        }
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(produce_err.has_value() && *produce_err == nexus::WireError::None)
        << "el proxy no relevó el Produce al backend a tiempo";
    EXPECT_EQ(base_offset, 0);  // primera escritura del log del backend.

    // Fetch por el MISMO canal de proxy: con RF=1 la escritura está confirmada → high_watermark
    // = 5.
    ASSERT_TRUE(client.has_value());
    const std::optional<std::int64_t> hwm = try_fetch_hwm(client->fd(), "orders", 0);
    ASSERT_TRUE(hwm.has_value());
    EXPECT_EQ(*hwm, 5);

    client.reset();  // cierra el cliente: el relevo ve EOF y devuelve la conexión al pool.
    proxy->stop();
    backend->stop();
    proxy_thread.join();
    backend_thread.join();
}

}  // namespace

#else

TEST(ProxyServerE2E, OmitidoSinIoUring) {
    GTEST_SKIP() << "compilado sin io_uring";
}

#endif  // NEXUS_HAVE_IOURING
