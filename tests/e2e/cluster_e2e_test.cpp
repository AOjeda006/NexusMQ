// E2E del clúster multi-nodo (ADR-0025): 3 `Server` reales, cada uno con su plano de cliente y su
// plano inter-nodo (Raft) sobre sockets de loopback. Cierra el lazo que la simulación determinista
// del arnés no cubre: elección de líder, replicación a quórum y failover **sobre la red real**
// (transporte de D3.5 + persistencia/compactación de D1/D2/D3.6 juntos).
#include <gtest/gtest.h>
#include <sys/socket.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
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
                ("nexus_cluster_e2e_" + tag + "_" +
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

// --- Helpers de framing por socket crudo (cliente del plano de datos) ---

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

// Envía un Produce de `count` records a `topic`/0 y devuelve el WireError de la respuesta (con el
// `base_offset` por out-param si confirmó). Devuelve nullopt si el socket falló.
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

// Hace Fetch de `topic`/0 desde `offset` y devuelve el high_watermark (o nullopt si falló o el
// nodo devolvió error).
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

// --- Arnés de clúster: N `Server` reales sobre loopback con plano inter-nodo cableado ---

struct ClusterNode {
    std::unique_ptr<TempDir> dir;
    std::optional<nexus::Server> server;
    std::thread thread;
    nexus::NodeId id = 0;
    std::uint16_t client_port = 0;
    std::uint16_t cluster_port = 0;
    bool running = false;
};

class Cluster {
public:
    Cluster() = default;
    ~Cluster() { stop(); }
    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;

    // Construye, enlaza y arranca `n` nodos. Devuelve false si io_uring no está disponible (SKIP).
    [[nodiscard]] bool start(int n, std::uint64_t base_seed) {
        // Paso 1: construir y enlazar cada nodo (puertos cliente e inter-nodo efímeros).
        for (nexus::NodeId id = 1; id <= n; ++id) {
            auto node = std::make_unique<ClusterNode>();
            node->id = id;
            node->dir = std::make_unique<TempDir>("n" + std::to_string(id));
            nexus::Server::Config config = make_config(id, node->dir->path(), n, base_seed);
            try {
                node->server.emplace(std::move(config));
            } catch (const std::system_error&) {
                return false;  // io_uring no disponible.
            }
            if (!node->server
                     ->create_topic("r", 1, /*replication_factor=*/static_cast<std::int16_t>(n))
                     .has_value()) {
                return false;
            }
            if (!node->server->bind().has_value()) {
                return false;
            }
            node->client_port = node->server->port();
            node->cluster_port = node->server->cluster_port();
            nodes_.push_back(std::move(node));
        }
        // Paso 2: ya conocidos los puertos inter-nodo efímeros, cablear el directorio de peers de
        // cada nodo (los demás nodos) y arrancar.
        for (auto& node : nodes_) {
            node->server->set_peers(peers_excluding(node->id));
        }
        for (auto& node : nodes_) {
            node->thread = std::thread{[srv = &*node->server] { srv->run(); }};
            node->running = true;
        }
        return true;
    }

    void stop() {
        for (auto& node : nodes_) {
            stop_node_by_index(static_cast<int>(&node - nodes_.data()));
        }
        nodes_.clear();
    }

    // Para y une el nodo `idx` (0-based) — simula la caída de un nodo (failover).
    void stop_node_by_index(int idx) {
        ClusterNode& node = *nodes_[static_cast<std::size_t>(idx)];
        if (!node.running) {
            return;
        }
        node.server->stop();
        node.thread.join();
        node.running = false;
    }

    [[nodiscard]] int size() const { return static_cast<int>(nodes_.size()); }
    [[nodiscard]] std::uint16_t client_port(int idx) const {
        return nodes_[static_cast<std::size_t>(idx)]->client_port;
    }
    [[nodiscard]] bool running(int idx) const {
        return nodes_[static_cast<std::size_t>(idx)]->running;
    }

private:
    static nexus::Server::Config make_config(nexus::NodeId id, const std::filesystem::path& dir,
                                             int n, std::uint64_t base_seed) {
        nexus::Server::Config config;
        config.host = "127.0.0.1";
        config.port = 0;          // plano de cliente efímero
        config.cluster_port = 0;  // plano inter-nodo efímero
        config.data_dir = dir;
        config.advertised_host = "127.0.0.1";
        config.node_id = id;
        config.num_reactors = 1;
        // Timeouts cortos para una elección rápida sobre loopback; semilla distinta por nodo para
        // desincronizar los temporizadores y evitar votos divididos.
        config.raft_config.election_timeout_min = 100ms;
        config.raft_config.election_timeout_max = 200ms;
        config.raft_config.heartbeat_interval = 30ms;
        config.raft_config.random_seed = base_seed + id;
        // Membresía: los demás nodos (con direcciones provisionales; se reescriben tras `bind`).
        std::unordered_map<nexus::NodeId, nexus::PeerAddress> members;
        for (nexus::NodeId other = 1; other <= n; ++other) {
            if (other != id) {
                members.emplace(other, nexus::PeerAddress{.host = "127.0.0.1", .port = 0});
            }
        }
        config.peers = nexus::PeerDirectory{std::move(members)};
        return config;
    }

    // Directorio de peers (los demás nodos) con sus puertos inter-nodo reales ya enlazados.
    [[nodiscard]] nexus::PeerDirectory peers_excluding(nexus::NodeId self) const {
        std::unordered_map<nexus::NodeId, nexus::PeerAddress> members;
        for (const auto& node : nodes_) {
            if (node->id != self) {
                members.emplace(
                    node->id, nexus::PeerAddress{.host = "127.0.0.1", .port = node->cluster_port});
            }
        }
        return nexus::PeerDirectory{std::move(members)};
    }

    std::vector<std::unique_ptr<ClusterNode>> nodes_;
};

// Busca el líder produciendo una escritura: en rondas (hasta `budget`), prueba un Produce en cada
// nodo vivo; el primero que confirma (None) es el líder y la escritura se replicó a quórum sobre
// TCP. Devuelve el índice del líder (0-based) y su `base_offset`, o -1 si no emergió a tiempo.
int find_leader_via_produce(Cluster& cluster, const std::string& topic, std::int32_t count,
                            std::chrono::milliseconds budget, std::int64_t* base_offset) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        for (int idx = 0; idx < cluster.size(); ++idx) {
            if (!cluster.running(idx)) {
                continue;
            }
            nexus::expected<nexus::Socket> client =
                nexus::Socket::connect("127.0.0.1", cluster.client_port(idx));
            if (!client) {
                continue;
            }
            const std::optional<nexus::WireError> err =
                try_produce(client->fd(), topic, count, base_offset);
            client->close();
            if (err && *err == nexus::WireError::None) {
                return idx;
            }
        }
        std::this_thread::sleep_for(30ms);
    }
    return -1;
}

// Sondea el high_watermark de `topic`/0 en el nodo `idx` hasta que alcanza `expected` (la escritura
// se confirmó a quórum: el commit_index solo avanza con quórum de acks) o se agota el plazo.
bool wait_for_hwm(Cluster& cluster, int idx, const std::string& topic, std::int64_t expected,
                  std::chrono::milliseconds budget) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        nexus::expected<nexus::Socket> client =
            nexus::Socket::connect("127.0.0.1", cluster.client_port(idx));
        if (client) {
            const std::optional<std::int64_t> hwm = try_fetch_hwm(client->fd(), topic, 0);
            client->close();
            if (hwm && *hwm >= expected) {
                return true;
            }
        }
        std::this_thread::sleep_for(30ms);
    }
    return false;
}

TEST(ClusterE2E, TresNodos_EligenLiderYConfirmanEscrituraAQuorum) {
    Cluster cluster;
    if (!cluster.start(3, /*base_seed=*/100)) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }

    // Un líder emerge sobre la red real y confirma una escritura (commit exige quórum = 2/3 por
    // TCP).
    std::int64_t base_offset = -1;
    const int leader = find_leader_via_produce(cluster, "r", 3, 15s, &base_offset);
    ASSERT_GE(leader, 0) << "ningún líder confirmó una escritura en el plazo";
    EXPECT_EQ(base_offset, 0);  // primera escritura del log.

    // La escritura se confirma a quórum: el high_watermark del líder avanza a 3 cuando una mayoría
    // (2/3) replicó la entrada por TCP y el commit_index la alcanzó.
    EXPECT_TRUE(wait_for_hwm(cluster, leader, "r", 3, 10s))
        << "el high_watermark del líder no alcanzó la escritura confirmada";

    cluster.stop();
}

TEST(ClusterE2E, Replicacion_LaEscrituraSeVeEnUnSeguidor) {
    Cluster cluster;
    if (!cluster.start(3, /*base_seed=*/200)) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }

    std::int64_t base_offset = -1;
    const int leader = find_leader_via_produce(cluster, "r", 3, 15s, &base_offset);
    ASSERT_GE(leader, 0);

    // Un seguidor (cualquier nodo distinto del líder) acaba viendo la escritura: el líder le
    // replica la entrada por TCP y le propaga el commit (leader_commit en AppendEntries) → su hwm
    // llega a 3.
    const int follower = (leader + 1) % cluster.size();
    EXPECT_TRUE(wait_for_hwm(cluster, follower, "r", 3, 10s))
        << "el seguidor no replicó/expuso la escritura del líder";

    cluster.stop();
}

TEST(ClusterE2E, FailoverDeLider_LosSupervivientesEligenNuevoLiderYConfirman) {
    Cluster cluster;
    if (!cluster.start(3, /*base_seed=*/300)) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }

    // Primer líder confirma una escritura a quórum.
    std::int64_t base_offset = -1;
    const int leader1 = find_leader_via_produce(cluster, "r", 3, 15s, &base_offset);
    ASSERT_GE(leader1, 0);
    EXPECT_EQ(base_offset, 0);
    ASSERT_TRUE(wait_for_hwm(cluster, leader1, "r", 3, 10s));

    // Cae el líder: los 2 supervivientes son quórum del grupo (2/3) y eligen uno nuevo.
    cluster.stop_node_by_index(leader1);

    // Una segunda escritura confirma en el nuevo líder, que debe ser distinto del caído y continuar
    // el log (base_offset 3) replicando al otro superviviente para alcanzar quórum.
    std::int64_t base_offset_2 = -1;
    const int leader2 = find_leader_via_produce(cluster, "r", 3, 15s, &base_offset_2);
    ASSERT_GE(leader2, 0) << "los supervivientes no eligieron un nuevo líder tras el failover";
    EXPECT_NE(leader2, leader1);
    EXPECT_EQ(base_offset_2, 3);  // continúa tras el prefijo confirmado [0,2].
    EXPECT_TRUE(wait_for_hwm(cluster, leader2, "r", 6, 10s))
        << "el nuevo líder no confirmó la segunda escritura a quórum";

    cluster.stop();
}

}  // namespace
