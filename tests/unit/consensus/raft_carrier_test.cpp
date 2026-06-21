// Pruebas de RaftCarrier (ADR-0025): el portador conduce la FSM de cada réplica (on_tick) y enruta
// los RPC recibidos (on_message) devolviendo las replies por el RaftMessageSink. Un doble de
// transporte (red virtual determinista) serializa cada RaftEnvelope por el wire y lo entrega al
// portador destino, de modo que el test ejercita portador + sobre juntos: elección de líder y
// replicación a quórum, sin hilos ni sockets.
#include "consensus/raft_carrier.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "broker/replicated_partition.hpp"
#include "common/bytes.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "consensus/raft_node.hpp"
#include "consensus/raft_wire.hpp"
#include "protocol/codec.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace {

using namespace std::chrono_literals;
using Millis = std::chrono::milliseconds;

class TempDir {
public:
    explicit TempDir(const std::string& tag)
        : path_(std::filesystem::temp_directory_path() / ("nexus_carrier_" + tag)) {
        std::filesystem::remove_all(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

nexus::RecordBatch make_batch(std::int32_t count) {
    nexus::RecordBatchHeader header;
    header.producer_id = -1;
    header.producer_epoch = 0;
    header.base_sequence = -1;
    header.record_count = count;
    return nexus::RecordBatch{
        header, std::vector<std::byte>(static_cast<std::size_t>(count) * 4, std::byte{0xAB})};
}

// Cluster de réplicas conducidas por RaftCarrier sobre una red virtual: cada sobre se serializa por
// el wire (round-trip) y se encola; el drenado lo entrega al portador destino. Reloj virtual; sin
// latencia (Raft tolera cualquier *timing*). El sumidero es el propio cluster.
class CarrierCluster : public nexus::RaftMessageSink {
public:
    CarrierCluster(const std::vector<nexus::NodeId>& voters, std::uint64_t seed) {
        for (const nexus::NodeId id : voters) {
            std::vector<nexus::NodeId> peers;
            for (const nexus::NodeId other : voters) {
                if (other != id) {
                    peers.push_back(other);
                }
            }
            auto dir = std::make_unique<TempDir>(std::to_string(seed) + "_" + std::to_string(id));
            auto plog = nexus::PartitionLog::open(dir->path(), nexus::LogConfig{});
            EXPECT_TRUE(plog.has_value());
            nexus::RaftConfig cfg;
            cfg.random_seed = seed;
            auto part =
                nexus::ReplicatedPartition::create(id, std::move(peers), std::move(*plog), cfg);
            EXPECT_TRUE(part.has_value());
            nodes_.emplace(id, Holder{.dir = std::move(dir), .part = std::move(*part), .carrier = {}});
        }
        // Los portadores se crean tras fijar las particiones (la referencia a raft() es estable).
        for (auto& [id, holder] : nodes_) {
            holder.carrier =
                std::make_unique<nexus::RaftCarrier>("orders", 0, holder.part.raft(), *this);
        }
    }

    void send(const nexus::RaftEnvelope& envelope) override {
        // Round-trip por el wire: ejercita la (de)serialización del sobre en el camino real.
        nexus::Buffer buffer;
        nexus::Encoder enc{buffer};
        envelope.encode(enc);
        nexus::Decoder dec{buffer.as_span()};
        auto decoded = nexus::RaftEnvelope::decode(dec);
        EXPECT_TRUE(decoded.has_value());
        queue_.push_back(std::move(*decoded));
    }

    [[nodiscard]] nexus::ReplicatedPartition& part(nexus::NodeId id) { return nodes_.at(id).part; }

    [[nodiscard]] std::optional<nexus::NodeId> leader() const {
        std::optional<nexus::NodeId> found;
        for (const auto& [id, holder] : nodes_) {
            if (holder.part.is_leader()) {
                if (found) {
                    return std::nullopt;  // más de uno: aún no estable.
                }
                found = id;
            }
        }
        return found;
    }

    void step(Millis dt = Millis{10}) {
        now_ += dt;
        for (auto& [id, holder] : nodes_) {
            holder.carrier->on_tick(now_);
        }
        drain();
    }

    template <class Pred>
    bool run_until(Pred pred, Millis limit, Millis dt = Millis{10}) {
        for (Millis elapsed{0}; elapsed < limit; elapsed += dt) {
            step(dt);
            if (pred()) {
                return true;
            }
        }
        return pred();
    }

private:
    struct Holder {
        std::unique_ptr<TempDir> dir;
        nexus::ReplicatedPartition part;
        std::unique_ptr<nexus::RaftCarrier> carrier;
    };

    // Entrega los sobres encolados al portador destino, en rondas, hasta que la red se aquieta.
    void drain() {
        for (int round = 0; round < kMaxRounds && !queue_.empty(); ++round) {
            std::vector<nexus::RaftEnvelope> batch = std::move(queue_);
            queue_.clear();
            for (const nexus::RaftEnvelope& env : batch) {
                const auto it = nodes_.find(env.message.to);
                if (it != nodes_.end()) {
                    it->second.carrier->on_message(now_, env.message);
                }
            }
        }
    }

    static constexpr int kMaxRounds = 100;

    nexus::MonoTime now_{};
    std::unordered_map<nexus::NodeId, Holder> nodes_;
    std::vector<nexus::RaftEnvelope> queue_;
};

TEST(RaftCarrier, TresNodos_EligenUnLiderUnico) {
    CarrierCluster cluster({1, 2, 3}, 7);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 5000ms));
    EXPECT_TRUE(cluster.leader().has_value());
}

TEST(RaftCarrier, TresNodos_ProduceEnLider_ReplicaYConfirmaAQuorum) {
    CarrierCluster cluster({1, 2, 3}, 7);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 5000ms));
    const nexus::NodeId leader = *cluster.leader();

    const auto last = cluster.part(leader).produce(make_batch(3));
    ASSERT_TRUE(last.has_value());
    // Tras proponer, deja replicar: la escritura se hace visible cuando el quórum la confirma.
    ASSERT_TRUE(cluster.run_until(
        [&] { return cluster.part(leader).high_watermark() == 3; }, 3000ms));
    EXPECT_EQ(cluster.part(leader).high_watermark(), 3);
}

TEST(RaftCarrier, NoLider_RechazaProduce) {
    CarrierCluster cluster({1, 2, 3}, 7);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 5000ms));
    const nexus::NodeId leader = *cluster.leader();
    const nexus::NodeId follower = leader == 1 ? 2 : 1;

    const auto rejected = cluster.part(follower).produce(make_batch(1));
    EXPECT_FALSE(rejected.has_value());
}

}  // namespace
