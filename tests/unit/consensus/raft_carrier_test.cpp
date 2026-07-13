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
#include "consensus/raft_state.hpp"
#include "consensus/raft_state_store.hpp"
#include "consensus/raft_wire.hpp"
#include "protocol/codec.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"
#include "telemetry/metrics.hpp"

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
            nodes_.emplace(id,
                           Holder{.dir = std::move(dir), .part = std::move(*part), .carrier = {}});
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
    [[nodiscard]] nexus::RaftCarrier& carrier(nexus::NodeId id) { return *nodes_.at(id).carrier; }

    /// Cablea @p reg al portador de @p id (las series quedan etiquetadas por su réplica).
    void enable_metrics(nexus::NodeId id, nexus::MetricsRegistry& reg) {
        nodes_.at(id).carrier->set_metrics(reg);
    }

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
    ASSERT_TRUE(
        cluster.run_until([&] { return cluster.part(leader).high_watermark() == 3; }, 3000ms));
    EXPECT_EQ(cluster.part(leader).high_watermark(), 3);
}

TEST(RaftCarrier, Observe_Lider_ReportaRolTerminoYPeers) {
    CarrierCluster cluster({1, 2, 3}, 7);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 5000ms));
    const nexus::NodeId leader = *cluster.leader();

    const nexus::RaftObservation obs = cluster.carrier(leader).observe();
    EXPECT_EQ(obs.role, nexus::RaftRole::Leader);
    EXPECT_EQ(obs.topic, "orders");
    EXPECT_EQ(obs.partition, 0);
    EXPECT_GE(obs.term, 1);
    EXPECT_EQ(obs.peers.size(), 2U);  // 3 nodos → 2 peers.
    ASSERT_TRUE(obs.leader_hint.has_value());
    EXPECT_EQ(*obs.leader_hint, leader);
}

TEST(RaftCarrier, Observe_Seguidor_ReportaRolSeguidor) {
    CarrierCluster cluster({1, 2, 3}, 7);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 5000ms));
    const nexus::NodeId leader = *cluster.leader();
    const nexus::NodeId follower = leader == 1 ? 2 : 1;

    const nexus::RaftObservation obs = cluster.carrier(follower).observe();
    EXPECT_EQ(obs.role, nexus::RaftRole::Follower);
    EXPECT_EQ(obs.commit_index, cluster.part(follower).high_watermark());
}

TEST(RaftCarrier, NoLider_RechazaProduce) {
    CarrierCluster cluster({1, 2, 3}, 7);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 5000ms));
    const nexus::NodeId leader = *cluster.leader();
    const nexus::NodeId follower = leader == 1 ? 2 : 1;

    const auto rejected = cluster.part(follower).produce(make_batch(1));
    EXPECT_FALSE(rejected.has_value());
}

// Sumidero que descarta los mensajes (un nodo único no tiene peers a quien enviar).
class DiscardSink : public nexus::RaftMessageSink {
public:
    void send(const nexus::RaftEnvelope& /*envelope*/) override {}
};

TEST(RaftCarrier, PersisteEstadoAntesDeTransportar_TerminoYVotoEnDisco) {
    TempDir dir{"persist_1"};
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    nexus::RaftConfig cfg;
    cfg.random_seed = 7;
    auto part = nexus::ReplicatedPartition::create(1, {}, std::move(*plog), cfg);
    ASSERT_TRUE(part.has_value());
    auto store = nexus::RaftStateStore::open((dir.path() / "raft-state").string());
    ASSERT_TRUE(store.has_value());

    DiscardSink sink;
    nexus::RaftCarrier carrier{"orders", 0, part->raft(), sink, &*store};
    ASSERT_TRUE(carrier.recover().has_value());  // log vacío -> estado inicial.

    nexus::MonoTime now{};
    for (int i = 0; i < 300 && !part->is_leader(); ++i) {
        now += 10ms;
        carrier.on_tick(now);
    }
    ASSERT_TRUE(part->is_leader());

    // El portador guardó término/voto (regla §5) antes de transportar: debe estar en disco.
    const auto reloaded = store->load();
    ASSERT_TRUE(reloaded.has_value());
    EXPECT_EQ(reloaded->current_term(), part->raft().current_term());
    ASSERT_TRUE(reloaded->voted_for().has_value());
    EXPECT_EQ(*reloaded->voted_for(), 1);  // se votó a sí mismo.
}

TEST(RaftCarrier, Recover_SiembraElEstadoLeidoDeDisco) {
    TempDir dir{"recover_1"};
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    auto store = nexus::RaftStateStore::open((dir.path() / "raft-state").string());
    ASSERT_TRUE(store.has_value());
    // Estado previo en disco: término 5, voto a 1 (simula un reinicio).
    ASSERT_TRUE(store->save(nexus::RaftPersistentState::restore(5, 1)).has_value());

    nexus::RaftConfig cfg;
    cfg.random_seed = 7;
    auto part = nexus::ReplicatedPartition::create(1, {}, std::move(*plog), cfg);
    ASSERT_TRUE(part.has_value());

    DiscardSink sink;
    nexus::RaftCarrier carrier{"orders", 0, part->raft(), sink, &*store};
    EXPECT_EQ(part->raft().current_term(), 0);  // aún sin restaurar.
    ASSERT_TRUE(carrier.recover().has_value());
    EXPECT_EQ(part->raft().current_term(), 5);  // restaurado de disco.
}

// Lleva un votante único a líder conduciéndolo con su portador.
nexus::MonoTime drive_to_leader(nexus::RaftCarrier& carrier, nexus::ReplicatedPartition& part) {
    nexus::MonoTime now{};
    for (int i = 0; i < 300 && !part.is_leader(); ++i) {
        now += 10ms;
        carrier.on_tick(now);
    }
    return now;
}

TEST(RaftCarrier, Compacta_AlSuperarUmbralDeEntradasAplicadas) {
    TempDir dir{"compact_1"};
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    nexus::RaftConfig cfg;
    cfg.random_seed = 7;
    auto part = nexus::ReplicatedPartition::create(1, {}, std::move(*plog), cfg);
    ASSERT_TRUE(part.has_value());

    DiscardSink sink;
    nexus::RaftCarrier carrier{"orders",
                               0,
                               part->raft(),
                               sink,
                               nullptr,
                               &part->raft_log(),
                               nexus::CompactionPolicy{.applied_entries_threshold = 3}};
    nexus::MonoTime now = drive_to_leader(carrier, *part);
    ASSERT_TRUE(part->is_leader());
    ASSERT_EQ(part->raft_log().snapshot_index(), 0);  // nada compactado todavía.

    // Tres entradas aplicadas (votante único: confirma de inmediato) alcanzan el umbral.
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(part->produce(make_batch(1)).has_value());
    }
    ASSERT_EQ(part->commit_index(), 3);

    now += 10ms;
    carrier.on_tick(now);
    EXPECT_EQ(part->raft_log().snapshot_index(), 3);  // compactó hasta el commit_index.
}

TEST(RaftCarrier, NoCompacta_BajoElUmbral) {
    TempDir dir{"compact_2"};
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    nexus::RaftConfig cfg;
    cfg.random_seed = 7;
    auto part = nexus::ReplicatedPartition::create(1, {}, std::move(*plog), cfg);
    ASSERT_TRUE(part.has_value());

    DiscardSink sink;
    nexus::RaftCarrier carrier{"orders",
                               0,
                               part->raft(),
                               sink,
                               nullptr,
                               &part->raft_log(),
                               nexus::CompactionPolicy{.applied_entries_threshold = 10}};
    nexus::MonoTime now = drive_to_leader(carrier, *part);
    ASSERT_TRUE(part->is_leader());
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(part->produce(make_batch(1)).has_value());
    }
    now += 10ms;
    carrier.on_tick(now);
    EXPECT_EQ(part->raft_log().snapshot_index(), 0);  // 3 < 10: no compacta.
}

TEST(RaftCarrier, NoCompacta_PoliticaDesactivadaPorDefecto) {
    // Con `RaftLog` pero sin política (umbral 0, por defecto) el portador nunca compacta.
    TempDir dir{"compact_3"};
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    nexus::RaftConfig cfg;
    cfg.random_seed = 7;
    auto part = nexus::ReplicatedPartition::create(1, {}, std::move(*plog), cfg);
    ASSERT_TRUE(part.has_value());

    DiscardSink sink;
    nexus::RaftCarrier carrier{"orders", 0, part->raft(), sink, nullptr, &part->raft_log()};
    nexus::MonoTime now = drive_to_leader(carrier, *part);
    ASSERT_TRUE(part->is_leader());
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(part->produce(make_batch(1)).has_value());
    }
    now += 10ms;
    carrier.on_tick(now);
    EXPECT_EQ(part->raft_log().snapshot_index(), 0);  // política desactivada: nunca compacta.
}

// Etiquetas de la réplica `(orders, 0)` que usa el portador para sus series.
const nexus::Labels kReplicaLabels{{"topic", "orders"}, {"partition", "0"}};

TEST(RaftCarrier, Metrics_LiderUnico_PublicaRolTerminoYCommit) {
    TempDir dir{"metrics_1"};
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    nexus::RaftConfig cfg;
    cfg.random_seed = 7;
    auto part = nexus::ReplicatedPartition::create(1, {}, std::move(*plog), cfg);
    ASSERT_TRUE(part.has_value());

    DiscardSink sink;
    nexus::RaftCarrier carrier{"orders", 0, part->raft(), sink};
    nexus::MetricsRegistry reg;
    carrier.set_metrics(reg);

    nexus::MonoTime now = drive_to_leader(carrier, *part);
    ASSERT_TRUE(part->is_leader());
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(part->produce(make_batch(1)).has_value());
    }
    now += 10ms;
    carrier.on_tick(now);  // publica el estado tras avanzar la FSM.

    EXPECT_EQ(reg.gauge("nexus_raft_leader", kReplicaLabels).value(), 1);  // es líder.
    EXPECT_GE(reg.gauge("nexus_raft_term", kReplicaLabels).value(),
              1);  // ganó al menos un término.
    EXPECT_EQ(reg.gauge("nexus_raft_commit_index", kReplicaLabels).value(), 3);  // high-watermark.
    // Votante único: confirma de inmediato, así que el log va a la par del commit y sin backlog.
    EXPECT_EQ(reg.gauge("nexus_raft_log_last_index", kReplicaLabels).value(), 3);
    EXPECT_EQ(reg.gauge("nexus_raft_uncommitted_entries", kReplicaLabels).value(), 0);
}

TEST(RaftCarrier, Metrics_LatenciaDeCommit_SeObservaAlConfirmar) {
    TempDir dir{"metrics_lat"};
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    nexus::RaftConfig cfg;
    cfg.random_seed = 7;
    auto part = nexus::ReplicatedPartition::create(1, {}, std::move(*plog), cfg);
    ASSERT_TRUE(part.has_value());

    DiscardSink sink;
    nexus::RaftCarrier carrier{"orders", 0, part->raft(), sink};
    nexus::MetricsRegistry reg;
    carrier.set_metrics(reg);

    nexus::MonoTime now = drive_to_leader(carrier, *part);
    ASSERT_TRUE(part->is_leader());
    ASSERT_TRUE(
        part->produce(make_batch(1)).has_value());  // índice 1 (votante único: confirma ya).
    carrier.note_proposed(now);                     // sella el propose en `now`.
    now += 50ms;
    carrier.on_tick(now);  // al ver commit >= 1, observa la latencia (50 ms).

    const nexus::Histogram& hist =
        reg.histogram("nexus_raft_commit_latency_seconds", kReplicaLabels);
    EXPECT_EQ(hist.count(), 1U);
    EXPECT_NEAR(hist.sum(), 0.05, 1e-6);
}

TEST(RaftCarrier, Metrics_ReplicacionAQuorum_CuentaMensajesYEntradas) {
    CarrierCluster cluster({1, 2, 3}, 7);
    nexus::MetricsRegistry r1;
    nexus::MetricsRegistry r2;
    nexus::MetricsRegistry r3;
    cluster.enable_metrics(1, r1);
    cluster.enable_metrics(2, r2);
    cluster.enable_metrics(3, r3);

    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 5000ms));
    const nexus::NodeId leader = *cluster.leader();
    ASSERT_TRUE(cluster.part(leader).produce(make_batch(3)).has_value());
    ASSERT_TRUE(
        cluster.run_until([&] { return cluster.part(leader).high_watermark() == 3; }, 3000ms));

    nexus::MetricsRegistry& leader_reg = leader == 1 ? r1 : (leader == 2 ? r2 : r3);
    nexus::MetricsRegistry& follower_reg = leader == 1 ? r2 : r1;

    // El líder replicó entradas a los seguidores y publicó su commit_index (entradas del log de
    // Raft, no registros: un batch es una entrada).
    EXPECT_GT(leader_reg.counter("nexus_raft_entries_replicated_total", kReplicaLabels).value(),
              0U);
    EXPECT_GT(leader_reg.counter("nexus_raft_messages_sent_total", kReplicaLabels).value(), 0U);
    EXPECT_GT(cluster.part(leader).commit_index(), 0U);
    EXPECT_EQ(leader_reg.gauge("nexus_raft_commit_index", kReplicaLabels).value(),
              static_cast<std::int64_t>(cluster.part(leader).commit_index()));
    // Un seguidor recibió mensajes del líder (AppendEntries) y respondió.
    EXPECT_GT(follower_reg.counter("nexus_raft_messages_received_total", kReplicaLabels).value(),
              0U);
    EXPECT_GT(follower_reg.counter("nexus_raft_messages_sent_total", kReplicaLabels).value(), 0U);

    // En régimen estacionario (todo replicado a quórum): el log del líder iguala su commit_index,
    // no queda backlog y cada seguidor está al día (lag 0).
    EXPECT_EQ(leader_reg.gauge("nexus_raft_log_last_index", kReplicaLabels).value(),
              leader_reg.gauge("nexus_raft_commit_index", kReplicaLabels).value());
    EXPECT_EQ(leader_reg.gauge("nexus_raft_uncommitted_entries", kReplicaLabels).value(), 0);
    for (const nexus::NodeId peer : {nexus::NodeId{1}, nexus::NodeId{2}, nexus::NodeId{3}}) {
        if (peer == leader) {
            continue;
        }
        const nexus::Labels peer_labels{
            {"topic", "orders"}, {"partition", "0"}, {"peer", std::to_string(peer)}};
        EXPECT_EQ(leader_reg.gauge("nexus_raft_follower_lag", peer_labels).value(), 0)
            << "peer " << peer;
    }
}

}  // namespace
