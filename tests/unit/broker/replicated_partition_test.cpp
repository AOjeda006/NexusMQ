// Pruebas de ReplicatedPartition: produce->propose, high-watermark=commit_index (acks=quorum),
// idempotencia por productor y rechazo en no-lider. El caso multi-nodo enruta los RPC de Raft entre
// varias ReplicatedPartition sobre un reloj virtual (latencia fija) para comprobar que una
// escritura no se hace visible hasta que el quorum la confirma.
#include "broker/replicated_partition.hpp"

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
#include <variant>
#include <vector>

#include "broker/partition_base.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "consensus/raft_node.hpp"
#include "consensus/raft_rpc.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace {

using namespace std::chrono_literals;
using Millis = std::chrono::milliseconds;

// Directorio temporal autolimpiante (un log por nodo).
class TempDir {
public:
    explicit TempDir(const std::string& tag)
        : path_(std::filesystem::temp_directory_path() / ("nexus_rp_" + tag)) {
        std::filesystem::remove_all(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

// Batch con campos de idempotencia explicitos y un payload de `count*4` bytes.
nexus::RecordBatch make_batch(nexus::ProducerId producer_id, nexus::Sequence base_seq,
                              std::int32_t count) {
    nexus::RecordBatchHeader header;
    header.producer_id = producer_id;
    header.producer_epoch = 0;
    header.base_sequence = base_seq;
    header.record_count = count;
    return nexus::RecordBatch{
        header, std::vector<std::byte>(static_cast<std::size_t>(count) * 4, std::byte{0xAB})};
}

// Cluster de ReplicatedPartition sobre reloj y red virtuales: enruta los RPC de `raft()` con
// latencia fija. Sin hilos ni sockets; reproducible por semilla.
class PartCluster {
public:
    PartCluster(const std::vector<nexus::NodeId>& voters, std::uint64_t seed) {
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
            nodes_.emplace(id, Holder{std::move(dir), std::move(*part)});
        }
    }

    [[nodiscard]] nexus::ReplicatedPartition& part(nexus::NodeId id) { return nodes_.at(id).part; }

    /// El lider (si hay exactamente uno).
    [[nodiscard]] std::optional<nexus::NodeId> leader() const {
        std::optional<nexus::NodeId> found;
        for (const auto& [id, holder] : nodes_) {
            if (holder.part.is_leader()) {
                if (found) {
                    return std::nullopt;
                }
                found = id;
            }
        }
        return found;
    }

    void run_for(Millis total, Millis step = Millis{10}) {
        for (Millis elapsed{0}; elapsed < total; elapsed += step) {
            tick(step);
        }
    }

    template <class Pred>
    bool run_until(Pred pred, Millis limit, Millis step = Millis{10}) {
        for (Millis elapsed{0}; elapsed < limit; elapsed += step) {
            tick(step);
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
    };
    struct InFlight {
        nexus::MonoTime due;
        nexus::RaftMessage msg;
    };

    static constexpr Millis kLatency{10};

    void tick(Millis step) {
        now_ += step;
        for (auto& [id, holder] : nodes_) {
            holder.part.raft().tick(now_);
        }
        pump();
        deliver_due();
        pump();
    }

    void pump() {
        for (auto& [id, holder] : nodes_) {
            for (nexus::RaftMessage& msg : holder.part.raft().take_messages()) {
                if (nodes_.contains(msg.to)) {
                    wire_.push_back(InFlight{.due = now_ + kLatency, .msg = std::move(msg)});
                }
            }
        }
    }

    void deliver_due() {
        std::vector<InFlight> due;
        std::vector<InFlight> rest;
        for (InFlight& f : wire_) {
            (f.due <= now_ ? due : rest).push_back(std::move(f));
        }
        wire_ = std::move(rest);
        for (const InFlight& f : due) {
            deliver(f.msg);
        }
    }

    template <class Payload>
    void schedule_reply(nexus::NodeId from, nexus::NodeId to, Payload payload) {
        wire_.push_back(InFlight{
            .due = now_ + kLatency,
            .msg = nexus::RaftMessage{.from = from, .to = to, .payload = std::move(payload)}});
    }

    void deliver(const nexus::RaftMessage& msg) {
        nexus::RaftNode& target = nodes_.at(msg.to).part.raft();
        if (const auto* a = std::get_if<nexus::RequestVoteArgs>(&msg.payload)) {
            schedule_reply(msg.to, msg.from, target.on_request_vote(now_, *a));
        } else if (const auto* a = std::get_if<nexus::AppendEntriesArgs>(&msg.payload)) {
            schedule_reply(msg.to, msg.from, target.on_append_entries(now_, *a));
        } else if (const auto* a = std::get_if<nexus::TimeoutNowArgs>(&msg.payload)) {
            target.on_timeout_now(now_, *a);
        } else if (const auto* r = std::get_if<nexus::RequestVoteReply>(&msg.payload)) {
            target.on_request_vote_reply(now_, msg.from, *r);
        } else if (const auto* r = std::get_if<nexus::AppendEntriesReply>(&msg.payload)) {
            target.on_append_entries_reply(now_, msg.from, *r);
        }
    }

    nexus::MonoTime now_{};
    std::unordered_map<nexus::NodeId, Holder> nodes_;
    std::vector<InFlight> wire_;
};

TEST(ReplicatedPartition, Produce_NodoUnico_SeEligeLiderYConfirmaDeInmediato) {
    PartCluster cluster({1}, 42);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    nexus::ReplicatedPartition& part = cluster.part(1);

    const nexus::expected<nexus::Offset> last = part.produce(make_batch(-1, -1, 3));
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 2);  // base 0, count 3 -> ultimo offset 2.
    EXPECT_EQ(part.commit_index(), 1);
    EXPECT_EQ(part.high_watermark(), 3);  // un solo votante: confirma al proponer.
}

TEST(ReplicatedPartition, SeSirvePorLaInterfazBase_DespachoPolimorfico) {
    // El broker sirve cualquier partición por `PartitionBase` sin conocer el tipo concreto: la
    // réplica debe responder produce/fetch/high_watermark por el despacho virtual.
    PartCluster cluster({1}, 42);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    nexus::PartitionBase& base = cluster.part(1);

    ASSERT_TRUE(base.is_leader());
    const nexus::expected<nexus::Offset> last = base.produce(make_batch(-1, -1, 3));
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(base.high_watermark(), 3);
    const nexus::expected<nexus::FetchResult> result = base.fetch(0, 64U * 1024U);
    ASSERT_TRUE(result.has_value());
}

TEST(ReplicatedPartition, Fetch_TrasProduceConfirmado_DevuelveBatches) {
    PartCluster cluster({1}, 42);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    nexus::ReplicatedPartition& part = cluster.part(1);
    ASSERT_TRUE(part.produce(make_batch(-1, -1, 4)).has_value());

    const nexus::expected<nexus::FetchResult> result = part.fetch(0, 64U * 1024U);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->batches.empty());
    EXPECT_EQ(result->next_offset, part.high_watermark());
}

TEST(ReplicatedPartition, Produce_NoEsLider_DevuelveUnsupported) {
    PartCluster cluster({1, 2, 3}, 7);
    // Sin conducir la eleccion: todos arrancan como seguidores.
    const nexus::expected<nexus::Offset> rejected = cluster.part(1).produce(make_batch(-1, -1, 1));
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), nexus::ErrorCode::Unsupported);
}

TEST(ReplicatedPartition, Produce_Idempotente_Duplicado_NoReanexa) {
    PartCluster cluster({1}, 42);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    nexus::ReplicatedPartition& part = cluster.part(1);

    ASSERT_TRUE(part.produce(make_batch(/*pid=*/1, /*base=*/0, /*count=*/3)).has_value());
    const nexus::Offset hw_before = part.high_watermark();
    const nexus::expected<nexus::Offset> dup = part.produce(make_batch(1, 0, 3));
    ASSERT_TRUE(dup.has_value());                 // se reconoce sin error.
    EXPECT_EQ(*dup, 2);                           // ultimo offset ya aplicado.
    EXPECT_EQ(part.high_watermark(), hw_before);  // no re-anexa.
}

TEST(ReplicatedPartition, Produce_Idempotente_Hueco_DevuelveOutOfRange) {
    PartCluster cluster({1}, 42);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    nexus::ReplicatedPartition& part = cluster.part(1);

    const nexus::expected<nexus::Offset> gap = part.produce(make_batch(/*pid=*/2, /*base=*/5, 1));
    ASSERT_FALSE(gap.has_value());
    EXPECT_EQ(gap.error().code(), nexus::ErrorCode::OutOfRange);
    EXPECT_EQ(part.high_watermark(), 0);  // nada anexado.
}

TEST(ReplicatedPartition, Produce_TresNodos_ConfirmaTrasQuorumYAvanzaHighWatermark) {
    PartCluster cluster({1, 2, 3}, 13);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    const nexus::NodeId leader = *cluster.leader();

    const nexus::expected<nexus::Offset> last = cluster.part(leader).produce(make_batch(-1, -1, 3));
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 2);
    // Acks=quorum: recien propuesto, todavia no confirmado (la mayoria no ha respondido).
    EXPECT_EQ(cluster.part(leader).commit_index(), 0);
    EXPECT_EQ(cluster.part(leader).high_watermark(), 0);

    const bool committed =
        cluster.run_until([&] { return cluster.part(leader).commit_index() >= 1; }, 2000ms);
    EXPECT_TRUE(committed);
    EXPECT_EQ(cluster.part(leader).high_watermark(), 3);  // ya visible para los consumidores.

    cluster.run_for(1000ms);  // los seguidores reciben la entrada confirmada.
    for (const nexus::NodeId id : {1, 2, 3}) {
        EXPECT_EQ(cluster.part(id).log().log_end_offset(), 3);
    }
}

}  // namespace
