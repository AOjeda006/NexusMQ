// Pruebas deterministas de RaftNode (ADR-0015): elección, voto y manejo de AppendEntries en el
// seguidor. El nodo es una máquina de estados sin E/S: se le inyecta `now` y se drenan sus
// mensajes.
#include "consensus/raft_node.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "common/bytes.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "consensus/raft_log.hpp"
#include "consensus/raft_rpc.hpp"
#include "consensus/raft_state.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const std::string& tag)
        : path_(std::filesystem::temp_directory_path() / ("nexus_raftnode_" + tag)) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] std::string meta_path() const { return (path_ / "raft-meta").string(); }
    [[nodiscard]] std::filesystem::path log_dir() const { return path_ / "log"; }

private:
    std::filesystem::path path_;
};

// Un nodo completo (dir + PartitionLog + RaftLog + RaftNode) con direcciones estables.
struct NodeBox {
    std::unique_ptr<TempDir> dir;
    std::unique_ptr<nexus::PartitionLog> plog;
    std::unique_ptr<nexus::RaftLog> rlog;
    std::unique_ptr<nexus::RaftNode> node;
};

std::unique_ptr<NodeBox> make_node(nexus::NodeId id, std::vector<nexus::NodeId> peers,
                                   const std::string& tag) {
    auto box = std::make_unique<NodeBox>();
    box->dir = std::make_unique<TempDir>(tag + "_" + std::to_string(id));
    auto plog = nexus::PartitionLog::open(box->dir->log_dir(), nexus::LogConfig{});
    EXPECT_TRUE(plog.has_value());
    box->plog = std::make_unique<nexus::PartitionLog>(std::move(*plog));
    auto rlog = nexus::RaftLog::open(*box->plog, box->dir->meta_path());
    EXPECT_TRUE(rlog.has_value());
    box->rlog = std::make_unique<nexus::RaftLog>(std::move(*rlog));
    nexus::RaftConfig cfg;
    cfg.random_seed = 7;
    box->node = std::make_unique<nexus::RaftNode>(id, std::move(peers), *box->rlog, cfg);
    return box;
}

nexus::MonoTime at(std::int64_t ms) {
    return nexus::MonoTime{} + std::chrono::milliseconds{ms};
}

// Entrada de datos con payload = RecordBatch de `records` records.
nexus::RaftLogEntry data_entry(nexus::Term term, nexus::Index index, std::int32_t records) {
    nexus::RecordBatchHeader header;
    header.base_offset = 0;
    header.record_count = records;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(8, std::byte{0xCD})};
    nexus::Buffer buffer;
    batch.encode(buffer);
    const nexus::ByteSpan span = buffer.as_span();
    return nexus::RaftLogEntry{.term = term,
                               .index = index,
                               .type = nexus::RaftEntryType::Data,
                               .payload = std::vector<std::byte>(span.begin(), span.end())};
}

// RecordBatch con `records` records (payload para `propose`).
nexus::RecordBatch make_batch(std::int32_t records) {
    nexus::RecordBatchHeader header;
    header.base_offset = 0;
    header.record_count = records;
    return nexus::RecordBatch{header, std::vector<std::byte>(8, std::byte{0xAB})};
}

// Entrega los mensajes proactivos de `sender` a sus destinos y enruta la respuesta de vuelta.
void route_messages(nexus::RaftNode& sender, std::vector<NodeBox*>& cluster, nexus::MonoTime now) {
    for (const nexus::RaftMessage& msg : sender.take_messages()) {
        nexus::RaftNode* target = nullptr;
        for (NodeBox* box : cluster) {
            if (box->node->id() == msg.to) {
                target = box->node.get();
            }
        }
        if (target == nullptr) {
            continue;
        }
        if (const auto* vote = std::get_if<nexus::RequestVoteArgs>(&msg.payload)) {
            const auto reply = target->on_request_vote(now, *vote);
            sender.on_request_vote_reply(now, msg.to, reply);
        } else if (const auto* append = std::get_if<nexus::AppendEntriesArgs>(&msg.payload)) {
            const auto reply = target->on_append_entries(now, *append);
            sender.on_append_entries_reply(now, msg.to, reply);
        }
    }
}

TEST(RaftNode, NodoUnico_SeAutoeligeLider) {
    auto box = make_node(1, {}, "single");
    box->node->tick(at(0));     // arma el temporizador.
    box->node->tick(at(2000));  // vence el election timeout (<= 1500).
    EXPECT_TRUE(box->node->is_leader());
    EXPECT_EQ(box->node->current_term(), 1);
}

TEST(RaftNode, TresNodos_EligenUnLider) {
    auto a = make_node(1, {2, 3}, "elect");
    auto b = make_node(2, {1, 3}, "elect");
    auto c = make_node(3, {1, 2}, "elect");
    std::vector<NodeBox*> cluster{a.get(), b.get(), c.get()};
    for (NodeBox* box : cluster) {
        box->node->tick(at(0));  // todos arman.
    }
    a->node->tick(at(2000));                      // el nodo 1 vence primero y se postula.
    route_messages(*a->node, cluster, at(2000));  // reparte RequestVote y recoge votos.

    EXPECT_TRUE(a->node->is_leader());
    EXPECT_EQ(a->node->current_term(), 1);
    EXPECT_EQ(b->node->role(), nexus::RaftRole::Follower);
    EXPECT_EQ(b->node->current_term(), 1);  // el voto subió su término.
    EXPECT_EQ(c->node->role(), nexus::RaftRole::Follower);
}

TEST(RaftNode, Heartbeat_EvitaNuevaEleccion) {
    auto a = make_node(1, {2, 3}, "hb");
    auto b = make_node(2, {1, 3}, "hb");
    auto c = make_node(3, {1, 2}, "hb");
    std::vector<NodeBox*> cluster{a.get(), b.get(), c.get()};
    for (NodeBox* box : cluster) {
        box->node->tick(at(0));
    }
    a->node->tick(at(2000));
    route_messages(*a->node, cluster, at(2000));  // 1 es líder; emite heartbeats.
    ASSERT_TRUE(a->node->is_leader());
    route_messages(*a->node, cluster, at(2000));  // reparte los heartbeats iniciales a 2 y 3.

    // El seguidor 2, con heartbeat reciente (t=2000), no se postula a t=2500.
    b->node->tick(at(2500));
    EXPECT_EQ(b->node->role(), nexus::RaftRole::Follower);
}

TEST(RaftNode, RequestVote_LogMenosActualizado_DenegaElVoto) {
    auto voter = make_node(1, {2}, "stale");
    // El votante tiene una entrada (término 5): su log adelanta al del candidato.
    const std::vector<nexus::RaftLogEntry> seed{data_entry(5, 1, 1)};
    ASSERT_TRUE(voter->rlog->append(seed).has_value());

    const nexus::RequestVoteArgs args{
        .term = 6, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0, .pre_vote = false};
    const auto reply = voter->node->on_request_vote(at(100), args);
    EXPECT_FALSE(reply.vote_granted);  // el log del candidato no está al día.
    EXPECT_EQ(reply.term, 6);          // pero adopta el término mayor.
}

TEST(RaftNode, AppendEntries_TerminoMayor_PasaASeguidor) {
    auto box = make_node(1, {}, "stepdown");
    box->node->tick(at(0));
    box->node->tick(at(2000));  // se convierte en líder, término 1.
    ASSERT_TRUE(box->node->is_leader());

    const nexus::AppendEntriesArgs args{.term = 5,
                                        .leader_id = 9,
                                        .prev_log_index = 0,
                                        .prev_log_term = 0,
                                        .entries = {},
                                        .leader_commit = 0,
                                        .leader_epoch = 1};
    const auto reply = box->node->on_append_entries(at(2100), args);
    EXPECT_TRUE(reply.success);
    EXPECT_EQ(box->node->role(), nexus::RaftRole::Follower);
    EXPECT_EQ(box->node->current_term(), 5);
}

TEST(RaftNode, AppendEntries_AnexaEntradasYAvanzaCommit) {
    auto box = make_node(1, {2}, "append");
    const nexus::AppendEntriesArgs args{.term = 2,
                                        .leader_id = 2,
                                        .prev_log_index = 0,
                                        .prev_log_term = 0,
                                        .entries = {data_entry(2, 1, 1), data_entry(2, 2, 1)},
                                        .leader_commit = 1,
                                        .leader_epoch = 1};
    const auto reply = box->node->on_append_entries(at(50), args);
    EXPECT_TRUE(reply.success);
    EXPECT_EQ(box->rlog->last_index(), 2);
    EXPECT_EQ(box->node->commit_index(), 1);  // min(leader_commit=1, last_index=2)
}

TEST(RaftNode, AppendEntries_PrevLogIndexInexistente_Falla) {
    auto box = make_node(1, {2}, "gap");
    const nexus::AppendEntriesArgs args{.term = 1,
                                        .leader_id = 2,
                                        .prev_log_index = 5,  // el seguidor no tiene tanto log
                                        .prev_log_term = 1,
                                        .entries = {},
                                        .leader_commit = 0,
                                        .leader_epoch = 1};
    const auto reply = box->node->on_append_entries(at(10), args);
    EXPECT_FALSE(reply.success);
    EXPECT_EQ(reply.conflict_index, 1);  // last_index (0) + 1
}

TEST(RaftNode, Propose_NoLider_DevuelveError) {
    auto box = make_node(1, {2, 3}, "propfollower");
    box->node->tick(at(0));  // sigue siendo seguidor.
    const auto idx = box->node->propose(make_batch(1));
    ASSERT_FALSE(idx.has_value());
    EXPECT_EQ(idx.error().code(), nexus::ErrorCode::Unsupported);
}

TEST(RaftNode, Propose_NodoUnico_ConfirmaInmediato) {
    auto box = make_node(1, {}, "prop1");
    box->node->tick(at(0));
    box->node->tick(at(2000));  // se autoelige líder (mayoría inmediata).
    ASSERT_TRUE(box->node->is_leader());

    const auto idx = box->node->propose(make_batch(1));
    ASSERT_TRUE(idx.has_value());
    EXPECT_EQ(*idx, 1);
    EXPECT_EQ(box->rlog->last_index(), 1);
    EXPECT_EQ(box->node->commit_index(), 1);  // sin peers: mayoría = uno mismo.
}

TEST(RaftNode, Propose_ReplicaYConfirmaPorMayoria) {
    auto a = make_node(1, {2, 3}, "prop3");
    auto b = make_node(2, {1, 3}, "prop3");
    auto c = make_node(3, {1, 2}, "prop3");
    std::vector<NodeBox*> cluster{a.get(), b.get(), c.get()};
    for (NodeBox* box : cluster) {
        box->node->tick(at(0));
    }
    a->node->tick(at(2000));
    route_messages(*a->node, cluster, at(2000));  // 1 se elige líder, término 1.
    ASSERT_TRUE(a->node->is_leader());
    route_messages(*a->node, cluster, at(2000));  // reparte los heartbeats iniciales.

    const auto idx = a->node->propose(make_batch(1));
    ASSERT_TRUE(idx.has_value());
    EXPECT_EQ(*idx, 1);
    route_messages(*a->node, cluster, at(2010));  // replica la entrada y recoge los acks.

    EXPECT_EQ(a->node->commit_index(), 1);  // confirmada por mayoría del término actual.
    EXPECT_EQ(b->rlog->last_index(), 1);
    EXPECT_EQ(c->rlog->last_index(), 1);
}

TEST(RaftNode, Replicacion_SeguidorAtrasado_RetrocedeYAlcanza) {
    auto a = make_node(1, {2}, "catchup");  // 2 nodos: mayoría = 2.
    auto b = make_node(2, {1}, "catchup");
    std::vector<NodeBox*> cluster{a.get(), b.get()};
    // El futuro líder arranca con 3 entradas (término 1) que el seguidor no tiene.
    const std::vector<nexus::RaftLogEntry> seed{data_entry(1, 1, 1), data_entry(1, 2, 1),
                                                data_entry(1, 3, 1)};
    ASSERT_TRUE(a->rlog->append(seed).has_value());

    for (NodeBox* box : cluster) {
        box->node->tick(at(0));
    }
    a->node->tick(at(2000));
    route_messages(*a->node, cluster, at(2000));  // 1 se elige líder, término 2.
    ASSERT_TRUE(a->node->is_leader());

    // Propone una entrada del término actual (índice 4): necesaria para poder confirmar (§5.4).
    const auto idx = a->node->propose(make_batch(1));
    ASSERT_TRUE(idx.has_value());
    EXPECT_EQ(*idx, 4);

    // Varias rondas: el seguidor rechaza por hueco, el líder retrocede next_index y reenvía.
    for (int round = 0; round < 5; ++round) {
        route_messages(*a->node, cluster, at(2010));
    }
    EXPECT_EQ(b->rlog->last_index(), 4);    // alcanzó toda la cola.
    EXPECT_EQ(a->node->commit_index(), 4);  // confirmada al replicar la entrada del término 2.
}

}  // namespace
