// Pruebas de los tipos de estado de Raft: invariantes de término/voto (persistente) y de progreso
// de réplica del líder (volátil). Tipos de valor puros, sin red ni disco.
#include "consensus/raft_state.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <vector>

#include "common/types.hpp"

namespace {

TEST(RaftPersistentState, PorDefecto_TerminoCeroSinVoto) {
    const nexus::RaftPersistentState state;
    EXPECT_EQ(state.current_term(), 0);
    EXPECT_FALSE(state.voted_for().has_value());
}

TEST(RaftRole, RaftRoleName_NombresEstables) {
    EXPECT_EQ(nexus::raft_role_name(nexus::RaftRole::Follower), "follower");
    EXPECT_EQ(nexus::raft_role_name(nexus::RaftRole::PreCandidate), "pre_candidate");
    EXPECT_EQ(nexus::raft_role_name(nexus::RaftRole::Candidate), "candidate");
    EXPECT_EQ(nexus::raft_role_name(nexus::RaftRole::Leader), "leader");
}

TEST(RaftPersistentState, RecordVote_GuardaElCandidato) {
    nexus::RaftPersistentState state;
    state.record_vote(7);
    ASSERT_TRUE(state.voted_for().has_value());
    EXPECT_EQ(*state.voted_for(), 7);
}

TEST(RaftPersistentState, AdvanceTerm_SubeTerminoYDescartaElVoto) {
    nexus::RaftPersistentState state;
    state.record_vote(3);
    state.advance_term(5);
    EXPECT_EQ(state.current_term(), 5);
    EXPECT_FALSE(state.voted_for().has_value());  // un voto por término: se resetea al avanzar.
}

TEST(RaftPersistentState, EleccionPropia_IncrementaTerminoYVotaASiMismo) {
    nexus::RaftPersistentState state;
    const nexus::NodeId self = 2;
    state.advance_term(state.current_term() + 1);
    state.record_vote(self);
    EXPECT_EQ(state.current_term(), 1);
    ASSERT_TRUE(state.voted_for().has_value());
    EXPECT_EQ(*state.voted_for(), self);
}

TEST(RaftVolatileState, PorDefecto_IndicesEnCero) {
    const nexus::RaftVolatileState state;
    EXPECT_EQ(state.commit_index(), 0);
    EXPECT_EQ(state.last_applied(), 0);
}

TEST(RaftVolatileState, ResetLeaderProgress_InicializaNextYMatchPorPeer) {
    nexus::RaftVolatileState state;
    const std::array<nexus::NodeId, 2> peers{10, 11};
    const nexus::Index last_log_index = 4;
    state.reset_leader_progress(peers, last_log_index);
    EXPECT_EQ(state.next_index(10), 5);  // last_log_index + 1
    EXPECT_EQ(state.next_index(11), 5);
    EXPECT_EQ(state.match_index(10), 0);  // nada confirmado todavía
    EXPECT_EQ(state.match_index(11), 0);
}

TEST(RaftVolatileState, SetNextYMatch_ActualizanPorPeer) {
    nexus::RaftVolatileState state;
    const std::array<nexus::NodeId, 1> peers{10};
    state.reset_leader_progress(peers, 0);
    state.set_match_index(10, 3);
    state.set_next_index(10, 4);
    EXPECT_EQ(state.match_index(10), 3);
    EXPECT_EQ(state.next_index(10), 4);
}

TEST(RaftVolatileState, ClearLeaderProgress_OlvidaElProgreso) {
    nexus::RaftVolatileState state;
    const std::array<nexus::NodeId, 1> peers{10};
    state.reset_leader_progress(peers, 2);
    state.clear_leader_progress();
    EXPECT_EQ(state.next_index(10), 0);  // peer desconocido → 0
    EXPECT_EQ(state.match_index(10), 0);
}

TEST(RaftLogEntry, Igualdad_PorValor) {
    const nexus::RaftLogEntry a{.term = 1,
                                .index = 1,
                                .type = nexus::RaftEntryType::Data,
                                .payload = {std::byte{0x01}, std::byte{0x02}}};
    nexus::RaftLogEntry b = a;
    EXPECT_EQ(a, b);
    b.term = 2;
    EXPECT_NE(a, b);
}

TEST(Snapshot, Igualdad_PorValor) {
    const nexus::Snapshot a{
        .last_included_index = 9, .last_included_term = 3, .state = {std::byte{0xAB}}};
    nexus::Snapshot b = a;
    EXPECT_EQ(a, b);
    b.last_included_index = 10;
    EXPECT_NE(a, b);
}

}  // namespace
