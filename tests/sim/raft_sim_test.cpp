// Escenarios deterministas sobre el arnés de simulación de Raft (reloj y red virtuales): elección,
// failover por caída del líder, propuesta replicada y partición de red (con pre-vote evitando la
// disrupción de la minoría). En todos se comprueban las invariantes de seguridad del arnés.
#include "sim/raft_sim.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <vector>

#include "common/record.hpp"
#include "common/types.hpp"

namespace {

using namespace std::chrono_literals;

nexus::RecordBatch make_batch() {
    nexus::RecordBatchHeader header;
    header.base_offset = 0;
    header.record_count = 1;
    return nexus::RecordBatch{header, std::vector<std::byte>(8, std::byte{0xAB})};
}

TEST(RaftSim, Eleccion_CincoNodos_ConvergeAUnLider) {
    nexus::sim::Cluster cluster({1, 2, 3, 4, 5}, 42);
    const bool elected = cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms);
    EXPECT_TRUE(elected);
    EXPECT_EQ(cluster.leader_count(), 1U);
    EXPECT_TRUE(cluster.invariants_hold()) << cluster.violations().front();
}

TEST(RaftSim, Failover_CaeElLider_EmergeOtroEnTerminoMayor) {
    nexus::sim::Cluster cluster({1, 2, 3, 4, 5}, 7);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    const nexus::NodeId first = *cluster.leader();
    const nexus::Term term_before = cluster.node(first).current_term();

    cluster.crash(first);
    const bool refeated = cluster.run_until(
        [&] {
            const auto current = cluster.leader();
            return current.has_value() && *current != first;
        },
        5000ms);
    ASSERT_TRUE(refeated);
    const nexus::NodeId second = *cluster.leader();
    EXPECT_NE(second, first);
    EXPECT_GT(cluster.node(second).current_term(), term_before);
    EXPECT_TRUE(cluster.invariants_hold()) << cluster.violations().front();
}

TEST(RaftSim, Propuesta_SeReplicaYConfirmaEnElCluster) {
    nexus::sim::Cluster cluster({1, 2, 3}, 13);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    const nexus::NodeId leader = *cluster.leader();

    const auto index = cluster.node(leader).propose(make_batch());
    ASSERT_TRUE(index.has_value());

    const bool committed =
        cluster.run_until([&] { return cluster.node(leader).commit_index() >= *index; }, 2000ms);
    EXPECT_TRUE(committed);

    cluster.run_for(1000ms);  // deja que los seguidores reciban la entrada confirmada.
    for (const nexus::NodeId id : {1, 2, 3}) {
        EXPECT_GE(cluster.log(id).last_index(), *index);
    }
    EXPECT_TRUE(cluster.invariants_hold()) << cluster.violations().front();
}

TEST(RaftSim, Particion_MinoriaNoDisrumpe_MayoriaConservaSuLider) {
    nexus::sim::Cluster cluster({1, 2, 3, 4, 5}, 21);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    const nexus::NodeId leader = *cluster.leader();
    const nexus::Term leader_term = cluster.node(leader).current_term();

    // Aísla dos nodos que NO son el líder: quedan en minoría (2 de 5, sin quórum).
    std::vector<nexus::NodeId> minority;
    for (const nexus::NodeId id : {1, 2, 3, 4, 5}) {
        if (id != leader && minority.size() < 2) {
            minority.push_back(id);
        }
    }
    cluster.partition(minority);
    cluster.run_for(3000ms);

    EXPECT_EQ(cluster.leader(), leader);  // el líder sigue en la mayoría.
    EXPECT_EQ(cluster.leader_count(), 1U);
    for (const nexus::NodeId m : minority) {
        EXPECT_FALSE(cluster.node(m).is_leader());
        // Pre-vote (§9.6): la minoría no consigue quórum de pre-votos → no infla su término.
        EXPECT_LE(cluster.node(m).current_term(), leader_term);
    }

    cluster.heal();
    cluster.run_for(3000ms);
    EXPECT_EQ(cluster.leader_count(), 1U);
    EXPECT_TRUE(cluster.invariants_hold()) << cluster.violations().front();
}

}  // namespace
