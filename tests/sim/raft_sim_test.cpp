// Escenarios deterministas sobre el arnés de simulación de Raft (reloj y red virtuales): elección,
// failover por caída del líder, propuesta replicada y partición de red (con pre-vote evitando la
// disrupción de la minoría). Los escenarios de **caos** (cierre de Fase 2) estresan la postura
// **CP**: una entrada confirmada sobrevive al failover (Leader Completeness §5.4); un líder aislado
// en minoría no confirma mientras la mayoría sí, y al sanar la red reconcilia su cola no confirmada
// (sin split-brain de escrituras); y reinicios rodantes con propuestas mantienen el progreso. En
// todos se comprueban las invariantes de seguridad del arnés (a lo sumo un líder por término; las
// entradas confirmadas nunca divergen).
#include "sim/raft_sim.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <optional>
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

TEST(RaftSim, Failover_EntradaConfirmadaSobreviveAlNuevoLider) {
    // Leader Completeness (§5.4): una entrada confirmada antes de la caída del líder está presente
    // en el nuevo líder y sigue confirmada (CP: lo confirmado no se pierde en un failover).
    nexus::sim::Cluster cluster({1, 2, 3, 4, 5}, 99);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    const nexus::NodeId first = *cluster.leader();

    const auto committed = cluster.node(first).propose(make_batch());
    ASSERT_TRUE(committed.has_value());
    ASSERT_TRUE(cluster.run_until([&] { return cluster.node(first).commit_index() >= *committed; },
                                  2000ms));
    cluster.run_for(1000ms);  // propaga la confirmación a los seguidores.

    cluster.crash(first);
    ASSERT_TRUE(cluster.run_until(
        [&] {
            const auto current = cluster.leader();
            return current.has_value() && *current != first;
        },
        5000ms));
    const nexus::NodeId next = *cluster.leader();

    // El nuevo líder contiene la entrada confirmada (ganó la elección por tener el log al día).
    EXPECT_GE(cluster.log(next).last_index(), *committed);

    // Y la mantiene confirmada: al confirmar una entrada de su propio término, `commit_index` ≥ la
    // entrada heredada (§5.4: el líder confirma entradas previas vía una entrada del término
    // actual).
    const auto fresh = cluster.node(next).propose(make_batch());
    ASSERT_TRUE(fresh.has_value());
    ASSERT_TRUE(
        cluster.run_until([&] { return cluster.node(next).commit_index() >= *fresh; }, 3000ms));
    EXPECT_GE(cluster.node(next).commit_index(), *committed);
    EXPECT_TRUE(cluster.invariants_hold()) << cluster.violations().front();
}

TEST(RaftSim, ParticionDelLider_AisladoNoConfirma_AlSanarReconcilia) {
    // CP / anti-split-brain: el líder aislado en minoría (1 de 5) anexa local pero no confirma; la
    // mayoría elige otro líder y confirma; al sanar, el viejo líder cede y trunca su cola no
    // confirmada, adoptando el log de la mayoría.
    nexus::sim::Cluster cluster({1, 2, 3, 4, 5}, 123);
    ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 3000ms));
    const nexus::NodeId old_leader = *cluster.leader();

    cluster.partition({old_leader});
    cluster.run_for(500ms);  // drena los RPC en vuelo previos a la partición.
    const nexus::Index stuck = cluster.node(old_leader).commit_index();

    // El líder aislado propone: anexa a su log local pero no puede confirmar (sin quórum).
    const auto stranded = cluster.node(old_leader).propose(make_batch());
    ASSERT_TRUE(stranded.has_value());
    cluster.run_for(2000ms);
    EXPECT_EQ(cluster.node(old_leader).commit_index(), stuck);

    // La mayoría (4 nodos) elige un nuevo líder y confirma una entrada propia. Mientras dura la
    // partición conviven dos «líderes» en términos distintos, así que `leader()` es ambiguo: busca
    // el líder en el lado mayoritario por nodo.
    const auto majority_leader = [&]() -> std::optional<nexus::NodeId> {
        for (const nexus::NodeId id : {1, 2, 3, 4, 5}) {
            if (id != old_leader && cluster.node(id).is_leader()) {
                return id;
            }
        }
        return std::nullopt;
    };
    ASSERT_TRUE(cluster.run_until([&] { return majority_leader().has_value(); }, 5000ms));
    const nexus::NodeId new_leader = *majority_leader();
    const auto committed = cluster.node(new_leader).propose(make_batch());
    ASSERT_TRUE(committed.has_value());
    ASSERT_TRUE(cluster.run_until(
        [&] { return cluster.node(new_leader).commit_index() >= *committed; }, 3000ms));

    // Sana la red: el viejo líder ve el término mayor, cede y reconcilia su log con la mayoría.
    cluster.heal();
    const bool reconciled = cluster.run_until(
        [&] {
            return !cluster.node(old_leader).is_leader() &&
                   cluster.node(old_leader).commit_index() >= *committed;
        },
        6000ms);
    EXPECT_TRUE(reconciled);
    EXPECT_EQ(cluster.leader_count(), 1U);
    EXPECT_TRUE(cluster.invariants_hold()) << cluster.violations().front();
}

TEST(RaftSim, Caos_ReiniciosRodantesConPropuestas_MantienenProgreso) {
    // Caos determinista: en cada ronda se confirma una propuesta, cae el líder, la mayoría restante
    // elige otro y sigue sirviendo, y el caído reingresa. El clúster nunca pierde el quórum (a lo
    // sumo un nodo caído) y al final todos convergen al último índice confirmado, sin violar
    // invariantes.
    nexus::sim::Cluster cluster({1, 2, 3, 4, 5}, 2024);
    nexus::Index last_committed = 0;

    for (int round = 0; round < 5; ++round) {
        ASSERT_TRUE(cluster.run_until([&] { return cluster.leader().has_value(); }, 4000ms))
            << "ronda " << round;
        const nexus::NodeId leader = *cluster.leader();
        const auto idx = cluster.node(leader).propose(make_batch());
        ASSERT_TRUE(idx.has_value()) << "ronda " << round;
        ASSERT_TRUE(
            cluster.run_until([&] { return cluster.node(leader).commit_index() >= *idx; }, 4000ms))
            << "ronda " << round;
        last_committed = *idx;

        cluster.crash(leader);    // cae el líder…
        cluster.run_for(2500ms);  // …la mayoría restante elige otro y sigue sirviendo.
        cluster.restart(leader);  // el nodo vuelve…
        cluster.run_for(1500ms);  // …y se pone al día.
    }

    // Tras el caos, todos los nodos convergen al último índice confirmado y las invariantes
    // aguantan.
    const bool converged = cluster.run_until(
        [&] {
            for (const nexus::NodeId id : {1, 2, 3, 4, 5}) {
                if (cluster.log(id).last_index() < last_committed) {
                    return false;
                }
            }
            return true;
        },
        6000ms);
    EXPECT_TRUE(converged);
    EXPECT_TRUE(cluster.invariants_hold()) << cluster.violations().front();
}

}  // namespace
