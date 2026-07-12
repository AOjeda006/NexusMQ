// Simulación determinista del 2PC de transacciones multi-partición (ADR-0033): verifica atomicidad
// (commit todo-o-nada visible, abort invisible), la contención del LSO, el failover del coordinador
// a mitad del commit, el fencing de zombis y un escenario de caos con semilla fija.
#include "transaction_sim.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "broker/transaction_coordinator.hpp"
#include "common/control_record.hpp"
#include "common/error.hpp"
#include "common/types.hpp"

namespace {

using nexus::ControlRecordType;
using nexus::ProducerIdentity;
using nexus::TopicPartition;
using nexus::sim::TxnSim;

TopicPartition tp(std::string topic) {
    return TopicPartition{.topic = std::move(topic), .partition = 0};
}

// Ejecuta una transacción completa: init, begin, produce a cada partición (payload único),
// add_partitions, commit/abort. Devuelve los (partición, payload) escritos.
struct Written {
    std::string partition;
    int payload = 0;
};

std::vector<Written> run_txn(TxnSim& sim, const std::string& txn_id,
                             const std::vector<std::string>& parts, bool commit) {
    const ProducerIdentity id = sim.coordinator().init_producer_id(sim.now(), txn_id);
    EXPECT_TRUE(sim.coordinator().begin(sim.now(), id.producer_id, id.producer_epoch).has_value());
    std::vector<Written> writes;
    std::vector<TopicPartition> participants;
    for (const std::string& p : parts) {
        const int payload = sim.next_payload();
        sim.partition(p).append_data(id.producer_id, id.producer_epoch, /*transactional=*/true,
                                     payload);
        writes.push_back(Written{.partition = p, .payload = payload});
        participants.push_back(tp(p));
    }
    EXPECT_TRUE(sim.coordinator()
                    .add_partitions(sim.now(), id.producer_id, id.producer_epoch, participants)
                    .has_value());
    const auto result = commit
                            ? sim.coordinator().commit(sim.now(), id.producer_id, id.producer_epoch)
                            : sim.coordinator().abort(sim.now(), id.producer_id, id.producer_epoch);
    EXPECT_TRUE(result.has_value());
    return writes;
}

// --- Atomicidad -----------------------------------------------------------

TEST(TransactionSim, Commit_HaceVisibleLaData) {
    TxnSim sim;
    const std::vector<Written> w = run_txn(sim, "app", {"orders"}, /*commit=*/true);
    sim.flush_markers();
    EXPECT_EQ(sim.partition("orders").read_committed(), std::vector<int>{w[0].payload});
}

TEST(TransactionSim, Abort_DejaLaDataInvisible) {
    TxnSim sim;
    run_txn(sim, "app", {"orders"}, /*commit=*/false);
    sim.flush_markers();
    EXPECT_TRUE(sim.partition("orders").read_committed().empty());
}

TEST(TransactionSim, MultiParticion_CommitAtomicoEnTodas) {
    TxnSim sim;
    const std::vector<Written> w = run_txn(sim, "app", {"orders", "payments"}, /*commit=*/true);
    sim.flush_markers();
    EXPECT_EQ(sim.partition("orders").read_committed(), std::vector<int>{w[0].payload});
    EXPECT_EQ(sim.partition("payments").read_committed(), std::vector<int>{w[1].payload});
}

TEST(TransactionSim, TransaccionAbierta_ElLsoRetieneLaData) {
    TxnSim sim;
    const ProducerIdentity id = sim.coordinator().init_producer_id(sim.now(), "app");
    ASSERT_TRUE(sim.coordinator().begin(sim.now(), id.producer_id, id.producer_epoch).has_value());
    const int payload = sim.next_payload();
    sim.partition("orders").append_data(id.producer_id, id.producer_epoch, true, payload);
    ASSERT_TRUE(sim.coordinator()
                    .add_partitions(sim.now(), id.producer_id, id.producer_epoch, {tp("orders")})
                    .has_value());
    // Sin commit todavía: la transacción está abierta y el LSO retiene su data.
    EXPECT_TRUE(sim.partition("orders").read_committed().empty());
    EXPECT_LT(sim.partition("orders").lso(), sim.partition("orders").high_watermark());

    ASSERT_TRUE(sim.coordinator().commit(sim.now(), id.producer_id, id.producer_epoch).has_value());
    sim.flush_markers();
    EXPECT_EQ(sim.partition("orders").read_committed(), std::vector<int>{payload});
}

// --- Failover del coordinador a mitad del 2PC -----------------------------

TEST(TransactionSim, FailoverAMitadDeCommit_SeResuelve) {
    TxnSim sim{/*coordinator_epoch=*/1};
    const std::vector<Written> w = run_txn(sim, "app", {"orders", "payments"}, /*commit=*/true);

    // Solo se entrega la mitad de los marcadores; el coordinador "cae".
    sim.flush_markers(/*deliver_fraction=*/0.5);

    // Nuevo coordinador (época 2) reanuda el 2PC pendiente y re-emite lo que falta.
    sim.failover(/*new_epoch=*/2);
    sim.flush_markers();

    // La transacción queda completa y atómicamente visible en ambas particiones.
    EXPECT_EQ(sim.partition("orders").read_committed(), std::vector<int>{w[0].payload});
    EXPECT_EQ(sim.partition("payments").read_committed(), std::vector<int>{w[1].payload});
}

// --- Fencing de zombis ----------------------------------------------------

TEST(TransactionSim, Reinit_FenciaAlZombiYLimpiaSuTransaccion) {
    TxnSim sim;
    // El zombi abre una transacción y escribe, pero no confirma.
    const ProducerIdentity zombie = sim.coordinator().init_producer_id(sim.now(), "app");
    ASSERT_TRUE(
        sim.coordinator().begin(sim.now(), zombie.producer_id, zombie.producer_epoch).has_value());
    const int zombie_payload = sim.next_payload();
    sim.partition("orders").append_data(zombie.producer_id, zombie.producer_epoch, true,
                                        zombie_payload);
    ASSERT_TRUE(
        sim.coordinator()
            .add_partitions(sim.now(), zombie.producer_id, zombie.producer_epoch, {tp("orders")})
            .has_value());

    // El productor reinicia (nueva encarnación): su transacción abierta se aborta.
    const ProducerIdentity reborn = sim.coordinator().init_producer_id(sim.now(), "app");
    EXPECT_EQ(reborn.producer_epoch, zombie.producer_epoch + 1);
    sim.flush_markers();  // entrega el marcador de abort del zombi.

    // La encarnación vieja queda expulsada.
    const auto fenced =
        sim.coordinator().begin(sim.now(), zombie.producer_id, zombie.producer_epoch);
    ASSERT_FALSE(fenced.has_value());
    EXPECT_EQ(fenced.error().code(), nexus::ErrorCode::Fenced);

    // La encarnación nueva confirma su propia transacción.
    ASSERT_TRUE(
        sim.coordinator().begin(sim.now(), reborn.producer_id, reborn.producer_epoch).has_value());
    const int good_payload = sim.next_payload();
    sim.partition("orders").append_data(reborn.producer_id, reborn.producer_epoch, true,
                                        good_payload);
    ASSERT_TRUE(
        sim.coordinator()
            .add_partitions(sim.now(), reborn.producer_id, reborn.producer_epoch, {tp("orders")})
            .has_value());
    ASSERT_TRUE(
        sim.coordinator().commit(sim.now(), reborn.producer_id, reborn.producer_epoch).has_value());
    sim.flush_markers();

    // Solo la data de la encarnación nueva es visible; la del zombi quedó abortada.
    EXPECT_EQ(sim.partition("orders").read_committed(), std::vector<int>{good_payload});
}

// --- Caos determinista ----------------------------------------------------

TEST(TransactionSim, Caos_TodoCommitVisibleYTodoAbortInvisible) {
    TxnSim sim{/*coordinator_epoch=*/1};
    std::mt19937_64 rng{0xC0FFEEULL};
    const std::vector<std::string> parts{"p0", "p1", "p2", "p3"};
    std::map<std::string, std::vector<int>> expected;  // payloads que deben verse por partición.
    nexus::Epoch coordinator_epoch = 1;

    for (int t = 0; t < 300; ++t) {
        // Subconjunto no vacío de particiones para esta transacción.
        std::vector<std::string> chosen;
        for (const std::string& p : parts) {
            if ((rng() & 1U) != 0U) {
                chosen.push_back(p);
            }
        }
        if (chosen.empty()) {
            chosen.push_back(parts[rng() % parts.size()]);
        }
        const bool commit = (rng() % 3) != 0;  // ~2/3 confirman.
        const std::string txn_id = "app-" + std::to_string(t % 8);
        const std::vector<Written> writes = run_txn(sim, txn_id, chosen, commit);
        if (commit) {
            for (const Written& w : writes) {
                expected[w.partition].push_back(w.payload);
            }
        }

        // Entrega caótica: a veces parcial + failover, siempre seguida de entrega completa.
        if ((rng() % 4) == 0) {
            sim.flush_markers(/*deliver_fraction=*/0.5);
            ++coordinator_epoch;
            sim.failover(coordinator_epoch);
        }
        sim.flush_markers();
    }
    sim.flush_markers();  // residuo.

    // Invariante: en cada partición se ve exactamente el conjunto de payloads confirmados.
    for (const std::string& p : parts) {
        std::vector<int> want = expected[p];
        std::ranges::sort(want);
        EXPECT_EQ(sim.partition(p).read_committed(), want) << "partición " << p;
    }
}

}  // namespace
