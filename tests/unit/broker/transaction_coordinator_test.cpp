// Pruebas de TransactionCoordinator: FSM del 2PC recuperable (begin/add/commit/abort/failover).
#include "broker/transaction_coordinator.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <vector>

#include "common/control_record.hpp"
#include "common/error.hpp"
#include "common/types.hpp"

namespace {

using namespace std::chrono_literals;
using nexus::ControlRecordType;
using nexus::MarkerWrite;
using nexus::TopicPartition;
using nexus::TransactionCoordinator;
using nexus::TransactionState;

TopicPartition tp(std::string topic, nexus::PartitionId p) {
    return TopicPartition{.topic = std::move(topic), .partition = p};
}

// Camino feliz de commit: begin → add → commit → escribir marcadores → complete.
TEST(TransactionCoordinator, CommitFeliz_EscribeMarcadoresYConcluye) {
    TransactionCoordinator coord{/*coordinator_epoch=*/1};
    const nexus::MonoTime t0{};

    ASSERT_TRUE(coord.begin(t0, /*pid=*/10, /*epoch=*/0).has_value());
    ASSERT_TRUE(coord.add_partitions(t0, 10, 0, {tp("orders", 0), tp("payments", 1)}).has_value());
    ASSERT_TRUE(coord.commit(t0, 10, 0).has_value());

    const auto* meta = coord.find(10);
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state, TransactionState::PrepareCommit);

    std::vector<MarkerWrite> markers = coord.take_pending_markers();
    ASSERT_EQ(markers.size(), 2U);
    for (const MarkerWrite& m : markers) {
        EXPECT_EQ(m.decision, ControlRecordType::Commit);
        EXPECT_EQ(m.producer_id, 10);
        EXPECT_EQ(m.coordinator_epoch, 1);
    }

    coord.on_marker_written(10, 0, tp("orders", 0));
    EXPECT_EQ(coord.find(10)->state, TransactionState::PrepareCommit);  // aún falta 1
    coord.on_marker_written(10, 0, tp("payments", 1));
    EXPECT_EQ(coord.find(10)->state, TransactionState::CompleteCommit);
}

TEST(TransactionCoordinator, Abort_EmiteMarcadoresDeAbort) {
    TransactionCoordinator coord{2};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 5, 0).has_value());
    ASSERT_TRUE(coord.add_partitions(t0, 5, 0, {tp("t", 0)}).has_value());
    ASSERT_TRUE(coord.abort(t0, 5, 0).has_value());

    const std::vector<MarkerWrite> markers = coord.take_pending_markers();
    ASSERT_EQ(markers.size(), 1U);
    EXPECT_EQ(markers[0].decision, ControlRecordType::Abort);
    EXPECT_EQ(coord.find(5)->state, TransactionState::PrepareAbort);
}

TEST(TransactionCoordinator, CommitSinParticipantes_ConcluyeSinMarcadores) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 7, 0).has_value());
    ASSERT_TRUE(coord.commit(t0, 7, 0).has_value());
    EXPECT_TRUE(coord.take_pending_markers().empty());
    EXPECT_EQ(coord.find(7)->state, TransactionState::CompleteCommit);
}

TEST(TransactionCoordinator, AddPartitions_Idempotente_NoDuplica) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, 0).has_value());
    ASSERT_TRUE(coord.add_partitions(t0, 1, 0, {tp("t", 0), tp("t", 1)}).has_value());
    ASSERT_TRUE(coord.add_partitions(t0, 1, 0, {tp("t", 1), tp("t", 0)}).has_value());  // repetidas
    EXPECT_EQ(coord.find(1)->partitions.size(), 2U);
}

// --- Fencing por época ----------------------------------------------------

TEST(TransactionCoordinator, EpocaInferior_EsFenced) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, /*epoch=*/5).has_value());
    const auto r = coord.add_partitions(t0, 1, /*epoch=*/4, {tp("t", 0)});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::Fenced);

    const auto b = coord.begin(t0, 1, /*epoch=*/4);
    ASSERT_FALSE(b.has_value());
    EXPECT_EQ(b.error().code(), nexus::ErrorCode::Fenced);
}

TEST(TransactionCoordinator, EpocaSuperior_ReemplazaTransaccion) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, /*epoch=*/0).has_value());
    ASSERT_TRUE(coord.add_partitions(t0, 1, 0, {tp("t", 0)}).has_value());
    // Nueva encarnación (epoch 1) reinicia la transacción, fenciando a la anterior.
    ASSERT_TRUE(coord.begin(t0, 1, /*epoch=*/1).has_value());
    EXPECT_EQ(coord.find(1)->producer_epoch, 1);
    EXPECT_TRUE(coord.find(1)->partitions.empty());
}

TEST(TransactionCoordinator, BeginSobreTransaccionEnCurso_EsInvalidArgument) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, 0).has_value());
    const auto r = coord.begin(t0, 1, 0);  // misma época, ya Ongoing
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(TransactionCoordinator, ReBeginTrasCompletar_AbreNuevaTransaccion) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, 0).has_value());
    ASSERT_TRUE(coord.commit(t0, 1, 0).has_value());  // sin participantes → CompleteCommit
    ASSERT_EQ(coord.find(1)->state, TransactionState::CompleteCommit);
    // Misma época puede reabrir tras concluir.
    ASSERT_TRUE(coord.begin(t0, 1, 0).has_value());
    EXPECT_EQ(coord.find(1)->state, TransactionState::Ongoing);
}

TEST(TransactionCoordinator, OperarSinBegin_EsNotFound) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    const auto r = coord.commit(t0, 99, 0);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::NotFound);
}

TEST(TransactionCoordinator, CommitDosVeces_SegundaEsInvalidArgument) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, 0).has_value());
    ASSERT_TRUE(coord.add_partitions(t0, 1, 0, {tp("t", 0)}).has_value());
    ASSERT_TRUE(coord.commit(t0, 1, 0).has_value());
    const auto r = coord.commit(t0, 1, 0);  // ya en PrepareCommit
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

// --- Timeout --------------------------------------------------------------

TEST(TransactionCoordinator, Tick_AbortaTransaccionExpirada) {
    TransactionCoordinator coord{3, /*txn_timeout=*/1000ms};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, 0).has_value());
    ASSERT_TRUE(coord.add_partitions(t0, 1, 0, {tp("t", 0)}).has_value());

    coord.tick(t0 + 500ms);  // dentro del timeout: sigue Ongoing
    EXPECT_EQ(coord.find(1)->state, TransactionState::Ongoing);

    coord.tick(t0 + 1500ms);  // supera el timeout: abort del servidor
    EXPECT_EQ(coord.find(1)->state, TransactionState::PrepareAbort);
    const std::vector<MarkerWrite> markers = coord.take_pending_markers();
    ASSERT_EQ(markers.size(), 1U);
    EXPECT_EQ(markers[0].decision, ControlRecordType::Abort);
}

// --- Acks obsoletos / idempotencia ---------------------------------------

TEST(TransactionCoordinator, MarkerWritten_DuplicadoNoSobreDecrementa) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, 0).has_value());
    ASSERT_TRUE(coord.add_partitions(t0, 1, 0, {tp("t", 0), tp("t", 1)}).has_value());
    ASSERT_TRUE(coord.commit(t0, 1, 0).has_value());

    coord.on_marker_written(1, 0, tp("t", 0));
    coord.on_marker_written(1, 0, tp("t", 0));                         // ack duplicado: ignorado
    EXPECT_EQ(coord.find(1)->state, TransactionState::PrepareCommit);  // aún falta t/1
    coord.on_marker_written(1, 0, tp("t", 1));
    EXPECT_EQ(coord.find(1)->state, TransactionState::CompleteCommit);
}

TEST(TransactionCoordinator, MarkerWritten_EpocaObsoleta_Ignorado) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, /*epoch=*/2).has_value());
    ASSERT_TRUE(coord.add_partitions(t0, 1, 2, {tp("t", 0)}).has_value());
    ASSERT_TRUE(coord.commit(t0, 1, 2).has_value());
    coord.on_marker_written(1, /*epoch=*/1, tp("t", 0));  // época vieja: ignorado
    EXPECT_EQ(coord.find(1)->state, TransactionState::PrepareCommit);
}

// --- Failover (2PC recuperable) ------------------------------------------

TEST(TransactionCoordinator, ResumePending_ReEmiteMarcadoresConEpocaNueva) {
    TransactionCoordinator coord{/*coordinator_epoch=*/1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, 0).has_value());
    ASSERT_TRUE(coord.add_partitions(t0, 1, 0, {tp("t", 0), tp("t", 1)}).has_value());
    ASSERT_TRUE(coord.commit(t0, 1, 0).has_value());
    // Se escribe solo uno de los dos marcadores antes del "fallo".
    coord.on_marker_written(1, 0, tp("t", 0));
    (void)coord.take_pending_markers();  // el portador anterior ya los drenó

    // Failover: nuevo coordinador con época mayor reanuda el 2PC pendiente.
    coord.set_coordinator_epoch(2);
    coord.resume_pending();
    const std::vector<MarkerWrite> markers = coord.take_pending_markers();
    ASSERT_EQ(markers.size(), 1U);  // solo el marcador que faltaba
    EXPECT_EQ(markers[0].partition, tp("t", 1));
    EXPECT_EQ(markers[0].decision, ControlRecordType::Commit);
    EXPECT_EQ(markers[0].coordinator_epoch, 2);  // sellado con la época nueva

    coord.on_marker_written(1, 0, tp("t", 1));
    EXPECT_EQ(coord.find(1)->state, TransactionState::CompleteCommit);
}

TEST(TransactionCoordinator, ResumePending_IgnoraTransaccionesConcluidas) {
    TransactionCoordinator coord{1};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coord.begin(t0, 1, 0).has_value());
    ASSERT_TRUE(coord.commit(t0, 1, 0).has_value());  // CompleteCommit (sin participantes)
    coord.set_coordinator_epoch(2);
    coord.resume_pending();
    EXPECT_TRUE(coord.take_pending_markers().empty());
}

}  // namespace
