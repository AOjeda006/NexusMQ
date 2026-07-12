// Pruebas de PartitionTxnIndex: LSO por partición y filtrado read_committed de abortados.
#include "broker/partition_txn_index.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <vector>

#include "common/control_record.hpp"
#include "common/record.hpp"
#include "common/types.hpp"

namespace {

using nexus::AbortedTxn;
using nexus::ControlRecordType;
using nexus::PartitionTxnIndex;
using nexus::RecordBatch;

// Batch de datos: transaccional o no, con productor y offset base dados.
RecordBatch data_batch(nexus::ProducerId pid, nexus::Offset base, bool transactional) {
    nexus::RecordBatchHeader h;
    h.base_offset = base;
    h.producer_id = pid;
    h.record_count = 1;
    h.attrs = transactional ? nexus::attrs_with_transactional(0, true) : 0;
    return RecordBatch{h, std::vector<std::byte>{std::byte{1}}};
}

// Batch de control (marcador) de un productor.
RecordBatch marker_batch(nexus::ProducerId pid, ControlRecordType decision) {
    return nexus::build_control_batch({.type = decision, .coordinator_epoch = 0, .version = 0}, pid,
                                      /*producer_epoch=*/0);
}

// --- LSO ------------------------------------------------------------------

TEST(PartitionTxnIndex, SinTransaccionAbierta_LsoEsHighWatermark) {
    PartitionTxnIndex index;
    EXPECT_EQ(index.last_stable_offset(100), 100);
    EXPECT_FALSE(index.has_open());
}

TEST(PartitionTxnIndex, TransaccionAbierta_LsoSeDetieneEnSuInicio) {
    PartitionTxnIndex index;
    index.on_data(/*pid=*/7, /*base_offset=*/40);
    index.on_data(/*pid=*/7, /*base_offset=*/45);  // mismo productor: no mueve el inicio
    EXPECT_EQ(index.last_stable_offset(100), 40);
    EXPECT_TRUE(index.has_open());
}

TEST(PartitionTxnIndex, Commit_LiberaElLso) {
    PartitionTxnIndex index;
    index.on_data(7, 40);
    index.on_marker(7, ControlRecordType::Commit, /*marker_offset=*/50);
    EXPECT_EQ(index.last_stable_offset(100), 100);
    EXPECT_EQ(index.aborted_count(), 0U);
}

TEST(PartitionTxnIndex, Abort_LiberaElLsoYRegistraRango) {
    PartitionTxnIndex index;
    index.on_data(7, 40);
    index.on_marker(7, ControlRecordType::Abort, /*marker_offset=*/50);
    EXPECT_EQ(index.last_stable_offset(100), 100);
    ASSERT_EQ(index.aborted_count(), 1U);
    const std::vector<AbortedTxn> aborted = index.aborted_transactions(0);
    ASSERT_EQ(aborted.size(), 1U);
    EXPECT_EQ(aborted[0].producer_id, 7);
    EXPECT_EQ(aborted[0].first_offset, 40);
}

TEST(PartitionTxnIndex, VariasAbiertas_LsoEsElMinimo) {
    PartitionTxnIndex index;
    index.on_data(1, 60);
    index.on_data(2, 30);
    index.on_data(3, 90);
    EXPECT_EQ(index.last_stable_offset(200), 30);
    index.on_marker(2, ControlRecordType::Commit, 100);
    EXPECT_EQ(index.last_stable_offset(200), 60);
}

TEST(PartitionTxnIndex, AbortedTransactions_FiltraPorFetchOffset) {
    PartitionTxnIndex index;
    index.on_data(1, 10);
    index.on_marker(1, ControlRecordType::Abort, /*marker=*/20);  // rango [10,20)
    index.on_data(2, 30);
    index.on_marker(2, ControlRecordType::Abort, /*marker=*/40);  // rango [30,40)

    // Fetch desde 25: solo la segunda (su marcador 40 > 25); la primera (marcador 20) ya pasó.
    const std::vector<AbortedTxn> from25 = index.aborted_transactions(25);
    ASSERT_EQ(from25.size(), 1U);
    EXPECT_EQ(from25[0].producer_id, 2);
    // Fetch desde 0: ambas, ordenadas por first_offset.
    const std::vector<AbortedTxn> from0 = index.aborted_transactions(0);
    ASSERT_EQ(from0.size(), 2U);
    EXPECT_EQ(from0[0].first_offset, 10);
    EXPECT_EQ(from0[1].first_offset, 30);
}

TEST(PartitionTxnIndex, EvictBelow_DescartaAbortadasAntiguas) {
    PartitionTxnIndex index;
    index.on_data(1, 10);
    index.on_marker(1, ControlRecordType::Abort, 20);
    index.on_data(2, 100);
    index.on_marker(2, ControlRecordType::Abort, 110);
    index.evict_below(/*log_start=*/50);  // la primera (marcador 20) desaparece
    EXPECT_EQ(index.aborted_count(), 1U);
    EXPECT_EQ(index.aborted_transactions(0).at(0).producer_id, 2);
}

// --- Filtrado read_committed ---------------------------------------------

TEST(FilterCommitted, ExcluyeBatchesDeControl) {
    std::vector<RecordBatch> batches;
    batches.push_back(data_batch(1, 0, /*transactional=*/false));
    batches.push_back(marker_batch(1, ControlRecordType::Commit));
    const std::vector<RecordBatch> out = nexus::filter_committed(batches, {});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_FALSE(nexus::is_control(out[0].header().attrs));
}

TEST(FilterCommitted, NoTransaccionales_PasanSiempre) {
    std::vector<RecordBatch> batches;
    batches.push_back(data_batch(-1, 0, false));
    batches.push_back(data_batch(-1, 1, false));
    const std::vector<RecordBatch> out = nexus::filter_committed(batches, {});
    EXPECT_EQ(out.size(), 2U);
}

TEST(FilterCommitted, TransaccionAbortada_SeFiltraHastaElMarcador) {
    // Productor 5 escribe data transaccional en 10 y 11, luego su ABORT; después un batch
    // commiteado.
    std::vector<RecordBatch> batches;
    batches.push_back(data_batch(5, 10, /*transactional=*/true));
    batches.push_back(data_batch(5, 11, true));
    batches.push_back(marker_batch(5, ControlRecordType::Abort));
    batches.push_back(data_batch(5, 13, false));  // no transaccional posterior: visible

    const std::vector<AbortedTxn> aborted{{.producer_id = 5, .first_offset = 10}};
    const std::vector<RecordBatch> out = nexus::filter_committed(batches, aborted);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].header().base_offset, 13);
}

TEST(FilterCommitted, TransaccionCommiteada_EsVisible) {
    std::vector<RecordBatch> batches;
    batches.push_back(data_batch(5, 10, true));
    batches.push_back(marker_batch(5, ControlRecordType::Commit));
    // Sin abortadas: la data transaccional committeada pasa (se excluye solo el marcador).
    const std::vector<RecordBatch> out = nexus::filter_committed(batches, {});
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].header().base_offset, 10);
}

TEST(FilterCommitted, ProductoresInterleaved_SoloElAbortadoSeFiltra) {
    // pid 1 abortado [10,30); pid 2 committeado.
    std::vector<RecordBatch> batches;
    batches.push_back(data_batch(1, 10, true));  // abortado
    batches.push_back(data_batch(2, 20, true));  // committeado
    batches.push_back(marker_batch(1, ControlRecordType::Abort));
    batches.push_back(marker_batch(2, ControlRecordType::Commit));
    batches.push_back(data_batch(1, 40, true));  // nueva txn de pid 1, no abortada

    const std::vector<AbortedTxn> aborted{{.producer_id = 1, .first_offset = 10}};
    const std::vector<RecordBatch> out = nexus::filter_committed(batches, aborted);
    // Visibles: data de pid 2 (offset 20) y la nueva data de pid 1 (offset 40).
    ASSERT_EQ(out.size(), 2U);
    EXPECT_EQ(out[0].header().base_offset, 20);
    EXPECT_EQ(out[1].header().base_offset, 40);
}

TEST(FilterCommitted, DosAbortadasDelMismoProductor_AmbasSeFiltran) {
    std::vector<RecordBatch> batches;
    batches.push_back(data_batch(1, 10, true));  // txn A (abortada)
    batches.push_back(marker_batch(1, ControlRecordType::Abort));
    batches.push_back(data_batch(1, 30, true));  // txn B (abortada)
    batches.push_back(marker_batch(1, ControlRecordType::Abort));
    batches.push_back(data_batch(1, 50, false));  // no transaccional: visible

    const std::vector<AbortedTxn> aborted{{.producer_id = 1, .first_offset = 10},
                                          {.producer_id = 1, .first_offset = 30}};
    const std::vector<RecordBatch> out = nexus::filter_committed(batches, aborted);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].header().base_offset, 50);
}

}  // namespace
