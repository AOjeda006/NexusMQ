// Pruebas de ProducerSession: máquina de idempotencia por (producer_id, partición) (§5.9).
#include "broker/producer_session.hpp"

#include <gtest/gtest.h>

#include <cstdint>

namespace {

using nexus::ProducerSession;
using SeqCheck = nexus::ProducerSession::SeqCheck;

ProducerSession make_session() {
    return ProducerSession{/*producer_id=*/42, /*epoch=*/0};
}

TEST(ProducerSession, RecienCreada_GuardaIdentidadYSinSecuencia) {
    const ProducerSession session{/*producer_id=*/7, /*epoch=*/3};
    EXPECT_EQ(session.producer_id(), 7);
    EXPECT_EQ(session.epoch(), 3);
    EXPECT_EQ(session.last_sequence(), ProducerSession::kNoSequence);
}

TEST(ProducerSession, Check_PrimerBatchEnCero_Acepta) {
    const ProducerSession session = make_session();
    EXPECT_EQ(session.check(/*base_seq=*/0, /*count=*/5), SeqCheck::Accept);
}

TEST(ProducerSession, Check_PrimerBatchConHueco_DevuelveGap) {
    const ProducerSession session = make_session();
    EXPECT_EQ(session.check(/*base_seq=*/3, /*count=*/1), SeqCheck::Gap);
}

TEST(ProducerSession, CheckYAdvance_SecuenciaContigua_Acepta) {
    ProducerSession session = make_session();
    ASSERT_EQ(session.check(0, 5), SeqCheck::Accept);
    session.advance(4);  // batch [0,4] aceptado → última secuencia = 4
    EXPECT_EQ(session.last_sequence(), 4);
    // El siguiente esperado es 5.
    EXPECT_EQ(session.check(5, 3), SeqCheck::Accept);
    EXPECT_EQ(session.check(6, 1), SeqCheck::Gap);
}

TEST(ProducerSession, Check_BatchYaConsumido_DevuelveDuplicate) {
    ProducerSession session = make_session();
    ASSERT_EQ(session.check(0, 5), SeqCheck::Accept);
    session.advance(4);                                   // consumidas [0,4]
    EXPECT_EQ(session.check(0, 5), SeqCheck::Duplicate);  // reintento exacto
    EXPECT_EQ(session.check(2, 2), SeqCheck::Duplicate);  // subrango ya consumido
}

TEST(ProducerSession, Check_SolapamientoParcial_DevuelveGap) {
    ProducerSession session = make_session();
    ASSERT_EQ(session.check(0, 5), SeqCheck::Accept);
    session.advance(4);  // consumidas [0,4]; esperado = 5
    // Batch [3,6]: empieza antes del esperado pero lo sobrepasa → irreconciliable.
    EXPECT_EQ(session.check(3, 4), SeqCheck::Gap);
}

}  // namespace
