// Pruebas de ProducerSession: idempotencia effectively-once por (producer_id, partición) (§5.9).
#include "broker/producer_session.hpp"

#include <gtest/gtest.h>

#include <cstdint>

namespace {

using nexus::ProducerSession;
using SeqCheck = nexus::ProducerSession::SeqCheck;

ProducerSession make_session(std::int16_t epoch = 0) {
    return ProducerSession{/*producer_id=*/42, epoch};
}

TEST(ProducerSession, RecienCreada_GuardaIdentidadYSinSecuencia) {
    const ProducerSession session{/*producer_id=*/7, /*epoch=*/3};
    EXPECT_EQ(session.producer_id(), 7);
    EXPECT_EQ(session.epoch(), 3);
    EXPECT_EQ(session.last_sequence(), ProducerSession::kNoSequence);
}

TEST(ProducerSession, Check_PrimerBatchEnCero_Acepta) {
    const ProducerSession session = make_session();
    EXPECT_EQ(session.check(/*epoch=*/0, /*base_seq=*/0, /*count=*/5), SeqCheck::Accept);
}

TEST(ProducerSession, Check_PrimerBatchConHueco_DevuelveGap) {
    const ProducerSession session = make_session();
    EXPECT_EQ(session.check(/*epoch=*/0, /*base_seq=*/3, /*count=*/1), SeqCheck::Gap);
}

TEST(ProducerSession, CheckYAccept_SecuenciaContigua_Acepta) {
    ProducerSession session = make_session();
    ASSERT_EQ(session.check(0, 0, 5), SeqCheck::Accept);
    session.accept(/*epoch=*/0, /*base_seq=*/0, /*count=*/5, /*base_offset=*/0);  // [0,4] aceptado
    EXPECT_EQ(session.last_sequence(), 4);
    // El siguiente esperado es 5.
    EXPECT_EQ(session.check(0, 5, 3), SeqCheck::Accept);
    EXPECT_EQ(session.check(0, 6, 1), SeqCheck::Gap);
}

TEST(ProducerSession, Check_BatchYaConsumido_DevuelveDuplicate) {
    ProducerSession session = make_session();
    ASSERT_EQ(session.check(0, 0, 5), SeqCheck::Accept);
    session.accept(0, 0, 5, 0);                              // consumidas [0,4]
    EXPECT_EQ(session.check(0, 0, 5), SeqCheck::Duplicate);  // reintento exacto
    EXPECT_EQ(session.check(0, 2, 2), SeqCheck::Duplicate);  // subrango ya consumido
}

TEST(ProducerSession, Check_SolapamientoParcial_DevuelveGap) {
    ProducerSession session = make_session();
    ASSERT_EQ(session.check(0, 0, 5), SeqCheck::Accept);
    session.accept(0, 0, 5, 0);  // consumidas [0,4]; esperado = 5
    // Batch [3,6]: empieza antes del esperado pero lo sobrepasa → irreconciliable.
    EXPECT_EQ(session.check(0, 3, 4), SeqCheck::Gap);
}

TEST(ProducerSession, Check_EpocaInferior_DevuelveFenced) {
    ProducerSession session = make_session(/*epoch=*/5);
    ASSERT_EQ(session.check(5, 0, 3), SeqCheck::Accept);
    session.accept(5, 0, 3, 0);
    // Un productor con época anterior está expulsado: no puede escribir aunque la secuencia cuadre.
    EXPECT_EQ(session.check(/*epoch=*/4, /*base_seq=*/3, /*count=*/1), SeqCheck::Fenced);
    EXPECT_EQ(session.check(/*epoch=*/0, /*base_seq=*/0, /*count=*/1), SeqCheck::Fenced);
}

TEST(ProducerSession, Check_EpocaSuperior_ReiniciaSecuencia) {
    ProducerSession session = make_session(/*epoch=*/2);
    ASSERT_EQ(session.check(2, 0, 5), SeqCheck::Accept);
    session.accept(2, 0, 5, 100);  // última secuencia = 4 en la época 2
    // Nueva encarnación (época 3): la secuencia se reinicia; el primer batch debe empezar en 0.
    EXPECT_EQ(session.check(/*epoch=*/3, /*base_seq=*/5, /*count=*/1), SeqCheck::Gap);
    EXPECT_EQ(session.check(/*epoch=*/3, /*base_seq=*/0, /*count=*/2), SeqCheck::Accept);
    session.accept(3, 0, 2, 105);
    EXPECT_EQ(session.epoch(), 3);
    EXPECT_EQ(session.last_sequence(), 1);  // [0,1] en la época nueva
}

TEST(ProducerSession, DuplicateBaseOffset_RecuerdaElUltimoBatch) {
    ProducerSession session = make_session();
    ASSERT_EQ(session.check(0, 0, 3), SeqCheck::Accept);
    session.accept(0, 0, 3, /*base_offset=*/40);  // batch [0,2] → offsets [40,42]
    EXPECT_EQ(session.duplicate_base_offset(0), 40);
    // Solo se recuerda el último batch aceptado: un base_seq desconocido devuelve -1.
    EXPECT_EQ(session.duplicate_base_offset(7), -1);
}

}  // namespace
