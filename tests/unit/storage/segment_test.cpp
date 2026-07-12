#include "storage/segment.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "io/file.hpp"
#include "storage/segment_crypto.hpp"

namespace {

// KEK de prueba (64 hex = 256 bits); solo para tests, jamás una clave real.
nexus::EncryptionKey test_key() {
    auto key = nexus::EncryptionKey::from_hex(
        "0f0e0d0c0b0a09080706050403020100fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0");
    EXPECT_TRUE(key.has_value());
    return std::move(*key);
}

// Crea (y limpia con RAII) un directorio temporal único para un segmento.
class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_segment_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

// Batch con 'count' records de 'payload_len' bytes (contenido 0xAB) y el base_offset dado.
nexus::RecordBatch make_batch(nexus::Offset base_offset, std::int32_t count,
                              std::size_t payload_len) {
    nexus::RecordBatchHeader header;
    header.base_offset = base_offset;
    header.record_count = count;
    return nexus::RecordBatch{header, std::vector<std::byte>(payload_len, std::byte{0xAB})};
}

// Segmento con 4 batches de 3 records y payload 10 (encoded_size 46 c/u):
// offsets b0 0..2, b1 3..5, b2 6..8, b3 9..11. Con interval 64 el índice ancla en b2 (pos 92).
nexus::Segment filled_segment(const std::filesystem::path& dir) {
    auto seg = nexus::Segment::create(dir, /*base_offset=*/0, /*interval=*/64);
    EXPECT_TRUE(seg.has_value());
    for (const nexus::Offset base : {0, 3, 6, 9}) {
        EXPECT_TRUE(seg->append(make_batch(base, 3, 10)).has_value());
    }
    return std::move(*seg);
}

TEST(Segment, Append_VariosBatches_DevuelveUltimoOffsetYCreceTamano) {
    const TempDir dir("append");
    auto seg = nexus::Segment::create(dir.path(), /*base_offset=*/0, /*interval=*/64);
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->base_offset(), 0);
    EXPECT_EQ(seg->size_bytes(), 0U);

    const auto off0 = seg->append(make_batch(0, 3, 10));  // offsets 0..2
    const auto off1 = seg->append(make_batch(3, 2, 10));  // offsets 3..4
    const auto off2 = seg->append(make_batch(5, 5, 10));  // offsets 5..9
    ASSERT_TRUE(off0.has_value() && off1.has_value() && off2.has_value());
    EXPECT_EQ(*off0, 2);
    EXPECT_EQ(*off1, 4);
    EXPECT_EQ(*off2, 9);
    EXPECT_GT(seg->size_bytes(), 0U);
}

TEST(Segment, IsFull_SuperaUmbral_DevuelveTrue) {
    const TempDir dir("full");
    auto seg = nexus::Segment::create(dir.path(), 0, 64);
    ASSERT_TRUE(seg.has_value());
    EXPECT_FALSE(seg->is_full(1024));
    ASSERT_TRUE(seg->append(make_batch(0, 1, 200)).has_value());
    EXPECT_TRUE(seg->is_full(64));  // ya supera 64 bytes
    EXPECT_FALSE(seg->is_full(100000));
}

TEST(Segment, SealYReopen_PreservaTamanoYContenidoEnDisco) {
    const TempDir dir("reopen");
    nexus::Offset off_seen = -1;
    std::size_t bytes_seen = 0;
    {
        auto seg = nexus::Segment::create(dir.path(), 0, 64);
        ASSERT_TRUE(seg.has_value());
        const auto off = seg->append(make_batch(0, 4, 16));
        ASSERT_TRUE(off.has_value());
        off_seen = *off;
        bytes_seen = seg->size_bytes();
        ASSERT_TRUE(seg->seal().has_value());
        EXPECT_EQ(seg->state(), nexus::Segment::State::Closed);
    }
    auto reopened = nexus::Segment::open(dir.path(), 0, 64);
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->size_bytes(), bytes_seen);

    // El primer batch del .log decodifica correctamente y conserva su último offset.
    auto log = nexus::File::open((dir.path() / "00000000000000000000.log").string(),
                                 nexus::File::Mode::ReadOnly);
    ASSERT_TRUE(log.has_value());
    std::vector<std::byte> raw(bytes_seen);
    ASSERT_TRUE(log->read_at(raw, 0).has_value());
    const auto decoded = nexus::RecordBatch::decode(nexus::ByteSpan{raw});
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->last_offset(), off_seen);
}

TEST(Segment, Read_DesdeOffsetMedio_DevuelveBatchDesdeEseOffset) {
    const TempDir dir("read_medio");
    const auto seg = filled_segment(dir.path());
    const auto fr = seg.read(4, 100000);
    ASSERT_TRUE(fr.has_value());
    const auto first = nexus::RecordBatch::decode(fr->batches.as_span());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->header().base_offset, 3);  // el batch que contiene el offset 4
    EXPECT_EQ(fr->next_offset, 12);             // tras el último batch (last 11)
}

TEST(Segment, Read_RespetaMaxBytes_DevuelveAlMenosUnBatch) {
    const TempDir dir("read_max");
    const auto seg = filled_segment(dir.path());
    const auto fr = seg.read(0, 46);  // encoded_size de un batch = 36 + 10
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->batches.size(), 46U);  // exactamente un batch
    EXPECT_EQ(fr->next_offset, 3);       // tras b0 (last 2)
}

TEST(Segment, Read_OffsetMasAllaDelFinal_DevuelveVacio) {
    const TempDir dir("read_fin");
    const auto seg = filled_segment(dir.path());
    const auto fr = seg.read(100, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_TRUE(fr->batches.empty());
    EXPECT_EQ(fr->next_offset, 100);
}

TEST(Segment, Read_SeekViaIndice_LocalizaBatchCorrecto) {
    const TempDir dir("read_seek");
    const auto seg = filled_segment(dir.path());
    const auto fr = seg.read(7, 100000);  // floor(7) -> ancla {6,92}; salta b0/b1
    ASSERT_TRUE(fr.has_value());
    const auto first = nexus::RecordBatch::decode(fr->batches.as_span());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->header().base_offset, 6);
    EXPECT_EQ(fr->next_offset, 12);
}

// Devuelve la ruta del .log de un segmento de base 0.
std::string log_path(const std::filesystem::path& dir) {
    return (dir / "00000000000000000000.log").string();
}

TEST(Segment, Recover_LogIntegro_DevuelveUltimoOffsetSinTruncar) {
    const TempDir dir("recover_ok");
    auto seg = filled_segment(dir.path());  // offsets 0..11 (4 batches)
    const std::size_t before = seg.size_bytes();
    const auto last = seg.recover();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 11);
    EXPECT_EQ(seg.size_bytes(), before);  // no truncó nada
}

TEST(Segment, Recover_LogVacio_DevuelveBaseMenosUno) {
    const TempDir dir("recover_vacio");
    auto seg = nexus::Segment::create(dir.path(), /*base_offset=*/5, 64);
    ASSERT_TRUE(seg.has_value());
    const auto last = seg->recover();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 4);  // base 5 - 1
    EXPECT_EQ(seg->size_bytes(), 0U);
}

TEST(Segment, Recover_ColaTorn_TruncaYDevuelveUltimoValido) {
    const TempDir dir("recover_torn");
    std::size_t valid_bytes = 0;
    {
        auto seg = nexus::Segment::create(dir.path(), 0, 64);
        ASSERT_TRUE(seg.has_value());
        ASSERT_TRUE(seg->append(make_batch(0, 3, 10)).has_value());  // 0..2
        ASSERT_TRUE(seg->append(make_batch(3, 3, 10)).has_value());  // 3..5
        ASSERT_TRUE(seg->seal().has_value());
        valid_bytes = seg->size_bytes();  // 2 * 46 = 92
    }
    // Simula un crash a mitad de escritura: 15 bytes de basura tras lo válido.
    {
        auto log = nexus::File::open(log_path(dir.path()), nexus::File::Mode::ReadWrite);
        ASSERT_TRUE(log.has_value());
        const std::vector<std::byte> garbage(15, std::byte{0x7F});
        ASSERT_TRUE(log->write_at(garbage, valid_bytes).has_value());
        ASSERT_TRUE(log->sync().has_value());
    }
    auto seg = nexus::Segment::open(dir.path(), 0, 64);
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->size_bytes(), valid_bytes + 15);  // ve la cola torn
    const auto last = seg->recover();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 5);                        // último offset válido
    EXPECT_EQ(seg->size_bytes(), valid_bytes);  // truncó la basura
    const auto fr = seg->read(0, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->next_offset, 6);  // solo los batches válidos
}

TEST(Segment, Recover_BatchConCrcCorrupto_TruncaDesdeElBatchDanado) {
    const TempDir dir("recover_crc");
    std::size_t valid_bytes = 0;
    {
        auto seg = nexus::Segment::create(dir.path(), 0, 64);
        ASSERT_TRUE(seg.has_value());
        ASSERT_TRUE(seg->append(make_batch(0, 3, 10)).has_value());  // 0..2
        ASSERT_TRUE(seg->seal().has_value());
        valid_bytes = seg->size_bytes();  // 46
    }
    // Escribe un batch completo pero con un byte corrupto (el CRC no cuadrará).
    {
        nexus::Buffer buf;
        make_batch(3, 3, 10).encode(buf);
        std::vector<std::byte> bytes{buf.as_span().begin(), buf.as_span().end()};
        bytes.back() ^= std::byte{0xFF};
        auto log = nexus::File::open(log_path(dir.path()), nexus::File::Mode::ReadWrite);
        ASSERT_TRUE(log.has_value());
        ASSERT_TRUE(log->write_at(bytes, valid_bytes).has_value());
        ASSERT_TRUE(log->sync().has_value());
    }
    auto seg = nexus::Segment::open(dir.path(), 0, 64);
    ASSERT_TRUE(seg.has_value());
    const auto last = seg->recover();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 2);                        // solo el primer batch es válido
    EXPECT_EQ(seg->size_bytes(), valid_bytes);  // truncó el batch dañado
}

TEST(Segment, Append_EnSegmentoSellado_DevuelveInvalidArgument) {
    const TempDir dir("sellado");
    auto seg = nexus::Segment::create(dir.path(), 0, 64);
    ASSERT_TRUE(seg.has_value());
    ASSERT_TRUE(seg->seal().has_value());
    const auto off = seg->append(make_batch(0, 1, 8));
    ASSERT_FALSE(off.has_value());
    EXPECT_EQ(off.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Segment, TruncateTo_FronteraDeBatch_RecortaCola) {
    const TempDir dir("trunc_frontera");
    auto seg = filled_segment(dir.path());        // base offsets 0,3,6,9 (4 batches de 46 bytes)
    ASSERT_TRUE(seg.truncate_to(6).has_value());  // descarta b2 (6..8) y b3 (9..11)
    EXPECT_EQ(seg.size_bytes(), 2U * 46U);        // quedan b0 y b1
    EXPECT_EQ(seg.state(), nexus::Segment::State::Active);
    // Tras el corte no queda nada en el offset 6.
    const auto fr = seg.read(6, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_TRUE(fr->batches.empty());
}

TEST(Segment, TruncateTo_BaseOffset_VaciaElSegmento) {
    const TempDir dir("trunc_vacia");
    auto seg = filled_segment(dir.path());
    ASSERT_TRUE(seg.truncate_to(0).has_value());
    EXPECT_EQ(seg.size_bytes(), 0U);
}

TEST(Segment, TruncateTo_MitadDeBatch_DevuelveInvalidArgument) {
    const TempDir dir("trunc_media");
    auto seg = filled_segment(dir.path());
    const auto bad = seg.truncate_to(4);  // 4 cae dentro de b1 (3..5): no es frontera
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Segment, TruncateTo_MitadDelUltimoBatch_DevuelveInvalidArgument) {
    const TempDir dir("trunc_ultimo");
    auto seg = filled_segment(dir.path());
    const auto bad = seg.truncate_to(10);  // 10 cae dentro de b3 (9..11): no es frontera
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Segment, TruncateThenAppend_ContinuaDesdeElCorte) {
    const TempDir dir("trunc_append");
    auto seg = filled_segment(dir.path());
    ASSERT_TRUE(seg.truncate_to(6).has_value());
    const auto off = seg.append(make_batch(6, 1, 10));  // reanuda en el offset del corte
    ASSERT_TRUE(off.has_value());
    EXPECT_EQ(*off, 6);
}

// --- Segmento cifrado (ADR-0031) ---

// Segmento cifrado con 4 batches de 3 records (offsets 0..2, 3..5, 6..8, 9..11).
nexus::Segment filled_encrypted_segment(const std::filesystem::path& dir,
                                        const nexus::EncryptionKey& key) {
    auto seg = nexus::Segment::create(dir, /*base_offset=*/0, /*interval=*/64, &key);
    EXPECT_TRUE(seg.has_value());
    for (const nexus::Offset base : {0, 3, 6, 9}) {
        EXPECT_TRUE(seg->append(make_batch(base, 3, 10)).has_value());
    }
    return std::move(*seg);
}

TEST(SegmentEncrypted, RoundTrip_LeeYDescifraDesdeOffsetMedio) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir dir("enc_rt");
    const auto key = test_key();
    const auto seg = filled_encrypted_segment(dir.path(), key);
    EXPECT_TRUE(seg.is_encrypted());

    const auto fr = seg.read(4, 100000);
    ASSERT_TRUE(fr.has_value());
    const auto first = nexus::RecordBatch::decode(fr->batches.as_span());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->header().base_offset, 3);  // el batch que contiene el offset 4
    EXPECT_EQ(fr->next_offset, 12);
}

TEST(SegmentEncrypted, DatosEnDisco_NoSonPlaintext) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir dir("enc_disk");
    const auto key = test_key();
    {
        auto seg = filled_encrypted_segment(dir.path(), key);
        ASSERT_TRUE(seg.seal().has_value());
    }
    auto log = nexus::File::open(log_path(dir.path()), nexus::File::Mode::ReadOnly);
    ASSERT_TRUE(log.has_value());
    std::array<std::byte, nexus::kEncSegmentHeaderSize> head{};
    ASSERT_TRUE(log->read_at(head, 0).has_value());
    EXPECT_TRUE(nexus::is_encrypted_segment_header(head));  // cabecera de cifrado presente
    // Los bytes de datos NO decodifican como un batch en claro (son ciphertext).
    std::vector<std::byte> raw(46);
    ASSERT_TRUE(log->read_at(raw, nexus::kEncSegmentHeaderSize).has_value());
    EXPECT_FALSE(nexus::RecordBatch::decode(nexus::ByteSpan{raw}).has_value());
}

TEST(SegmentEncrypted, SealYReopenConClave_LeeDescifrado) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir dir("enc_reopen");
    const auto key = test_key();
    {
        auto seg = filled_encrypted_segment(dir.path(), key);
        ASSERT_TRUE(seg.seal().has_value());
    }
    auto reopened = nexus::Segment::open(dir.path(), 0, 64, &key);
    ASSERT_TRUE(reopened.has_value());
    EXPECT_TRUE(reopened->is_encrypted());
    const auto fr = reopened->read(0, 100000);
    ASSERT_TRUE(fr.has_value());
    const auto first = nexus::RecordBatch::decode(fr->batches.as_span());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->header().base_offset, 0);
    EXPECT_EQ(fr->next_offset, 12);
}

TEST(SegmentEncrypted, OpenSinClave_DevuelveUnsupported) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir dir("enc_nokey");
    const auto key = test_key();
    {
        auto seg = filled_encrypted_segment(dir.path(), key);
        ASSERT_TRUE(seg.seal().has_value());
    }
    const auto reopened = nexus::Segment::open(dir.path(), 0, 64, /*key=*/nullptr);
    ASSERT_FALSE(reopened.has_value());
    EXPECT_EQ(reopened.error().code(), nexus::ErrorCode::Unsupported);
}

TEST(SegmentEncrypted, ByteAlteradoEnCiphertext_LecturaDevuelveCorrupt) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir dir("enc_tamper");
    const auto key = test_key();
    {
        auto seg = filled_encrypted_segment(dir.path(), key);
        ASSERT_TRUE(seg.seal().has_value());
    }
    // Altera un byte del ciphertext del primer bloque (cabecera 32 + cabecera de bloque 46 + 5).
    {
        auto log = nexus::File::open(log_path(dir.path()), nexus::File::Mode::ReadWrite);
        ASSERT_TRUE(log.has_value());
        const std::size_t pos = nexus::kEncSegmentHeaderSize + nexus::kEncBlockHeaderSize + 5;
        std::array<std::byte, 1> one{};
        ASSERT_TRUE(log->read_at(one, pos).has_value());
        one[0] ^= std::byte{0xFF};
        ASSERT_TRUE(log->write_at(one, pos).has_value());
        ASSERT_TRUE(log->sync().has_value());
    }
    auto seg = nexus::Segment::open(dir.path(), 0, 64, &key);
    ASSERT_TRUE(seg.has_value());
    const auto fr = seg->read(0, 100000);
    ASSERT_FALSE(fr.has_value());  // fallo autenticado, no datos corruptos silenciosos
    EXPECT_EQ(fr.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(SegmentEncrypted, RecoverColaTorn_TruncaYDevuelveUltimoValido) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir dir("enc_torn");
    const auto key = test_key();
    std::size_t valid_bytes = 0;
    {
        auto seg = nexus::Segment::create(dir.path(), 0, 64, &key);
        ASSERT_TRUE(seg.has_value());
        ASSERT_TRUE(seg->append(make_batch(0, 3, 10)).has_value());  // 0..2
        ASSERT_TRUE(seg->append(make_batch(3, 3, 10)).has_value());  // 3..5
        ASSERT_TRUE(seg->seal().has_value());
        valid_bytes = seg->size_bytes();
    }
    {
        auto log = nexus::File::open(log_path(dir.path()), nexus::File::Mode::ReadWrite);
        ASSERT_TRUE(log.has_value());
        const std::vector<std::byte> garbage(20, std::byte{0x7F});
        ASSERT_TRUE(log->write_at(garbage, valid_bytes).has_value());
        ASSERT_TRUE(log->sync().has_value());
    }
    auto seg = nexus::Segment::open(dir.path(), 0, 64, &key);
    ASSERT_TRUE(seg.has_value());
    const auto last = seg->recover();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 5);                        // último offset válido
    EXPECT_EQ(seg->size_bytes(), valid_bytes);  // truncó la basura
    const auto fr = seg->read(0, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->next_offset, 6);
}

TEST(SegmentEncrypted, TruncateThenAppend_ContinuaDesdeElCorte) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir dir("enc_trunc");
    const auto key = test_key();
    auto seg = filled_encrypted_segment(dir.path(), key);
    ASSERT_TRUE(seg.truncate_to(6).has_value());  // descarta b2 (6..8) y b3 (9..11)
    const auto off = seg.append(make_batch(6, 1, 10));
    ASSERT_TRUE(off.has_value());
    EXPECT_EQ(*off, 6);
    // Lo re-escrito se lee y descifra correctamente.
    const auto fr = seg.read(6, 100000);
    ASSERT_TRUE(fr.has_value());
    const auto batch = nexus::RecordBatch::decode(fr->batches.as_span());
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->header().base_offset, 6);
    EXPECT_EQ(fr->next_offset, 7);
}

}  // namespace
