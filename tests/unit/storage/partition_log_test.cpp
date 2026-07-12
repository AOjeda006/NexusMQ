#include "storage/partition_log.hpp"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <format>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "io/file.hpp"
#include "storage/local_storage_tier.hpp"
#include "storage/log_config.hpp"
#include "storage/retention.hpp"
#include "storage/segment_crypto.hpp"
#include "storage/storage_tier.hpp"

namespace {

// Directorio temporal único con limpieza RAII.
class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_plog_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

// Batch de 'count' records y 'payload_len' bytes; el base_offset de entrada es indiferente
// (PartitionLog lo reasigna).
nexus::RecordBatch make_batch(std::int32_t count, std::size_t payload_len) {
    nexus::RecordBatchHeader header;
    header.base_offset = -999;  // se ignora: el log asigna el offset
    header.record_count = count;
    return nexus::RecordBatch{header, std::vector<std::byte>(payload_len, std::byte{0xCD})};
}

nexus::LogConfig small_segments() {
    nexus::LogConfig cfg;
    cfg.segment_bytes = 100;  // rota tras superar 100 bytes
    cfg.index_interval_bytes = 64;
    return cfg;
}

// Ruta del .log del segmento de offset base dado dentro de @p dir.
std::string seg_log_path(const std::filesystem::path& dir, nexus::Offset base) {
    return (dir / std::format("{:020d}.log", base)).string();
}

// Log con 3 segmentos sellados/activo (offsets 0..5, dos batches de 76 bytes por segmento).
nexus::PartitionLog three_segments(const std::filesystem::path& dir) {
    auto plog = nexus::PartitionLog::open(dir, small_segments());
    EXPECT_TRUE(plog.has_value());
    for (int i = 0; i < 6; ++i) {
        EXPECT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    EXPECT_EQ(plog->segment_count(), 3U);
    return std::move(*plog);
}

TEST(PartitionLog, Open_DirectorioVacio_CreaPrimerSegmento) {
    const TempDir dir("vacio");
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    EXPECT_EQ(plog->segment_count(), 1U);
    EXPECT_EQ(plog->log_start_offset(), 0);
    EXPECT_EQ(plog->log_end_offset(), 0);
}

TEST(PartitionLog, Append_AsignaOffsetsContiguos) {
    const TempDir dir("offsets");
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());

    const auto o0 = plog->append(make_batch(3, 10));  // base 0 -> last 2
    const auto o1 = plog->append(make_batch(2, 10));  // base 3 -> last 4
    const auto o2 = plog->append(make_batch(5, 10));  // base 5 -> last 9
    ASSERT_TRUE(o0.has_value() && o1.has_value() && o2.has_value());
    EXPECT_EQ(*o0, 2);
    EXPECT_EQ(*o1, 4);
    EXPECT_EQ(*o2, 9);
    EXPECT_EQ(plog->log_end_offset(), 10);
}

TEST(PartitionLog, Append_SuperaSegmentBytes_RotaSegmento) {
    const TempDir dir("rota");
    auto plog = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_TRUE(plog.has_value());
    EXPECT_EQ(plog->segment_count(), 1U);

    // Cada batch ocupa 36 + 40 = 76 bytes; el segundo supera los 100 -> rota antes del 3.º.
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    EXPECT_GT(plog->segment_count(), 1U);  // hubo al menos una rotación
    EXPECT_EQ(plog->log_end_offset(), 3);  // offsets contiguos pese a rotar
}

TEST(PartitionLog, Reopen_TrasRotacion_PreservaOffsetsYSegmentos) {
    const TempDir dir("reopen");
    std::size_t segments = 0;
    {
        auto plog = nexus::PartitionLog::open(dir.path(), small_segments());
        ASSERT_TRUE(plog.has_value());
        for (int i = 0; i < 4; ++i) {
            ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
        }
        segments = plog->segment_count();
        ASSERT_GT(segments, 1U);
    }
    auto reopened = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->segment_count(), segments);
    EXPECT_EQ(reopened->log_start_offset(), 0);
    EXPECT_EQ(reopened->log_end_offset(), 4);
}

TEST(PartitionLog, Read_DentroDeUnSegmento_DevuelveDesdeElOffset) {
    const TempDir dir("read_uno");
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});  // segmento grande
    ASSERT_TRUE(plog.has_value());
    ASSERT_TRUE(plog->append(make_batch(3, 10)).has_value());  // 0..2
    ASSERT_TRUE(plog->append(make_batch(2, 10)).has_value());  // 3..4
    ASSERT_TRUE(plog->append(make_batch(5, 10)).has_value());  // 5..9

    const auto fr = plog->read(3, 100000);
    ASSERT_TRUE(fr.has_value());
    const auto first = nexus::RecordBatch::decode(fr->batches.as_span());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->header().base_offset, 3);
    EXPECT_EQ(fr->next_offset, 10);
}

TEST(PartitionLog, Read_CruzaSegmentos_DevuelveBatchesDeVariosSegmentos) {
    const TempDir dir("read_cruza");
    auto plog = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_TRUE(plog.has_value());
    // 4 batches de 1 record y payload 40 (encoded_size 76): caben 2 por segmento (152 > 100).
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    ASSERT_GT(plog->segment_count(), 1U);

    const auto fr = plog->read(0, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->batches.size(), 4U * 76U);  // los 4 batches, de ambos segmentos
    EXPECT_EQ(fr->next_offset, 4);
    const auto first = nexus::RecordBatch::decode(fr->batches.as_span());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->header().base_offset, 0);
}

TEST(PartitionLog, Read_OffsetMasAllaDelFinal_DevuelveVacio) {
    const TempDir dir("read_fin");
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    ASSERT_TRUE(plog->append(make_batch(3, 10)).has_value());  // 0..2, log_end 3

    const auto fr = plog->read(3, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_TRUE(fr->batches.empty());
    EXPECT_EQ(fr->next_offset, 3);
}

TEST(PartitionLog, Read_OffsetBajoLogStart_DevuelveOutOfRange) {
    const TempDir dir("read_bajo");
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    const auto fr = plog->read(-1, 100000);
    ASSERT_FALSE(fr.has_value());
    EXPECT_EQ(fr.error().code(), nexus::ErrorCode::OutOfRange);
}

nexus::LogConfig with_policy(nexus::FsyncPolicy policy, std::size_t interval = 1) {
    nexus::LogConfig cfg;
    cfg.fsync_policy = policy;
    cfg.fsync_interval_bytes = interval;
    return cfg;
}

TEST(PartitionLog, RecoveryPoint_Commit_AvanzaEnCadaAppend) {
    const TempDir dir("rp_commit");
    auto plog = nexus::PartitionLog::open(dir.path(), with_policy(nexus::FsyncPolicy::Commit));
    ASSERT_TRUE(plog.has_value());
    EXPECT_EQ(plog->recovery_point(), 0);
    ASSERT_TRUE(plog->append(make_batch(3, 10)).has_value());  // log_end 3
    EXPECT_EQ(plog->recovery_point(), 3);
    ASSERT_TRUE(plog->append(make_batch(2, 10)).has_value());  // log_end 5
    EXPECT_EQ(plog->recovery_point(), 5);
}

TEST(PartitionLog, RecoveryPoint_None_NoAvanzaHastaSyncExplicito) {
    const TempDir dir("rp_none");
    auto plog = nexus::PartitionLog::open(dir.path(), with_policy(nexus::FsyncPolicy::None));
    ASSERT_TRUE(plog.has_value());
    ASSERT_TRUE(plog->append(make_batch(3, 10)).has_value());
    ASSERT_TRUE(plog->append(make_batch(2, 10)).has_value());
    EXPECT_EQ(plog->recovery_point(), 0);  // None no sincroniza en append
    ASSERT_TRUE(plog->sync().has_value());
    EXPECT_EQ(plog->recovery_point(), plog->log_end_offset());
}

TEST(PartitionLog, RecoveryPoint_Interval_AvanzaAlSuperarElUmbral) {
    const TempDir dir("rp_interval");
    // Umbral 100 bytes; cada batch ocupa 36 + 40 = 76.
    auto plog = nexus::PartitionLog::open(
        dir.path(), with_policy(nexus::FsyncPolicy::Interval, /*interval=*/100));
    ASSERT_TRUE(plog.has_value());
    ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());  // 76 < 100 -> no sync
    EXPECT_EQ(plog->recovery_point(), 0);
    ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());  // 152 >= 100 -> sync
    EXPECT_EQ(plog->recovery_point(), 2);
}

TEST(PartitionLog, RecoveryPoint_RotacionAvanzaAlSellar) {
    const TempDir dir("rp_rota");
    nexus::LogConfig cfg = small_segments();  // segmento de 100 bytes
    cfg.fsync_policy = nexus::FsyncPolicy::None;
    auto plog = nexus::PartitionLog::open(dir.path(), cfg);
    ASSERT_TRUE(plog.has_value());
    for (int i = 0; i < 3; ++i) {  // el 3.er append rota (sella el segmento de offsets 0,1)
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    ASSERT_GT(plog->segment_count(), 1U);
    EXPECT_EQ(plog->recovery_point(), 2);  // el sellado quedó durable hasta el límite
}

TEST(PartitionLog, Reopen_ColaTornEnActivo_TruncaSinPerderConfirmados) {
    const TempDir dir("crash_torn");
    {
        auto plog = nexus::PartitionLog::open(dir.path(), small_segments());
        ASSERT_TRUE(plog.has_value());
        for (int i = 0; i < 4; ++i) {  // offsets 0..3, segmentos en base 0 y base 2
            ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
        }
        ASSERT_GT(plog->segment_count(), 1U);
    }
    // Crash a mitad de escritura: basura tras los batches válidos del segmento activo (base 2).
    {
        auto log = nexus::File::open(seg_log_path(dir.path(), 2), nexus::File::Mode::ReadWrite);
        ASSERT_TRUE(log.has_value());
        const auto size = log->size();
        ASSERT_TRUE(size.has_value());
        const std::vector<std::byte> garbage(15, std::byte{0x5A});
        ASSERT_TRUE(log->write_at(garbage, *size).has_value());
        ASSERT_TRUE(log->sync().has_value());
    }
    auto reopened = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->log_end_offset(), 4);  // los 4 confirmados se conservan
    const auto fr = reopened->read(0, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->batches.size(), 4U * 76U);
    EXPECT_EQ(fr->next_offset, 4);
}

TEST(PartitionLog, Reopen_BatchCorruptoEnActivo_TruncaDesdeElDanado) {
    const TempDir dir("crash_crc");
    {
        auto plog = nexus::PartitionLog::open(dir.path(), small_segments());
        ASSERT_TRUE(plog.has_value());
        for (int i = 0; i < 4; ++i) {  // offsets 0..3
            ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
        }
        ASSERT_GT(plog->segment_count(), 1U);
    }
    // Corrompe el último batch del segmento activo (base 2, offset 3): voltea su último byte.
    {
        auto log = nexus::File::open(seg_log_path(dir.path(), 2), nexus::File::Mode::ReadWrite);
        ASSERT_TRUE(log.has_value());
        const auto size = log->size();
        ASSERT_TRUE(size.has_value());
        std::array<std::byte, 1> last{};
        ASSERT_TRUE(log->read_at(last, *size - 1).has_value());
        last[0] ^= std::byte{0xFF};
        ASSERT_TRUE(log->write_at(last, *size - 1).has_value());
        ASSERT_TRUE(log->sync().has_value());
    }
    auto reopened = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->log_end_offset(), 3);  // el offset 3 corrupto se trunca
    const auto fr = reopened->read(0, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->next_offset, 3);
}

TEST(PartitionLog, EnforceRetention_PorTamano_TrimAntiguosPreservaActivo) {
    const TempDir dir("ret_size");
    auto plog = three_segments(dir.path());  // total 3*152 = 456 bytes
    nexus::RetentionPolicy pol;
    pol.retention_bytes = 350;  // borra el más antiguo (456-152=304 <= 350) y para
    ASSERT_TRUE(plog.enforce_retention(pol).has_value());
    EXPECT_EQ(plog.segment_count(), 2U);
    EXPECT_EQ(plog.log_start_offset(), 2);
    EXPECT_EQ(plog.log_end_offset(), 6);

    const auto borrado = plog.read(0, 1000);
    ASSERT_FALSE(borrado.has_value());
    EXPECT_EQ(borrado.error().code(), nexus::ErrorCode::OutOfRange);
    EXPECT_TRUE(plog.read(2, 100000).has_value());
}

TEST(PartitionLog, EnforceRetention_NuncaBorraElActivo) {
    const TempDir dir("ret_active");
    auto plog = three_segments(dir.path());
    nexus::RetentionPolicy pol;
    pol.retention_bytes = 0;  // todo excede: borra todos los sellados, no el activo
    ASSERT_TRUE(plog.enforce_retention(pol).has_value());
    EXPECT_EQ(plog.segment_count(), 1U);
    EXPECT_EQ(plog.log_start_offset(), 4);
    EXPECT_EQ(plog.log_end_offset(), 6);
    EXPECT_TRUE(plog.read(4, 100000).has_value());
}

TEST(PartitionLog, EnforceRetention_PorTiempo_BorraViejosPreservaRecientes) {
    const TempDir dir("ret_time");
    auto plog = three_segments(dir.path());
    // Envejece el .log del segmento más antiguo (base 0) una hora.
    const auto old_time = std::filesystem::file_time_type::clock::now() - std::chrono::hours(1);
    std::filesystem::last_write_time(seg_log_path(dir.path(), 0), old_time);

    nexus::RetentionPolicy pol;
    pol.retention_ms = 60000;  // 1 minuto: solo el .log envejecido es elegible
    ASSERT_TRUE(plog.enforce_retention(pol).has_value());
    EXPECT_EQ(plog.segment_count(), 2U);  // borra seg0; conserva el reciente y el activo
    EXPECT_EQ(plog.log_start_offset(), 2);
}

TEST(PartitionLog, EnforceRetention_SinLimites_NoBorraNada) {
    const TempDir dir("ret_none");
    auto plog = three_segments(dir.path());
    ASSERT_TRUE(plog.enforce_retention(nexus::RetentionPolicy{}).has_value());
    EXPECT_EQ(plog.segment_count(), 3U);
    EXPECT_EQ(plog.log_start_offset(), 0);
}

TEST(PartitionLog, TruncateTo_DentroDeUnSegmento_RetrocedeLogEnd) {
    const TempDir dir("trunc_uno");
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});  // segmento grande
    ASSERT_TRUE(plog.has_value());
    ASSERT_TRUE(plog->append(make_batch(3, 10)).has_value());  // 0..2
    ASSERT_TRUE(plog->append(make_batch(2, 10)).has_value());  // 3..4
    ASSERT_TRUE(plog->append(make_batch(5, 10)).has_value());  // 5..9
    ASSERT_EQ(plog->log_end_offset(), 10);

    ASSERT_TRUE(plog->truncate_to(5).has_value());  // descarta el tercer batch (5..9)
    EXPECT_EQ(plog->log_end_offset(), 5);
    const auto fr = plog->read(0, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->next_offset, 5);  // solo quedan los dos primeros batches
}

TEST(PartitionLog, TruncateThenAppend_OffsetsContinuanDesdeElCorte) {
    const TempDir dir("trunc_append");
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    ASSERT_TRUE(plog->append(make_batch(3, 10)).has_value());  // 0..2
    ASSERT_TRUE(plog->append(make_batch(2, 10)).has_value());  // 3..4
    ASSERT_TRUE(plog->truncate_to(3).has_value());             // descarta el segundo batch (3..4)
    EXPECT_EQ(plog->log_end_offset(), 3);
    const auto last = plog->append(make_batch(4, 10));  // reanuda en el offset 3
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 6);  // base 3 -> last 6
    EXPECT_EQ(plog->log_end_offset(), 7);
}

TEST(PartitionLog, TruncateTo_CruzaSegmentos_BorraSegmentosPosteriores) {
    const TempDir dir("trunc_cruza");
    auto plog = three_segments(dir.path());  // segmentos base 0,2,4; offsets 0..5
    ASSERT_EQ(plog.segment_count(), 3U);

    ASSERT_TRUE(plog.truncate_to(2).has_value());  // vacía el seg base 2 y borra el seg base 4
    EXPECT_EQ(plog.segment_count(), 2U);
    EXPECT_EQ(plog.log_end_offset(), 2);
    EXPECT_EQ(plog.log_start_offset(), 0);
    EXPECT_FALSE(std::filesystem::exists(seg_log_path(dir.path(), 4)));  // fichero borrado
    const auto fr = plog.read(0, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->next_offset, 2);  // solo offsets 0 y 1
}

TEST(PartitionLog, TruncateTo_LogEndOffset_EsNoOp) {
    const TempDir dir("trunc_noop");
    auto plog = three_segments(dir.path());
    const auto count_before = plog.segment_count();
    ASSERT_TRUE(plog.truncate_to(plog.log_end_offset()).has_value());
    EXPECT_EQ(plog.segment_count(), count_before);
    EXPECT_EQ(plog.log_end_offset(), 6);
}

TEST(PartitionLog, TruncateTo_MitadDeBatch_DevuelveInvalidArgument) {
    const TempDir dir("trunc_media");
    auto plog = nexus::PartitionLog::open(dir.path(), nexus::LogConfig{});
    ASSERT_TRUE(plog.has_value());
    ASSERT_TRUE(plog->append(make_batch(3, 10)).has_value());  // 0..2
    const auto bad = plog->truncate_to(1);                     // 1 cae dentro del batch (0..2)
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(PartitionLog, TruncateTo_PorEncimaDeLogEnd_DevuelveOutOfRange) {
    const TempDir dir("trunc_over");
    auto plog = three_segments(dir.path());
    const auto bad = plog.truncate_to(99);
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error().code(), nexus::ErrorCode::OutOfRange);
}

TEST(PartitionLog, TruncatePrefixTo_BorraSegmentosEnterosBajoOffset) {
    const TempDir dir("tprefix_basico");
    auto plog = three_segments(dir.path());  // segmentos base 0,2,4; offsets 0..5
    ASSERT_EQ(plog.segment_count(), 3U);

    ASSERT_TRUE(plog.truncate_prefix_to(2).has_value());  // el seg base 0 queda entero bajo 2
    EXPECT_EQ(plog.segment_count(), 2U);
    EXPECT_EQ(plog.log_start_offset(), 2);
    EXPECT_EQ(plog.log_end_offset(), 6);
    EXPECT_FALSE(std::filesystem::exists(seg_log_path(dir.path(), 0)));  // fichero borrado

    const auto borrado = plog.read(0, 1000);
    ASSERT_FALSE(borrado.has_value());
    EXPECT_EQ(borrado.error().code(), nexus::ErrorCode::OutOfRange);
    EXPECT_TRUE(plog.read(2, 100000).has_value());
}

TEST(PartitionLog, TruncatePrefixTo_NoExactoAlByte_ConservaSegmentoQueContieneOffset) {
    const TempDir dir("tprefix_inexacto");
    auto plog = three_segments(dir.path());  // base 0,2,4

    // offset 3 cae dentro del segmento base 2: solo el seg base 0 es enteramente anterior.
    ASSERT_TRUE(plog.truncate_prefix_to(3).has_value());
    EXPECT_EQ(plog.segment_count(), 2U);
    EXPECT_EQ(plog.log_start_offset(), 2);  // best-effort: por segmentos enteros, no al byte
}

TEST(PartitionLog, TruncatePrefixTo_NuncaBorraElActivo) {
    const TempDir dir("tprefix_activo");
    auto plog = three_segments(dir.path());

    ASSERT_TRUE(plog.truncate_prefix_to(plog.log_end_offset()).has_value());
    EXPECT_EQ(plog.segment_count(), 1U);  // conserva el activo (base 4)
    EXPECT_EQ(plog.log_start_offset(), 4);
    EXPECT_EQ(plog.log_end_offset(), 6);
    EXPECT_TRUE(plog.read(4, 100000).has_value());
}

TEST(PartitionLog, TruncatePrefixTo_PorDebajoDeLogStart_EsNoOp) {
    const TempDir dir("tprefix_noop");
    auto plog = three_segments(dir.path());
    ASSERT_TRUE(plog.truncate_prefix_to(2).has_value());  // log_start -> 2
    ASSERT_TRUE(plog.truncate_prefix_to(1).has_value());  // 1 <= log_start: no-op
    EXPECT_EQ(plog.segment_count(), 2U);
    EXPECT_EQ(plog.log_start_offset(), 2);
}

TEST(PartitionLog, TruncatePrefixTo_PorEncimaDeLogEnd_DevuelveOutOfRange) {
    const TempDir dir("tprefix_over");
    auto plog = three_segments(dir.path());
    const auto bad = plog.truncate_prefix_to(99);
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error().code(), nexus::ErrorCode::OutOfRange);
}

TEST(PartitionLog, TruncatePrefixTo_ReabreYPreservaElCorte) {
    const TempDir dir("tprefix_reopen");
    {
        auto plog = three_segments(dir.path());
        ASSERT_TRUE(plog.truncate_prefix_to(2).has_value());  // borra el seg base 0
        EXPECT_EQ(plog.log_start_offset(), 2);
    }
    auto reopened = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->log_start_offset(), 2);  // el segmento borrado no reaparece
    EXPECT_EQ(reopened->log_end_offset(), 6);
}

TEST(PartitionLog, ResetTo_VaciaYReabreEnLaBase) {
    const TempDir dir("reset_base");
    auto plog = three_segments(dir.path());  // offsets 0..5 en 3 segmentos
    ASSERT_TRUE(plog.reset_to(10).has_value());
    EXPECT_EQ(plog.segment_count(), 1U);
    EXPECT_EQ(plog.log_start_offset(), 10);
    EXPECT_EQ(plog.log_end_offset(), 10);
    EXPECT_EQ(plog.recovery_point(), 10);
    EXPECT_FALSE(std::filesystem::exists(seg_log_path(dir.path(), 0)));  // ficheros viejos borrados
    EXPECT_FALSE(std::filesystem::exists(seg_log_path(dir.path(), 4)));

    const auto fr = plog.read(10, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_TRUE(fr->batches.empty());
    EXPECT_EQ(fr->next_offset, 10);
}

TEST(PartitionLog, ResetTo_LuegoAppendAsignaDesdeLaBase) {
    const TempDir dir("reset_append");
    auto plog = three_segments(dir.path());
    ASSERT_TRUE(plog.reset_to(10).has_value());
    const auto last = plog.append(make_batch(3, 10));  // base 10 -> last 12
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 12);
    EXPECT_EQ(plog.log_end_offset(), 13);
    const auto borrado = plog.read(0, 1000);
    ASSERT_FALSE(borrado.has_value());
    EXPECT_EQ(borrado.error().code(), nexus::ErrorCode::OutOfRange);
}

TEST(PartitionLog, ResetTo_ReabreYConservaLaBase) {
    const TempDir dir("reset_reopen");
    {
        auto plog = three_segments(dir.path());
        ASSERT_TRUE(plog.reset_to(10).has_value());
        ASSERT_TRUE(plog.append(make_batch(2, 10)).has_value());  // base 10 -> last 11
        ASSERT_TRUE(plog.sync().has_value());
    }
    auto reopened = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->log_start_offset(), 10);
    EXPECT_EQ(reopened->log_end_offset(), 12);
}

TEST(PartitionLog, TruncateTo_ReabreYPreservaElCorte) {
    const TempDir dir("trunc_reopen");
    {
        auto plog = three_segments(dir.path());
        ASSERT_TRUE(plog.truncate_to(3).has_value());  // deja offsets 0..2
        EXPECT_EQ(plog.log_end_offset(), 3);
    }
    auto reopened = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->log_end_offset(), 3);
}

// --- Cifrado en reposo del log (ADR-0031) ---

std::shared_ptr<const nexus::EncryptionKey> plog_test_key() {
    auto key = nexus::EncryptionKey::from_hex(
        "1112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30");
    EXPECT_TRUE(key.has_value());
    return std::make_shared<const nexus::EncryptionKey>(std::move(*key));
}

nexus::LogConfig encrypted_small_segments(std::shared_ptr<const nexus::EncryptionKey> key) {
    nexus::LogConfig cfg = small_segments();
    cfg.encryption_key = std::move(key);
    return cfg;
}

// Lee todo el log offset a offset y verifica que cada batch decodifica con base_offset esperado.
void expect_readable_and_decodes(nexus::PartitionLog& plog) {
    nexus::Offset cursor = plog.log_start_offset();
    while (cursor < plog.log_end_offset()) {
        const auto fr = plog.read(cursor, 100000);
        ASSERT_TRUE(fr.has_value());
        const auto batch = nexus::RecordBatch::decode(fr->batches.as_span());
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(batch->header().base_offset, cursor);
        cursor = batch->last_offset() + 1;
    }
    EXPECT_EQ(cursor, plog.log_end_offset());
}

TEST(PartitionLogEncrypted, RotaLeeYReabreDescifrado) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir dir("enc_roundtrip");
    const auto key = plog_test_key();
    nexus::Offset end = 0;
    {
        auto plog = nexus::PartitionLog::open(dir.path(), encrypted_small_segments(key));
        ASSERT_TRUE(plog.has_value());
        for (int i = 0; i < 6; ++i) {
            ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
        }
        EXPECT_GT(plog->segment_count(), 1U);  // rotó (segmentos cifrados)
        end = plog->log_end_offset();
        expect_readable_and_decodes(*plog);
    }
    // Durabilidad: reabrir con la clave recupera y descifra.
    auto reopened = nexus::PartitionLog::open(dir.path(), encrypted_small_segments(key));
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->log_end_offset(), end);
    expect_readable_and_decodes(*reopened);
}

TEST(PartitionLogEncrypted, DatosEnDisco_TienenCabeceraDeCifrado) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir dir("enc_magic");
    const auto key = plog_test_key();
    {
        auto plog = nexus::PartitionLog::open(dir.path(), encrypted_small_segments(key));
        ASSERT_TRUE(plog.has_value());
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    auto log = nexus::File::open(seg_log_path(dir.path(), 0), nexus::File::Mode::ReadOnly);
    ASSERT_TRUE(log.has_value());
    std::array<std::byte, nexus::kEncSegmentHeaderSize> head{};
    ASSERT_TRUE(log->read_at(head, 0).has_value());
    EXPECT_TRUE(nexus::is_encrypted_segment_header(head));
}

TEST(PartitionLogEncrypted, ReabrirSinClave_DevuelveUnsupported) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir dir("enc_nokey");
    const auto key = plog_test_key();
    {
        auto plog = nexus::PartitionLog::open(dir.path(), encrypted_small_segments(key));
        ASSERT_TRUE(plog.has_value());
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    // Sin clave: un log cifrado no se puede abrir (degradación segura, no en claro accidental).
    const auto reopened = nexus::PartitionLog::open(dir.path(), small_segments());
    ASSERT_FALSE(reopened.has_value());
    EXPECT_EQ(reopened.error().code(), nexus::ErrorCode::Unsupported);
}

TEST(PartitionLogEncrypted, SinClave_EscribeEnClaro_ComoHoy) {
    const TempDir dir("plain");
    {
        auto plog = nexus::PartitionLog::open(dir.path(), small_segments());
        ASSERT_TRUE(plog.has_value());
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    auto log = nexus::File::open(seg_log_path(dir.path(), 0), nexus::File::Mode::ReadOnly);
    ASSERT_TRUE(log.has_value());
    std::array<std::byte, nexus::kEncSegmentHeaderSize> head{};
    ASSERT_TRUE(log->read_at(head, 0).has_value());
    EXPECT_FALSE(nexus::is_encrypted_segment_header(head));  // sin cabecera de cifrado: en claro
}

// --- Almacenamiento por niveles (tiered storage, ADR-0032) ---

// Config de segmentos pequeños con un tier de destino y la identidad de la partición.
nexus::LogConfig tiered_small_segments(nexus::StorageTier& tier, const char* topic = "t",
                                       std::int32_t partition = 0) {
    nexus::LogConfig cfg = small_segments();
    cfg.tier = &tier;
    cfg.tier_topic = topic;
    cfg.tier_partition = partition;
    return cfg;
}

TEST(PartitionLogTiered, OffloadSealed_DescargaLosSelladosYReclamaLocal) {
    const TempDir data("tier_offload_data");
    const TempDir obj("tier_offload_obj");
    nexus::LocalStorageTier tier(obj.path());

    auto plog = nexus::PartitionLog::open(data.path(), tiered_small_segments(tier));
    ASSERT_TRUE(plog.has_value());
    for (int i = 0; i < 6; ++i) {
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    ASSERT_EQ(plog->segment_count(), 3U);  // 2 sellados + activo

    const auto offloaded = plog->offload_sealed_to_tier();
    ASSERT_TRUE(offloaded.has_value());
    EXPECT_EQ(*offloaded, 2U);                    // los dos sellados
    EXPECT_EQ(plog->segment_count(), 1U);         // solo queda el activo local
    EXPECT_EQ(plog->tiered_segment_count(), 2U);  // dos en el tier (prefijo frío)

    // Reclamación: los `.log` locales de los sellados ya no están; el activo (base 4) sí.
    EXPECT_FALSE(std::filesystem::exists(seg_log_path(data.path(), 0)));
    EXPECT_FALSE(std::filesystem::exists(seg_log_path(data.path(), 2)));
    EXPECT_TRUE(std::filesystem::exists(seg_log_path(data.path(), 4)));

    // El rango del log no cambia: los datos siguen presentes (fríos + calientes).
    EXPECT_EQ(plog->log_start_offset(), 0);
    EXPECT_EQ(plog->log_end_offset(), 6);
    expect_readable_and_decodes(*plog);  // lectura transparente (rehidrata los fríos)
}

TEST(PartitionLogTiered, OffloadSealed_SinTier_EsNoOp) {
    const TempDir data("tier_noop");
    auto plog = three_segments(data.path());
    const auto offloaded = plog.offload_sealed_to_tier();
    ASSERT_TRUE(offloaded.has_value());
    EXPECT_EQ(*offloaded, 0U);
    EXPECT_EQ(plog.segment_count(), 3U);
    EXPECT_EQ(plog.tiered_segment_count(), 0U);
}

TEST(PartitionLogTiered, OffloadSealed_SegundaLlamada_EsIdempotente) {
    const TempDir data("tier_idem_data");
    const TempDir obj("tier_idem_obj");
    nexus::LocalStorageTier tier(obj.path());

    auto plog = nexus::PartitionLog::open(data.path(), tiered_small_segments(tier));
    ASSERT_TRUE(plog.has_value());
    for (int i = 0; i < 6; ++i) {
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    ASSERT_TRUE(plog->offload_sealed_to_tier().has_value());
    const auto again = plog->offload_sealed_to_tier();  // ya no hay sellados locales.
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(*again, 0U);
    EXPECT_EQ(plog->tiered_segment_count(), 2U);
    expect_readable_and_decodes(*plog);
}

TEST(PartitionLogTiered, Read_TrasOffload_RehidrataYDevuelveTodo) {
    const TempDir data("tier_read_data");
    const TempDir obj("tier_read_obj");
    nexus::LocalStorageTier tier(obj.path());

    auto plog = nexus::PartitionLog::open(data.path(), tiered_small_segments(tier));
    ASSERT_TRUE(plog.has_value());
    for (int i = 0; i < 6; ++i) {
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    ASSERT_TRUE(plog->offload_sealed_to_tier().has_value());

    // Una sola lectura amplia cruza el prefijo frío y el suffix caliente: los 6 batches.
    const auto fr = plog->read(0, 100000);
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->batches.size(), 6U * 76U);
    EXPECT_EQ(fr->next_offset, 6);
}

TEST(PartitionLogTiered, Reopen_TrasOffload_ReconstruyeElPrefijoFrioDesdeElTier) {
    const TempDir data("tier_reopen_data");
    const TempDir obj("tier_reopen_obj");
    nexus::LocalStorageTier tier(obj.path());

    nexus::Offset start = 0;
    nexus::Offset end = 0;
    {
        auto plog = nexus::PartitionLog::open(data.path(), tiered_small_segments(tier));
        ASSERT_TRUE(plog.has_value());
        for (int i = 0; i < 6; ++i) {
            ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
        }
        ASSERT_TRUE(plog->offload_sealed_to_tier().has_value());
        start = plog->log_start_offset();
        end = plog->log_end_offset();
    }
    // Reabrir con el mismo tier reconstruye el prefijo frío (el tier es la autoridad).
    auto reopened = nexus::PartitionLog::open(data.path(), tiered_small_segments(tier));
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->log_start_offset(), start);
    EXPECT_EQ(reopened->log_end_offset(), end);
    EXPECT_EQ(reopened->tiered_segment_count(), 2U);
    expect_readable_and_decodes(*reopened);  // sigue leyéndose todo, frío incluido
}

TEST(PartitionLogTiered, TruncatePrefix_TrasOffload_BorraElPrefijoFrioDelTier) {
    const TempDir data("tier_trunc_data");
    const TempDir obj("tier_trunc_obj");
    nexus::LocalStorageTier tier(obj.path());

    auto plog = nexus::PartitionLog::open(data.path(), tiered_small_segments(tier));
    ASSERT_TRUE(plog.has_value());
    for (int i = 0; i < 6; ++i) {
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    ASSERT_TRUE(plog->offload_sealed_to_tier().has_value());
    ASSERT_EQ(plog->tiered_segment_count(), 2U);

    // Recorta el prefijo por encima del primer segmento frío (base 0, rango [0,1]): se elimina del
    // tier; el segundo frío (base 2) contiene el offset 2 y se conserva.
    ASSERT_TRUE(plog->truncate_prefix_to(2).has_value());
    EXPECT_EQ(plog->tiered_segment_count(), 1U);
    EXPECT_EQ(plog->log_start_offset(), 2);
    EXPECT_FALSE(
        tier.contains(nexus::TierObjectKey{"t", 0, 0, nexus::SegmentFileKind::Log}).value());
    EXPECT_TRUE(
        tier.contains(nexus::TierObjectKey{"t", 0, 2, nexus::SegmentFileKind::Log}).value());
}

TEST(PartitionLogTiered, OffloadYRead_Cifrado_SubeCiphertextYDescifraAlRehidratar) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const TempDir data("tier_enc_data");
    const TempDir obj("tier_enc_obj");
    nexus::LocalStorageTier tier(obj.path());
    const auto key = plog_test_key();

    nexus::LogConfig cfg = tiered_small_segments(tier);
    cfg.encryption_key = key;

    auto plog = nexus::PartitionLog::open(data.path(), cfg);
    ASSERT_TRUE(plog.has_value());
    for (int i = 0; i < 6; ++i) {
        ASSERT_TRUE(plog->append(make_batch(1, 40)).has_value());
    }
    ASSERT_TRUE(plog->offload_sealed_to_tier().has_value());
    // Los segmentos cifrados son mayores (cabecera de segmento + de bloque) y rotan más a menudo,
    // así que hay varios sellados; basta con que se descargara al menos uno.
    ASSERT_GT(plog->tiered_segment_count(), 0U);

    // El objeto del tier es el `.log` cifrado tal cual: empieza por la cabecera de cifrado NXSEG1.
    const TempDir peek("tier_enc_peek");
    const auto local = peek.path() / "seg0.log";
    ASSERT_TRUE(tier.fetch_file(nexus::TierObjectKey{"t", 0, 0, nexus::SegmentFileKind::Log}, local)
                    .has_value());
    auto raw = nexus::File::open(local.string(), nexus::File::Mode::ReadOnly);
    ASSERT_TRUE(raw.has_value());
    std::array<std::byte, nexus::kEncSegmentHeaderSize> head{};
    ASSERT_TRUE(raw->read_at(head, 0).has_value());
    EXPECT_TRUE(nexus::is_encrypted_segment_header(head));

    // Lectura transparente: rehidrata y descifra con la clave del log.
    expect_readable_and_decodes(*plog);
}

}  // namespace
