/// @file   tests/crash/partition_crash_test.cpp
/// @brief  Test de crash: un proceso hijo confirma datos (fsync) y muere con SIGKILL a mitad
///         de una escritura; el padre recupera el log sin perder lo confirmado (§7.11 #2).

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "common/record.hpp"
#include "common/types.hpp"
#include "io/file.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace {

constexpr int kCommitted = 5;  // batches confirmados (fsync) antes del "crash"

std::string unique_dir() {
    return (std::filesystem::temp_directory_path() /
            ("nexus_crash_" + std::to_string(::getpid()) + "_" +
             std::to_string(::testing::UnitTest::GetInstance()->random_seed())))
        .string();
}

nexus::RecordBatch one_record_batch(std::size_t payload_len) {
    nexus::RecordBatchHeader header;
    header.record_count = 1;
    return nexus::RecordBatch{header, std::vector<std::byte>(payload_len, std::byte{0xA5})};
}

// Ejecutado por el proceso hijo: confirma kCommitted batches, deja una cola torn y se autodestruye
// con SIGKILL (sin teardown). No debe retornar; si algo falla antes de matarse, _exit(!=0).
[[noreturn]] void child_commit_then_crash(const std::string& dir) {
    nexus::LogConfig cfg;
    cfg.fsync_policy = nexus::FsyncPolicy::Commit;  // cada append es durable
    auto plog = nexus::PartitionLog::open(dir, cfg);
    if (!plog) {
        _exit(2);
    }
    for (int i = 0; i < kCommitted; ++i) {
        if (!plog->append(one_record_batch(20))) {
            _exit(3);
        }
    }
    // Simula una escritura interrumpida por el crash: cola torn tras lo confirmado.
    auto log = nexus::File::open((std::filesystem::path{dir} / "00000000000000000000.log").string(),
                                 nexus::File::Mode::ReadWrite);
    if (!log) {
        _exit(4);
    }
    const auto size = log->size();
    if (!size) {
        _exit(5);
    }
    const std::vector<std::byte> torn(11, std::byte{0x3C});
    if (!log->write_at(torn, *size)) {
        _exit(6);
    }
    ::kill(::getpid(), SIGKILL);  // muerte dura, sin limpieza
    _exit(7);                     // inalcanzable
}

TEST(PartitionCrash, KillTrasFsync_RecuperaConfirmadosYTruncaColaTorn) {
    const std::string dir = unique_dir();
    std::filesystem::remove_all(dir);

    const pid_t pid = ::fork();
    ASSERT_GE(pid, 0) << "fork falló";
    if (pid == 0) {
        child_commit_then_crash(dir);  // no retorna
    }

    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFSIGNALED(status)) << "el hijo debía morir por señal";
    ASSERT_EQ(WTERMSIG(status), SIGKILL);

    // El padre recupera: lo confirmado sobrevive, la cola torn se trunca.
    auto recovered = nexus::PartitionLog::open(dir, nexus::LogConfig{});
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ(recovered->log_end_offset(), kCommitted);

    const auto fr = recovered->read(0, 1U << 20);
    ASSERT_TRUE(fr.has_value());
    EXPECT_EQ(fr->next_offset, kCommitted);
    const auto first = nexus::RecordBatch::decode(fr->batches.as_span());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->header().base_offset, 0);

    std::filesystem::remove_all(dir);
}

}  // namespace
