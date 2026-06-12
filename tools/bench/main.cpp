/// @file   main.cpp
/// @brief  Benchmark del motor de log de NexusMQ (append: throughput + percentiles).
/// @ingroup bench
///
/// Mide el tiempo de servicio de `PartitionLog::append` y el throughput bajo cada política
/// de `fsync` (None/Interval/Commit), para cuantificar el **impacto de la durabilidad**
/// ("medido, no checklist"). El generador open-loop en red llega en Fase 1b (sin red no es
/// significativo); aquí se mide la latencia de servicio del motor monohilo (§8.2).

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <format>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bench_config.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "latency_histogram.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace {

using Clock = std::chrono::steady_clock;

// <print> es de GCC 14; el CI usa GCC 13. Se usa std::format + std::cout (disponible en ambos).
template <class... Args>
void print_line(std::format_string<Args...> fmt, Args&&... args) {
    std::cout << std::format(fmt, std::forward<Args>(args)...) << '\n';
}

nexus::RecordBatch make_batch(std::int32_t records, std::size_t payload_size) {
    nexus::RecordBatchHeader header;
    header.record_count = records;
    return nexus::RecordBatch{header, std::vector<std::byte>(payload_size, std::byte{0x42})};
}

std::string_view policy_name(nexus::FsyncPolicy policy) {
    switch (policy) {
        case nexus::FsyncPolicy::None:
            return "None    ";
        case nexus::FsyncPolicy::Interval:
            return "Interval";
        case nexus::FsyncPolicy::Commit:
            return "Commit  ";
    }
    return "?";
}

// Ejecuta el benchmark de append para una política y reporta una fila de resultados.
bool run_append_bench(const nexus::bench::BenchConfig& cfg) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("nexus_bench_" + std::to_string(static_cast<int>(cfg.fsync_policy)));
    std::filesystem::remove_all(dir);

    nexus::LogConfig log_cfg;
    log_cfg.fsync_policy = cfg.fsync_policy;
    log_cfg.fsync_interval_bytes = 64UL * 1024;  // Interval: fsync cada 64 KiB

    auto plog = nexus::PartitionLog::open(dir.string(), log_cfg);
    if (!plog) {
        std::cerr << std::format("open falló: {}\n", plog.error().message());
        return false;
    }

    const nexus::RecordBatch batch = make_batch(cfg.batch_records, cfg.payload_size);
    for (std::size_t i = 0; i < cfg.warmup_ops; ++i) {
        if (!plog->append(batch)) {
            std::cerr << "append (warmup) falló\n";
            return false;
        }
    }

    nexus::bench::LatencyHistogram hist;
    const auto wall_start = Clock::now();
    for (std::size_t i = 0; i < cfg.op_count; ++i) {
        const auto t0 = Clock::now();
        const auto result = plog->append(batch);
        const auto t1 = Clock::now();
        if (!result) {
            std::cerr << "append falló\n";
            return false;
        }
        hist.record(static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()));
    }
    const auto wall = std::chrono::duration<double>(Clock::now() - wall_start).count();

    const std::size_t bytes_per_op = nexus::RecordBatch::kHeaderSize + cfg.payload_size;
    const double ops_per_s = static_cast<double>(cfg.op_count) / wall;
    const double mb_per_s = ops_per_s * static_cast<double>(bytes_per_op) / (1024.0 * 1024.0);
    const auto us = [](std::uint64_t ns) { return static_cast<double>(ns) / 1000.0; };

    print_line("{}  {:>10.0f}  {:>9.1f}  {:>8.2f}  {:>8.2f}  {:>8.2f}  {:>9.2f}",
               policy_name(cfg.fsync_policy), ops_per_s, mb_per_s, us(hist.percentile(50)),
               us(hist.percentile(99)), us(hist.percentile(99.9)), us(hist.max()));

    std::filesystem::remove_all(dir);
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    nexus::bench::BenchConfig base;
    if (argc > 1) {
        std::size_t value = 0;
        const std::string_view arg{argv[1]};
        if (std::from_chars(arg.data(), arg.data() + arg.size(), value).ec == std::errc{}) {
            base.op_count = value;
        }
    }

    print_line("NexusMQ — benchmark de append ({} ops, payload {} B, {} records/batch)",
               base.op_count, base.payload_size, base.batch_records);
    print_line("{:<8}  {:>10}  {:>9}  {:>8}  {:>8}  {:>8}  {:>9}", "fsync", "ops/s", "MB/s",
               "p50(us)", "p99(us)", "p999(us)", "max(us)");

    for (const nexus::FsyncPolicy policy :
         {nexus::FsyncPolicy::None, nexus::FsyncPolicy::Interval, nexus::FsyncPolicy::Commit}) {
        nexus::bench::BenchConfig cfg = base;
        cfg.fsync_policy = policy;
        if (!run_append_bench(cfg)) {
            return 1;
        }
    }
    return 0;
}
