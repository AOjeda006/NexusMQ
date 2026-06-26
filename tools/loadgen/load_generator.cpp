/// @file   tools/loadgen/load_generator.cpp
/// @brief  Implementación del motor de carga open-loop (ver load_generator.hpp).
/// @ingroup loadgen

#include "load_generator.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <thread>
#include <vector>

#include "common/types.hpp"
#include "latency_histogram.hpp"  // header-only en tools/bench (DRY: mismo histograma que el append)
#include "load_schedule.hpp"

namespace nexus::loadgen {
namespace {

using Clock = std::chrono::steady_clock;
constexpr double kNanosPerSecond = 1'000'000'000.0;

/// Estado por conexión (un hilo): histograma propio + contadores + ventana medida (shared-nothing).
struct WorkerResult {
    bench::LatencyHistogram hist;  ///< Latencias registradas por esta conexión.
    std::uint64_t errors = 0;      ///< Peticiones fallidas (ventana medida).
    bool has_window = false;       ///< ¿Registró al menos una petición medida?
    MonoTime window_start;         ///< Envío de la primera petición medida.
    MonoTime window_end;           ///< Recepción de la última petición medida.
};

/// Nanosegundos entre dos instantes monótonos (saturado a 0 si @p to precede a @p from).
std::uint64_t elapsed_ns(MonoTime from, MonoTime to) {
    const auto delta = std::chrono::duration_cast<std::chrono::nanoseconds>(to - from).count();
    return delta > 0 ? static_cast<std::uint64_t>(delta) : 0;
}

/// @brief Procesa las peticiones de la conexión @p worker_id (índices worker_id, +connections, …).
/// @details Para cada índice espera hasta su instante previsto (open-loop) antes de disparar la
///   petición y registra la latencia **contra ese instante** (corrección de coordinated omission).
///   Las primeras `warmup_ops` peticiones se envían pero no se miden.
void run_worker(RequestRunner& runner, const LoadGenConfig& cfg, const OpenLoopSchedule& schedule,
                int worker_id, WorkerResult& out) {
    const std::uint64_t total = cfg.warmup_ops + cfg.op_count;
    const auto stride = static_cast<std::uint64_t>(cfg.connections);
    for (auto i = static_cast<std::uint64_t>(worker_id); i < total; i += stride) {
        const MonoTime intended = schedule.intended_at(i);
        if (schedule.is_paced()) {
            std::this_thread::sleep_until(intended);
        }
        const MonoTime sent = Clock::now();
        const expected<void> result = runner.run_once();
        const MonoTime done = Clock::now();
        if (i < cfg.warmup_ops) {
            continue;  // calentamiento: enviado pero no medido
        }
        if (!result) {
            ++out.errors;
            continue;
        }
        out.hist.record(elapsed_ns(intended, done));
        if (!out.has_window) {
            out.window_start = sent;
            out.has_window = true;
        }
        out.window_end = done;
    }
}

/// Fusiona los resultados por-hilo en un único informe (histograma agregado + throughput real).
LoadGenReport aggregate(const LoadGenConfig& cfg, const std::vector<WorkerResult>& results) {
    bench::LatencyHistogram merged;
    LoadGenReport report;
    report.target_rate = cfg.target_rate;
    bool has_window = false;
    MonoTime start{};
    MonoTime end{};
    for (const WorkerResult& worker : results) {
        merged.merge(worker.hist);
        report.errors += worker.errors;
        if (!worker.has_window) {
            continue;
        }
        start = has_window ? std::min(start, worker.window_start) : worker.window_start;
        end = has_window ? std::max(end, worker.window_end) : worker.window_end;
        has_window = true;
    }
    report.recorded = merged.count();
    report.p50_ns = merged.percentile(50);
    report.p99_ns = merged.percentile(99);
    report.p999_ns = merged.percentile(99.9);
    report.max_ns = merged.max();
    report.min_ns = merged.min();
    report.mean_ns = merged.mean();
    if (has_window && report.recorded > 0) {
        if (const std::uint64_t wall_ns = elapsed_ns(start, end); wall_ns > 0) {
            report.achieved_rate = static_cast<double>(report.recorded) * kNanosPerSecond /
                                   static_cast<double>(wall_ns);
        }
    }
    return report;
}

}  // namespace

LoadGenerator::LoadGenerator(LoadGenConfig config, RunnerFactory factory)
    : config_(config), factory_(std::move(factory)) {}

expected<LoadGenReport> LoadGenerator::run() {
    if (config_.connections < 1) {
        return make_error(ErrorCode::InvalidArgument, "connections debe ser >= 1");
    }
    if (config_.op_count == 0) {
        return make_error(ErrorCode::InvalidArgument, "op_count debe ser > 0");
    }
    const auto workers = static_cast<std::size_t>(config_.connections);

    // Crea un runner (una conexión) por hilo ANTES de medir: un fallo de conexión aborta la
    // campaña.
    std::vector<std::unique_ptr<RequestRunner>> runners;
    runners.reserve(workers);
    for (std::size_t w = 0; w < workers; ++w) {
        expected<std::unique_ptr<RequestRunner>> runner = factory_(static_cast<int>(w));
        if (!runner) {
            return std::unexpected<Error>(runner.error());
        }
        runners.push_back(std::move(*runner));
    }

    std::vector<WorkerResult> results(workers);
    const OpenLoopSchedule schedule{config_.target_rate, Clock::now()};

    // Un hilo por conexión (jthread no está en libc++ 18 → std::thread + join explícito).
    std::vector<std::thread> threads;
    threads.reserve(workers);
    for (std::size_t w = 0; w < workers; ++w) {
        threads.emplace_back([&runners, &results, &schedule, this, w] {
            run_worker(*runners[w], config_, schedule, static_cast<int>(w), results[w]);
        });
    }
    for (std::thread& worker : threads) {
        worker.join();
    }

    return aggregate(config_, results);
}

}  // namespace nexus::loadgen
