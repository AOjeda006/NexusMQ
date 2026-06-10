/// @file   main.cpp
/// @brief  Arnés de microbenchmarks de NexusMQ (esqueleto, M1).
/// @ingroup tools
///
/// Por ahora solo contiene un benchmark trivial que valida el arnés. El
/// generador de carga *open-loop* y el histograma de latencia (HdrHistogram,
/// metodología anti coordinated-omission, §8.2) llegan en M6.

#include <benchmark/benchmark.h>

#include "common/version.hpp"

namespace {

/// Benchmark trivial: mide el coste (nulo) de consultar la versión. Sirve para
/// comprobar que el arnés enlaza, ejecuta y reporta.
void bm_version(benchmark::State& state) {
    for (auto _ : state) {
        benchmark::DoNotOptimize(nexus::version());
    }
}
BENCHMARK(bm_version);

}  // namespace

BENCHMARK_MAIN();
