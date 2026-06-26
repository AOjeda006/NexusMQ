/// @file   tools/loadgen/loadgen_report.hpp
/// @brief  LoadGenReport: resultado de una campaña del generador de carga.
/// @ingroup loadgen

#pragma once

#include <cstdint>
#include <format>
#include <string>

namespace nexus::loadgen {

/// @brief Resultado agregado de una campaña open-loop. Afinidad: INMUTABLE.
/// @details Percentiles en **nanosegundos** (los registra `LatencyHistogram`), corregidos de
///   *coordinated omission* (latencia contra el instante previsto, no el de envío). `achieved_rate`
///   es el throughput real medido sobre la ventana medida (debe acercarse a `target_rate` si el
///   sistema no se satura: comparar ambos es la comprobación de saturación del método USE).
struct LoadGenReport {
    std::uint64_t recorded = 0;  ///< Peticiones medidas con éxito (registradas en el histograma).
    std::uint64_t errors = 0;    ///< Peticiones fallidas en la ventana medida (no entran al hist).
    double target_rate = 0.0;    ///< Tasa objetivo configurada (req/s); `0` = sin ritmo.
    double achieved_rate = 0.0;  ///< Throughput real medido (req/s) sobre la ventana medida.
    std::uint64_t p50_ns = 0;    ///< Mediana de latencia (ns).
    std::uint64_t p99_ns = 0;    ///< Percentil 99 de latencia (ns).
    std::uint64_t p999_ns = 0;   ///< Percentil 99.9 de latencia (ns).
    std::uint64_t max_ns = 0;    ///< Latencia máxima observada (ns).
    std::uint64_t min_ns = 0;    ///< Latencia mínima observada (ns).
    double mean_ns = 0.0;        ///< Latencia media (ns).
};

/// @brief Línea de resumen legible de @p report (tasas en req/s, latencias en µs).
[[nodiscard]] inline std::string summary_line(const LoadGenReport& report) {
    constexpr double kNsPerUs = 1'000.0;
    const auto us = [](std::uint64_t ns) { return static_cast<double>(ns) / kNsPerUs; };
    return std::format(
        "recorded={} errors={} target={:.0f} req/s achieved={:.0f} req/s | "
        "p50={:.2f} p99={:.2f} p999={:.2f} max={:.2f} (us)",
        report.recorded, report.errors, report.target_rate, report.achieved_rate, us(report.p50_ns),
        us(report.p99_ns), us(report.p999_ns), us(report.max_ns));
}

}  // namespace nexus::loadgen
