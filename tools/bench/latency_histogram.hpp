/// @file   tools/bench/latency_histogram.hpp
/// @brief  LatencyHistogram: histograma de latencias estilo HdrHistogram.
/// @ingroup bench

#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace nexus::bench {

/// @brief Histograma de latencias con error relativo acotado (estilo HdrHistogram).
/// @details Afinidad: REACTOR-LOCAL. Buckets **log-lineales**: cada octava (potencia de 2) se
///   divide en `2^kPrecisionBits` sub-buckets, lo que acota el error relativo a
///   `~2^-kPrecisionBits` (con 6 bits, ≈1.6%) en memoria reducida. Los valores `< 2^kPrecisionBits`
///   son **exactos**. Pensado para registrar latencias en nanosegundos sin *coordinated omission*
///   (el llamador mide cada operación y la registra).
class LatencyHistogram {
public:
    /// Bits de precisión: sub-buckets por octava = 2^kPrecisionBits.
    static constexpr int kPrecisionBits = 6;
    /// Frontera de la región lineal (valores menores se guardan exactos).
    static constexpr std::uint64_t kLinear = 1ULL << kPrecisionBits;

    /// Registra una muestra @p value (p. ej. nanosegundos).
    void record(std::uint64_t value) {
        const std::size_t idx = index_for(value);
        if (idx >= counts_.size()) {
            counts_.resize(idx + 1, 0);
        }
        ++counts_[idx];
        ++count_;
        max_ = std::max(max_, value);
        min_ = (count_ == 1) ? value : std::min(min_, value);
        sum_ += value;
    }

    /// @brief Valor en el percentil @p p (0..100). 0 si no hay muestras.
    [[nodiscard]] std::uint64_t percentile(double p) const {
        if (count_ == 0) {
            return 0;
        }
        const auto rank = static_cast<std::uint64_t>((p / 100.0) * static_cast<double>(count_));
        const std::uint64_t target = std::clamp<std::uint64_t>(rank, 1, count_);
        std::uint64_t cumulative = 0;
        for (std::size_t i = 0; i < counts_.size(); ++i) {
            cumulative += counts_[i];
            if (cumulative >= target) {
                return value_at(i);
            }
        }
        return max_;
    }

    /// Fusiona @p other en este histograma (para agregar mediciones).
    void merge(const LatencyHistogram& other) {
        if (other.counts_.size() > counts_.size()) {
            counts_.resize(other.counts_.size(), 0);
        }
        for (std::size_t i = 0; i < other.counts_.size(); ++i) {
            counts_[i] += other.counts_[i];
        }
        if (other.count_ > 0) {
            min_ = (count_ == 0) ? other.min_ : std::min(min_, other.min_);
        }
        count_ += other.count_;
        sum_ += other.sum_;
        max_ = std::max(max_, other.max_);
    }

    [[nodiscard]] std::uint64_t count() const noexcept { return count_; }
    [[nodiscard]] std::uint64_t max() const noexcept { return max_; }
    [[nodiscard]] std::uint64_t min() const noexcept { return count_ == 0 ? 0 : min_; }
    [[nodiscard]] double mean() const noexcept {
        return count_ == 0 ? 0.0 : static_cast<double>(sum_) / static_cast<double>(count_);
    }

private:
    // Índice del bucket de @p v (los valores < kLinear son exactos: índice = valor).
    [[nodiscard]] static std::size_t index_for(std::uint64_t value) {
        if (value < kLinear) {
            return static_cast<std::size_t>(value);
        }
        const int msb = std::bit_width(value) - 1;  // floor(log2 value) >= kPrecisionBits
        const int octave = msb - kPrecisionBits;    // >= 0
        const std::uint64_t sub = (value >> octave) - kLinear;  // [0, kLinear)
        return static_cast<std::size_t>(kLinear) +
               (static_cast<std::size_t>(octave) << kPrecisionBits) + static_cast<std::size_t>(sub);
    }

    // Cota inferior del rango de valores del bucket @p idx (representante del bucket).
    [[nodiscard]] static std::uint64_t value_at(std::size_t idx) {
        if (idx < kLinear) {
            return static_cast<std::uint64_t>(idx);
        }
        const std::uint64_t j = static_cast<std::uint64_t>(idx) - kLinear;
        const std::uint64_t octave = j >> kPrecisionBits;
        const std::uint64_t sub = j & (kLinear - 1);
        return (sub + kLinear) << octave;
    }

    std::vector<std::uint64_t> counts_;  ///< Conteo por bucket (crece bajo demanda).
    std::uint64_t count_ = 0;            ///< Muestras totales.
    std::uint64_t sum_ = 0;              ///< Suma (para la media).
    std::uint64_t max_ = 0;              ///< Máximo observado.
    std::uint64_t min_ = 0;              ///< Mínimo observado.
};

}  // namespace nexus::bench
