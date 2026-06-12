#include "latency_histogram.hpp"

#include <gtest/gtest.h>

#include <cstdint>

namespace {

using nexus::bench::LatencyHistogram;

TEST(LatencyHistogram, VacioDevuelveCeros) {
    const LatencyHistogram hist;
    EXPECT_EQ(hist.count(), 0U);
    EXPECT_EQ(hist.percentile(50), 0U);
    EXPECT_EQ(hist.max(), 0U);
    EXPECT_DOUBLE_EQ(hist.mean(), 0.0);
}

TEST(LatencyHistogram, ValoresPequenos_PercentilesExactos) {
    LatencyHistogram hist;
    for (std::uint64_t v = 1; v <= 100; ++v) {  // valores < 128 son exactos
        hist.record(v);
    }
    EXPECT_EQ(hist.count(), 100U);
    EXPECT_EQ(hist.min(), 1U);
    EXPECT_EQ(hist.max(), 100U);
    EXPECT_EQ(hist.percentile(50), 50U);
    EXPECT_EQ(hist.percentile(99), 99U);
    EXPECT_EQ(hist.percentile(100), 100U);
}

TEST(LatencyHistogram, ValoresGrandes_ErrorRelativoAcotado) {
    LatencyHistogram hist;
    constexpr std::uint64_t kValue = 1'000'000;  // ~1 ms en ns
    for (int i = 0; i < 1000; ++i) {
        hist.record(kValue);
    }
    const std::uint64_t p50 = hist.percentile(50);
    // Error relativo acotado por ~2^-kPrecisionBits (≈1.6%); el representante es la cota inferior.
    EXPECT_LE(p50, kValue);
    EXPECT_GE(p50, kValue - (kValue >> LatencyHistogram::kPrecisionBits) - 1);
    EXPECT_EQ(hist.max(), kValue);  // el máximo se guarda exacto
}

TEST(LatencyHistogram, Merge_SumaConteosYExtremos) {
    LatencyHistogram a;
    LatencyHistogram b;
    a.record(10);
    a.record(20);
    b.record(5);
    b.record(30);
    a.merge(b);
    EXPECT_EQ(a.count(), 4U);
    EXPECT_EQ(a.min(), 5U);
    EXPECT_EQ(a.max(), 30U);
    EXPECT_EQ(a.percentile(100), 30U);
}

}  // namespace
