// Pruebas de la ABI C de NexusMQ (nexus-ffi): paridad con el núcleo y round-trip W3C — F9.
#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/crc32c.hpp"
#include "ffi/nexus_ffi.h"
#include "telemetry/tracing.hpp"

namespace {

TEST(NexusFfi, Version_NonEmptyNulTerminated) {
    const char* version = nexus_version();
    ASSERT_NE(version, nullptr);
    EXPECT_FALSE(std::string{version}.empty());
}

TEST(NexusFfi, Crc32c_MatchesCoreImplementation) {
    const std::vector<std::byte> data{std::byte{'n'}, std::byte{'e'}, std::byte{'x'},
                                      std::byte{'u'}, std::byte{'s'}};
    // NOLINTNEXTLINE(*-reinterpret-cast): puente C↔C++ sobre los mismos bytes.
    const auto* raw = reinterpret_cast<const std::uint8_t*>(data.data());
    EXPECT_EQ(nexus_crc32c(raw, data.size()), nexus::crc32c(nexus::ByteSpan{data}));
}

TEST(NexusFfi, Crc32c_EmptyInputIsZero) {
    EXPECT_EQ(nexus_crc32c(nullptr, 0), 0U);
}

TEST(NexusFfi, Traceparent_FormatMatchesCoreAndRoundTrips) {
    constexpr std::uint64_t kHi = 0x0af7651916cd43ddULL;
    constexpr std::uint64_t kLo = 0x8448eb211c80319cULL;
    constexpr std::uint64_t kSpan = 0xb7ad6b7169203331ULL;
    constexpr std::uint8_t kFlags = 0x01;

    std::array<char, 56> buffer{};
    ASSERT_EQ(nexus_traceparent_format(kHi, kLo, kSpan, kFlags, buffer.data(), buffer.size()), 0);
    EXPECT_EQ(std::string{buffer.data()},
              "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");

    std::uint64_t hi = 0;
    std::uint64_t lo = 0;
    std::uint64_t span = 0;
    std::uint8_t flags = 0;
    ASSERT_EQ(nexus_traceparent_parse(buffer.data(), &hi, &lo, &span, &flags), 0);
    EXPECT_EQ(hi, kHi);
    EXPECT_EQ(lo, kLo);
    EXPECT_EQ(span, kSpan);
    EXPECT_EQ(flags, kFlags);
}

TEST(NexusFfi, Traceparent_FormatRejectsSmallBuffer) {
    std::array<char, 10> buffer{};
    EXPECT_EQ(nexus_traceparent_format(1, 2, 3, 0, buffer.data(), buffer.size()), -1);
}

TEST(NexusFfi, Traceparent_ParseRejectsMalformed) {
    EXPECT_EQ(nexus_traceparent_parse("no-es-un-traceparent", nullptr, nullptr, nullptr, nullptr),
              -1);
    EXPECT_EQ(nexus_traceparent_parse(nullptr, nullptr, nullptr, nullptr, nullptr), -1);
}

}  // namespace
