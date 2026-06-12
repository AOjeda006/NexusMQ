#include "common/varint.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"

namespace {

// Codifica y vuelve a decodificar, comprobando que el valor y los bytes consumidos cuadran.
std::uint64_t round_trip(std::uint64_t value) {
    std::array<std::byte, nexus::kMaxVarintBytes> buf{};
    const std::size_t written = nexus::put_varint(value, buf);
    EXPECT_GE(written, 1U);
    EXPECT_LE(written, nexus::kMaxVarintBytes);
    nexus::ByteSpan in{buf.data(), written};
    const auto decoded = nexus::get_varint(in);
    EXPECT_TRUE(decoded.has_value());
    EXPECT_TRUE(in.empty()) << "debe consumir exactamente los bytes del varint";
    return decoded.value_or(0);
}

TEST(Varint, RoundTrip_ValoresClave) {
    for (const std::uint64_t v :
         {std::uint64_t{0}, std::uint64_t{1}, std::uint64_t{127}, std::uint64_t{128},
          std::uint64_t{300}, std::uint64_t{16383}, std::uint64_t{16384},
          std::numeric_limits<std::uint64_t>::max()}) {
        EXPECT_EQ(round_trip(v), v) << "v=" << v;
    }
}

TEST(Varint, LongitudCrececonElValor) {
    std::array<std::byte, nexus::kMaxVarintBytes> buf{};
    EXPECT_EQ(nexus::put_varint(0, buf), 1U);
    EXPECT_EQ(nexus::put_varint(127, buf), 1U);
    EXPECT_EQ(nexus::put_varint(128, buf), 2U);
    EXPECT_EQ(nexus::put_varint(16383, buf), 2U);
    EXPECT_EQ(nexus::put_varint(16384, buf), 3U);
    EXPECT_EQ(nexus::put_varint(std::numeric_limits<std::uint64_t>::max(), buf), 10U);
}

TEST(Varint, RoundTrip_Aleatorio_Property) {
    std::mt19937_64 rng{0xC0FFEEU};
    for (int i = 0; i < 1000; ++i) {
        const std::uint64_t v = rng();
        EXPECT_EQ(round_trip(v), v);
    }
}

TEST(Varint, GetVarint_Truncado_DevuelveInvalidArgument) {
    // Un byte con bit de continuación pero sin sucesor: truncado.
    const std::array<std::byte, 1> buf{std::byte{0x80}};
    nexus::ByteSpan in{buf};
    const auto r = nexus::get_varint(in);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Varint, GetVarint_DemasiadoLargo_DevuelveInvalidArgument) {
    // 11 bytes, todos con continuación: excede el máximo de 10.
    std::vector<std::byte> buf(11, std::byte{0x80});
    nexus::ByteSpan in{buf};
    const auto r = nexus::get_varint(in);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Varint, GetVarint_AvanzaSoloLoConsumido) {
    std::array<std::byte, nexus::kMaxVarintBytes + 2> buf{};
    const std::size_t written = nexus::put_varint(300, buf);  // 2 bytes
    nexus::ByteSpan in{buf};
    const auto r = nexus::get_varint(in);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 300U);
    EXPECT_EQ(in.size(), buf.size() - written);  // el resto queda disponible
}

TEST(Zigzag, RoundTrip_ValoresConSigno) {
    for (const std::int64_t n :
         {std::int64_t{0}, std::int64_t{-1}, std::int64_t{1}, std::int64_t{-2}, std::int64_t{2},
          std::int64_t{-1000}, std::int64_t{1000}, std::numeric_limits<std::int64_t>::min(),
          std::numeric_limits<std::int64_t>::max()}) {
        EXPECT_EQ(nexus::zigzag_decode(nexus::zigzag_encode(n)), n) << "n=" << n;
    }
}

TEST(Zigzag, ValoresPequenos_ProducenVarintsCortos) {
    // |n| pequeño -> zigzag pequeño -> varint de 1 byte.
    EXPECT_EQ(nexus::zigzag_encode(0), 0U);
    EXPECT_EQ(nexus::zigzag_encode(-1), 1U);
    EXPECT_EQ(nexus::zigzag_encode(1), 2U);
    EXPECT_EQ(nexus::zigzag_encode(-2), 3U);
}

}  // namespace
