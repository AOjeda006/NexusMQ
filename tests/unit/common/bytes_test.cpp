#include "common/bytes.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <utility>

namespace {
constexpr std::array<std::byte, 3> kAbc{std::byte{0xA}, std::byte{0xB}, std::byte{0xC}};

TEST(Buffer, RecienConstruido_EstaVacio) {
    const nexus::Buffer buf;
    EXPECT_TRUE(buf.empty());
    EXPECT_EQ(buf.size(), 0U);
}

TEST(Buffer, Append_AcumulaBytesYActualizaSize) {
    nexus::Buffer buf;
    buf.append(kAbc);
    buf.append(kAbc);
    EXPECT_EQ(buf.size(), 6U);
    const nexus::ByteSpan span = buf.as_span();
    EXPECT_EQ(std::to_integer<int>(span[0]), 0xA);
    EXPECT_EQ(std::to_integer<int>(span[3]), 0xA);
    EXPECT_EQ(std::to_integer<int>(span[5]), 0xc);
}

TEST(Buffer, Clear_PoneSizeACeroPeroConservaCapacidad) {
    nexus::Buffer buf;
    buf.append(kAbc);
    const std::size_t cap = buf.capacity();
    buf.clear();
    EXPECT_EQ(buf.size(), 0U);
    EXPECT_EQ(buf.capacity(), cap);
}

TEST(Buffer, Movido_TransfiereLaPropiedad) {
    nexus::Buffer origen;
    origen.append(kAbc);
    const nexus::Buffer destino = std::move(origen);
    EXPECT_EQ(destino.size(), 3U);
}

TEST(Buffer, Extend_CreceYDevuelveColaMutableEscribible) {
    nexus::Buffer buf;
    buf.append(kAbc);
    const nexus::MutByteSpan tail = buf.extend(2);
    ASSERT_EQ(tail.size(), 2U);
    EXPECT_EQ(buf.size(), 5U);
    tail[0] = std::byte{0xD};
    tail[1] = std::byte{0xE};
    const nexus::ByteSpan span = buf.as_span();
    EXPECT_EQ(std::to_integer<int>(span[3]), 0xD);
    EXPECT_EQ(std::to_integer<int>(span[4]), 0xE);
}

TEST(Buffer, Truncate_ReduceSizeConservandoBytesYCapacidad) {
    nexus::Buffer buf;
    buf.append(kAbc);
    buf.append(kAbc);
    const std::size_t cap = buf.capacity();
    buf.truncate(2);
    EXPECT_EQ(buf.size(), 2U);
    EXPECT_EQ(buf.capacity(), cap);
    const nexus::ByteSpan span = buf.as_span();
    EXPECT_EQ(std::to_integer<int>(span[0]), 0xA);
    EXPECT_EQ(std::to_integer<int>(span[1]), 0xB);
}

}  // namespace
