#include "common/crc32c.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <span>
#include <string_view>

namespace {
nexus::ByteSpan bytes_of(std::string_view s) {
    return std::as_bytes(std::span<const char>{s.data(), s.size()});
}

TEST(Crc32c, CadenaVacia_DevuelveCero) {
    EXPECT_EQ(nexus::crc32c(nexus::ByteSpan{}), 0U);
}

TEST(Crc32c, Vector123456789_CoincideConElValorDeReferencia) {
    EXPECT_EQ(nexus::crc32c(bytes_of("123456789")), 0xE3069283U);
}

TEST(Crc32c, Encadenado_EquivaleAUnSoloCalculo) {
    const auto full = bytes_of("hola mundo");
    const nexus::Crc chained = nexus::crc32c(full.subspan(4), nexus::crc32c(full.subspan(0, 4)));
    EXPECT_EQ(chained, nexus::crc32c(full));
}

}  // namespace
