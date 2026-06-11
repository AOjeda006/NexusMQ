#include "common/crc32c.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <random>
#include <span>
#include <string_view>
#include <vector>

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

TEST(Crc32c, HardwareYSoftware_CoincidenParaVariasLongitudes) {
    if (!nexus::detail::cpu_has_crc32c()) {
        GTEST_SKIP() << "CPU sin SSE4.2: no hay camino hardware que comparar.";
    }
    std::mt19937_64 rng{0xC0FFEEU};
    std::uniform_int_distribution<int> byte_dist{0, 255};
    for (const std::size_t len : {0U, 1U, 7U, 8U, 9U, 64U, 1000U}) {
        std::vector<std::byte> buf(len);
        for (std::byte& b : buf) {
            b = static_cast<std::byte>(byte_dist(rng));
        }
        const nexus::ByteSpan span{buf};
        EXPECT_EQ(nexus::detail::crc32c_hw(span), nexus::detail::crc32c_sw(span)) << "len=" << len;
    }
}

}  // namespace
