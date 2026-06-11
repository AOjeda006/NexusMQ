#include "common/crc32c.hpp"

#include <array>
#include <cstddef>
#include <cstdint>

namespace nexus {
namespace {

/// Genera en compilación la tabla del CRC32C reflejado (Castagnoli 0x82F63B78):
/// kTable[b] = CRC de un único byte b partiendo de 0.
constexpr std::array<std::uint32_t, 256> make_table() {
    constexpr std::uint32_t kPoly = 0x82F63B78U;
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < table.size(); ++i) {
        std::uint32_t crc = i;
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ ((crc & 1U) != 0U ? kPoly : 0U);
        }
        table[i] = crc;
    }
    return table;
}

constexpr std::array<std::uint32_t, 256> kTable = make_table();

}  // namespace

Crc crc32c(ByteSpan data, Crc seed) noexcept {
    std::uint32_t crc = ~static_cast<std::uint32_t>(seed);
    for (const std::byte b : data) {
        const std::uint32_t index = (crc ^ std::to_integer<std::uint8_t>(b)) & 0xFFU;
        crc = kTable[index] ^ (crc >> 8);
    }
    return static_cast<Crc>(crc ^ 0xFFFFFFFFU);
}

}  // namespace nexus
