#include "common/crc32c.hpp"

#include <nmmintrin.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(_MSC_VER)
#include <intrin.h>  // __cpuid: detección de CPU en runtime (MSVC no tiene __builtin_cpu_supports)
#endif

// GCC/Clang necesitan [[gnu::target("sse4.2")]] para emitir SSE4.2 en una sola función sin exigir
// -msse4.2 a todo el binario. MSVC no admite el atributo (avisa C5030) y habilita los intrínsecos
// SSE4.2 de forma incondicional, así que ahí el marcador queda vacío.
#if defined(_MSC_VER)
#define NEXUS_TARGET_SSE42
#else
#define NEXUS_TARGET_SSE42 [[gnu::target("sse4.2")]]
#endif

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

namespace detail {

bool cpu_has_crc32c() noexcept {
#if defined(_MSC_VER)
    // MSVC no tiene __builtin_cpu_supports: consultamos CPUID hoja 1, bit 20 de ECX (SSE4.2).
    std::array<int, 4> regs{};
    __cpuid(regs.data(), 1);
    constexpr int kSse42BitEcx = 1 << 20;
    return (regs[2] & kSse42BitEcx) != 0;
#else
    return __builtin_cpu_supports("sse4.2");
#endif
}

Crc crc32c_sw(ByteSpan data, Crc seed) noexcept {
    std::uint32_t crc = ~static_cast<std::uint32_t>(seed);
    for (const std::byte b : data) {
        const std::uint32_t index = (crc ^ std::to_integer<std::uint8_t>(b)) & 0xFFU;
        crc = kTable[index] ^ (crc >> 8);
    }
    return static_cast<Crc>(crc ^ 0xFFFFFFFFU);
}

// El marcador (NEXUS_TARGET_SSE42) permite emitir instrucciones SSE4.2 en ESTA función sin exigir
// -msse4.2 a todo el binario; solo se llama si cpu_has_crc32c() (sin SIGILL/SIGILL equivalente).
NEXUS_TARGET_SSE42 Crc crc32c_hw(ByteSpan data, Crc seed) noexcept {
    std::uint32_t crc = ~static_cast<std::uint32_t>(seed);
    const std::size_t size = data.size();
    std::size_t i = 0;
    for (; i + sizeof(std::uint64_t) <= size; i += sizeof(std::uint64_t)) {
        std::uint64_t chunk = 0;
        std::memcpy(&chunk, data.data() + i, sizeof(chunk));
        crc = static_cast<std::uint32_t>(_mm_crc32_u64(crc, chunk));
    }
    for (; i < size; ++i) {
        crc = _mm_crc32_u8(crc, std::to_integer<std::uint8_t>(data[i]));
    }
    return static_cast<Crc>(crc ^ 0xFFFFFFFFU);
}

}  // namespace detail

Crc crc32c(ByteSpan data, Crc seed) noexcept {
    static const bool has_hw = detail::cpu_has_crc32c();
    return has_hw ? detail::crc32c_hw(data, seed) : detail::crc32c_sw(data, seed);
}

}  // namespace nexus
