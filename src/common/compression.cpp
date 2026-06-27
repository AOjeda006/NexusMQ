/// @file   common/compression.cpp
/// @brief  Implementación de la compresión por códec (None/LZ4/Zstd, condicional).
/// @ingroup common

#include "common/compression.hpp"

#include <array>
#include <cstring>
#include <limits>
#include <utility>  // std::cmp_not_equal

#include "common/types.hpp"  // store_le / load_le

#ifdef NEXUS_HAVE_LZ4
#include <lz4.h>
#endif
#ifdef NEXUS_HAVE_ZSTD
#include <zstd.h>
#endif

namespace nexus {
namespace {

/// Tamaño del prefijo (u32 LE) que guarda el tamaño original descomprimido.
constexpr std::size_t kSizePrefix = 4;

/// Soporte compilado de cada códec (constantes, no literales repetidos: evita branch-clone).
#ifdef NEXUS_HAVE_LZ4
constexpr bool kLz4Compiled = true;
#else
constexpr bool kLz4Compiled = false;
#endif
#ifdef NEXUS_HAVE_ZSTD
constexpr bool kZstdCompiled = true;
#else
constexpr bool kZstdCompiled = false;
#endif

/// Añade @p value como u32 little-endian al final de @p out.
void put_u32_le(std::uint32_t value, std::vector<std::byte>& out) {
    std::array<std::byte, 4> buf{};
    store_le<std::uint32_t>(value, MutByteSpan{buf});
    out.insert(out.end(), buf.begin(), buf.end());
}

}  // namespace

bool codec_available(Codec codec) noexcept {
    switch (codec) {
        case Codec::None:
            return true;
        case Codec::Lz4:
            return kLz4Compiled;
        case Codec::Zstd:
            return kZstdCompiled;
    }
    return false;
}

expected<std::vector<std::byte>> compress(Codec codec, ByteSpan input) {
    if (codec == Codec::None) {
        return std::vector<std::byte>{input.begin(), input.end()};
    }
    if (input.size() > std::numeric_limits<std::uint32_t>::max()) {
        return make_error(ErrorCode::InvalidArgument, "entrada demasiado grande para comprimir");
    }

    std::vector<std::byte> out;
    put_u32_le(static_cast<std::uint32_t>(input.size()), out);

    switch (codec) {
        case Codec::Lz4: {
#ifdef NEXUS_HAVE_LZ4
            const int src_size = static_cast<int>(input.size());
            const int bound = LZ4_compressBound(src_size);
            if (bound <= 0) {
                return make_error(ErrorCode::InvalidArgument, "tamaño LZ4 inválido");
            }
            out.resize(kSizePrefix + static_cast<std::size_t>(bound));
            // NOLINTNEXTLINE(*-reinterpret-cast): byte→char es alias seguro.
            const char* src = reinterpret_cast<const char*>(input.data());
            // NOLINTNEXTLINE(*-reinterpret-cast): byte→char es alias seguro.
            char* dst = reinterpret_cast<char*>(out.data() + kSizePrefix);
            const int written = LZ4_compress_default(src, dst, src_size, bound);
            if (written <= 0) {
                return make_error(ErrorCode::IoError, "fallo de compresión LZ4");
            }
            out.resize(kSizePrefix + static_cast<std::size_t>(written));
            return out;
#else
            return make_error(ErrorCode::Unsupported, "LZ4 no compilado");
#endif
        }
        case Codec::Zstd: {
#ifdef NEXUS_HAVE_ZSTD
            const std::size_t bound = ZSTD_compressBound(input.size());
            out.resize(kSizePrefix + bound);
            const std::size_t written = ZSTD_compress(out.data() + kSizePrefix, bound, input.data(),
                                                      input.size(), ZSTD_CLEVEL_DEFAULT);
            if (ZSTD_isError(written) != 0U) {
                return make_error(ErrorCode::IoError, "fallo de compresión Zstd");
            }
            out.resize(kSizePrefix + written);
            return out;
#else
            return make_error(ErrorCode::Unsupported, "Zstd no compilado");
#endif
        }
        case Codec::None:
            break;  // gestionado arriba.
    }
    return make_error(ErrorCode::Unsupported, "códec de compresión desconocido");
}

expected<std::vector<std::byte>> decompress(Codec codec, ByteSpan input, std::size_t max_output) {
    if (codec == Codec::None) {
        return std::vector<std::byte>{input.begin(), input.end()};
    }
    if (input.size() < kSizePrefix) {
        return make_error(ErrorCode::Corrupt, "bloque comprimido truncado");
    }
    const auto original_size = load_le<std::uint32_t>(input.subspan(0, kSizePrefix));
    // Anti decompression-bomb: rechaza antes de reservar memoria (§7.9).
    if (original_size > max_output) {
        return make_error(ErrorCode::Corrupt, "tamaño descomprimido excede el límite");
    }
    // `body` solo se consume en las ramas de códec compiladas (LZ4/Zstd). En una build sin ningún
    // códec nativo (p. ej. Windows sin LZ4/Zstd) queda sin usar: lo marcamos para no avisar.
    [[maybe_unused]] const ByteSpan body = input.subspan(kSizePrefix);
    std::vector<std::byte> out(original_size);

    switch (codec) {
        case Codec::Lz4: {
#ifdef NEXUS_HAVE_LZ4
            if (body.size() > std::numeric_limits<int>::max() ||
                out.size() > std::numeric_limits<int>::max()) {
                return make_error(ErrorCode::Corrupt, "bloque LZ4 demasiado grande");
            }
            // NOLINTNEXTLINE(*-reinterpret-cast): byte→char es alias seguro.
            const char* src = reinterpret_cast<const char*>(body.data());
            // NOLINTNEXTLINE(*-reinterpret-cast): byte→char es alias seguro.
            char* dst = reinterpret_cast<char*>(out.data());
            const int produced = LZ4_decompress_safe(src, dst, static_cast<int>(body.size()),
                                                     static_cast<int>(out.size()));
            if (produced < 0 || std::cmp_not_equal(produced, original_size)) {
                return make_error(ErrorCode::Corrupt, "bloque LZ4 inválido");
            }
            return out;
#else
            return make_error(ErrorCode::Unsupported, "LZ4 no compilado");
#endif
        }
        case Codec::Zstd: {
#ifdef NEXUS_HAVE_ZSTD
            const std::size_t produced =
                ZSTD_decompress(out.data(), out.size(), body.data(), body.size());
            if (ZSTD_isError(produced) != 0U || produced != original_size) {
                return make_error(ErrorCode::Corrupt, "bloque Zstd inválido");
            }
            return out;
#else
            return make_error(ErrorCode::Unsupported, "Zstd no compilado");
#endif
        }
        case Codec::None:
            break;  // gestionado arriba.
    }
    return make_error(ErrorCode::Unsupported, "códec de compresión desconocido");
}

}  // namespace nexus
