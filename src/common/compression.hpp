/// @file   common/compression.hpp
/// @brief  Compresión de bloques por códec (None/LZ4/Zstd) con protección anti *decompression
/// bomb*.
/// @ingroup common

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"  // Codec

namespace nexus {

/// Bits de códec dentro de `RecordBatchHeader::attrs` (los 2 bits bajos: 0=None, 1=Lz4, 2=Zstd).
inline constexpr std::uint16_t kCodecMask = 0x0003;

/// Extrae el códec de compresión de @p attrs.
[[nodiscard]] constexpr Codec codec_from_attrs(std::uint16_t attrs) noexcept {
    return static_cast<Codec>(attrs & kCodecMask);
}

/// Devuelve @p attrs con sus bits de códec fijados a @p codec.
[[nodiscard]] constexpr std::uint16_t attrs_with_codec(std::uint16_t attrs, Codec codec) noexcept {
    return static_cast<std::uint16_t>((attrs & ~kCodecMask) |
                                      (static_cast<std::uint16_t>(codec) & kCodecMask));
}

/// @brief ¿Hay soporte **compilado** para @p codec? `None` siempre; `Lz4`/`Zstd` según el build
///   (se compilan condicionalmente como el plano TLS, ADR-0019).
[[nodiscard]] bool codec_available(Codec codec) noexcept;

/// @brief Comprime @p input con @p codec. `None` => copia tal cual.
/// @details El resultado lleva el **tamaño original** como prefijo (u32 little-endian), para que
///   la descompresión pueda acotar la salida sin confiar en metadatos del propio formato.
/// @return Los bytes comprimidos, o `Unsupported` si el códec no está compilado.
[[nodiscard]] expected<std::vector<std::byte>> compress(Codec codec, ByteSpan input);

/// @brief Descomprime @p input con @p codec, **acotando** la salida a @p max_output bytes.
/// @details Defensa anti *decompression bomb* (§7.9): el tamaño original (prefijo) se compara con
///   @p max_output **antes** de reservar memoria; un bloque que se declare mayor se rechaza. `None`
///   => copia tal cual.
/// @return Los bytes descomprimidos, `Corrupt` si el bloque es inválido/excede @p max_output, o
///   `Unsupported` si el códec no está compilado.
[[nodiscard]] expected<std::vector<std::byte>> decompress(Codec codec, ByteSpan input,
                                                          std::size_t max_output);

}  // namespace nexus
