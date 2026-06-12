/// @file types.hpp
/// @brief Tipos de ancho fijo del dominio y serializacion little-endian
/// @ingroup common

#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "common/bytes.hpp"

namespace nexus {
using Offset = std::int64_t;       ///< Identificador lógico monótono dentro de una partición.
using PartitionId = std::int32_t;  ///< Índice de partición dentro de un topic.
using NodeId = std::int32_t;       ///< Identificador de nodo del cluster.
using Term = std::int64_t;         ///< Mandato de Raft.
using Index = std::int64_t;        ///< Índice en el log de Raft.
using Crc = std::uint32_t;         ///< Checksum CRC32C.
using Epoch = std::int32_t;        ///< Época de liderazgo de partición.

/// Códec de compresión de un RecordBatch.
enum class Codec : std::uint8_t { None, Lz4, Zstd };

/// Nivel de confirmación de una escritura: sin esperar, solo el líder, o quórum (§5.x).
enum class Acks : std::uint8_t { None = 0, Leader = 1, Quorum = 2 };

/// @brief Escribe @p value en orden little-endian (el formato en disco) sobre @p out.
/// @pre out.size() >= sizeof(T). Violarla es comportamiento indefinido.
template <std::integral T>
void store_le(T value, MutByteSpan out) noexcept {
    using Unsigned = std::make_unsigned_t<T>;
    const auto bits = static_cast<Unsigned>(value);
    for (std::size_t i = 0; i < sizeof(T); i++) {
        out[i] = static_cast<std::byte>((bits >> (8 * i)) & 0xFFU);
    }
}

/// @brief Lee un T en orden little-endian desde @p in.
/// @pre in.size() >= sizeof(T). Violarla es comportamiento indefinido.
template <std::integral T>
[[nodiscard]] T load_le(ByteSpan in) noexcept {
    using Unsigned = std::make_unsigned_t<T>;
    Unsigned bits = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        bits |= static_cast<Unsigned>(std::to_integer<std::uint8_t>(in[i])) << (8 * i);
    }
    return static_cast<T>(bits);
}

}  // namespace nexus
