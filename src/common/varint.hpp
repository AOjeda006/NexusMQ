/// @file   common/varint.hpp
/// @brief  Codificación varint (LEB128) y zigzag para enteros de longitud variable.
/// @ingroup common

#pragma once

#include <cstddef>
#include <cstdint>

#include "common/bytes.hpp"
#include "common/error.hpp"

namespace nexus {

/// Máximo de bytes de un varint de 64 bits (ceil(64/7) = 10).
inline constexpr std::size_t kMaxVarintBytes = 10;

/// @brief Codifica @p value como varint (LEB128 sin signo) al inicio de @p out.
/// @pre out.size() >= número de bytes necesarios (a lo sumo `kMaxVarintBytes`).
/// @return Número de bytes escritos (1..10).
[[nodiscard]] std::size_t put_varint(std::uint64_t value, MutByteSpan out) noexcept;

/// @brief Decodifica un varint desde el inicio de @p in y **avanza** @p in tras él.
/// @return El valor, o `InvalidArgument` si está truncado o excede 10 bytes (decodificador
///   defensivo: nunca lee fuera de @p in).
[[nodiscard]] expected<std::uint64_t> get_varint(ByteSpan& in);

/// Mapea un entero con signo a uno sin signo para varint (los pequeños |n| → varints cortos).
[[nodiscard]] constexpr std::uint64_t zigzag_encode(std::int64_t value) noexcept {
    return (static_cast<std::uint64_t>(value) << 1U) ^ static_cast<std::uint64_t>(value >> 63);
}

/// Inversa de `zigzag_encode`.
[[nodiscard]] constexpr std::int64_t zigzag_decode(std::uint64_t value) noexcept {
    return static_cast<std::int64_t>(value >> 1U) ^ -static_cast<std::int64_t>(value & 1U);
}

}  // namespace nexus
