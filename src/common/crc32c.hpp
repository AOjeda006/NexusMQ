/// @file   crc32c.hpp
/// @brief  Checksum CRC32C (polinomio Castagnoli) del log y las tramas.
/// @ingroup common

#pragma once

#include "common/bytes.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Calcula el CRC32C (Castagnoli) de @p data.
/// @param data Bytes que cubre el checksum.
/// @param seed CRC previo para encadenar trozos; 0 al empezar.
/// @return El CRC32C de @p data (combinado con @p seed).
/// @note  Usa la instrucción SSE4.2 si la CPU la soporta (detección en runtime),
///        con fallback software por tabla. Ambos caminos dan el mismo resultado.
[[nodiscard]] Crc crc32c(ByteSpan data, Crc seed = 0) noexcept;

/// Detalles de implementación expuestos solo para pruebas (hw vs sw).
namespace detail {

/// ¿Soporta esta CPU la instrucción CRC32 de SSE4.2? (detección en runtime)
[[nodiscard]] bool cpu_has_crc32c() noexcept;

/// Implementación software por tabla (siempre disponible).
[[nodiscard]] Crc crc32c_sw(ByteSpan data, Crc seed = 0) noexcept;

/// Implementación por hardware (SSE4.2). @pre cpu_has_crc32c() == true.
[[nodiscard]] Crc crc32c_hw(ByteSpan data, Crc seed = 0) noexcept;

}  // namespace detail

}  // namespace nexus
