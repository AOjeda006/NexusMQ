/// @file crc32c.hpp
/// @brief checksum CRC32C (polinomio Castagnoli) del log y las tramas.
/// @ingroup common

#pragma once

#include "common/bytes.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Calcula el checksum CRC32C (Castagnoli) de @p data.
/// @param data Bytes que cubre el checksum.
/// @param seed CRC previo para encadenar trozos; 0 al empezar.
/// @return El CRC32C de @p data (combinado con @p seed).
/// @note El CRC32C es el estándar de facto en sistemas de log (detecta corrupción silenciosa). De
/// momento usa la implementación software por tabla; el camino por hardware llega en M2.2b.
[[nodiscard]] Crc crc32c(ByteSpan data, Crc seed = 0) noexcept;

}  // namespace nexus
