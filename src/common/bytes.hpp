/// @file	bytes.hpp
/// @brief	Vistas no propietarias sobre secuencias de bytes (vocabulario de E/S binaria)
/// @ingroup common

#pragma once

#include <cstddef>
#include <span>

namespace nexus {

/// Vista de solo lectura sobre bytes contiguos (zero-copy). Afinidad INMUTABLE.
using ByteSpan = std::span<const std::byte>;

/// Vista mutable sobre bytes contiguos (zero-copy).
using MutByteSpan = std::span<std::byte>;
}  // namespace nexus
