/// @file   reactor/cache_line.hpp
/// @brief  Tamaño de línea de caché para separar variables y evitar false sharing.
/// @ingroup reactor

#pragma once

#include <cstddef>

namespace nexus {

/// @brief Tamaño de línea de caché asumido (bytes). Afinidad: INMUTABLE.
/// @details Constante fija (64 B en x86-64 y la mayoría de ARM) en vez de
///   `std::hardware_destructive_interference_size`: ese valor dispara el aviso
///   `-Winterference-size` de GCC (su uso en disposición de objetos no es estable
///   entre unidades de traducción) y aquí solo lo necesitamos para `alignas`, donde
///   un valor constante y suficientemente grande basta para apartar cada átomo
///   contendido a su propia línea.
inline constexpr std::size_t kCacheLineSize = 64;

}  // namespace nexus
