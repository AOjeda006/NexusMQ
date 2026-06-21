/// @file   common/fnv1a.hpp
/// @brief  FNV-1a de 64 bits: hash no criptográfico determinista y estable entre plataformas.
/// @ingroup common

#pragma once

#include <cstdint>
#include <string_view>

namespace nexus {

/// @brief Hash FNV-1a de 64 bits sobre los bytes de @p data. Afinidad: INMUTABLE (función pura).
/// @details Determinista y **estable entre plataformas**: no depende de `std::hash` (cuyo resultado
///   varía por implementación y ejecución). Lo usan el reparto de grupos de consumidores por núcleo
///   (`hash(group_id) % N`, ADR-0026) —parte del contrato interno de ubicación, no viaja por wire—
///   y el anillo de hashing consistente del balanceador. No es criptográfico.
/// @param data Bytes a hashear (vista no propietaria).
/// @return El hash de 64 bits.
[[nodiscard]] constexpr std::uint64_t fnv1a_64(std::string_view data) noexcept {
    std::uint64_t hash = 14695981039346656037ULL;  // offset basis FNV-1a 64.
    for (const char byte : data) {
        hash ^= static_cast<std::uint8_t>(byte);
        hash *= 1099511628211ULL;  // primo FNV.
    }
    return hash;
}

}  // namespace nexus
