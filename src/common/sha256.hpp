/// @file   sha256.hpp
/// @brief  SHA-256 (FIPS 180-4) y HMAC-SHA256 (RFC 2104) implementados a mano.
/// @ingroup common

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

#include "common/bytes.hpp"

namespace nexus {

/// Digest SHA-256: 32 bytes (256 bits). Afinidad: INMUTABLE (valor).
using Sha256Digest = std::array<std::byte, 32>;

/// @brief Estado de hashing SHA-256 **incremental** (FIPS 180-4).
/// @details Afinidad: REACTOR-LOCAL (no es thread-safe; cada hilo usa el suyo). Permite alimentar
/// el
///   mensaje por trozos con `update` y cerrar con `finish`; el HMAC lo reutiliza para los dos
///   pases.
/// @invariant `buffer_len_ < kBlockSize` entre llamadas a `update`.
class Sha256 {
public:
    /// Tamaño de bloque de compresión (512 bits).
    static constexpr std::size_t kBlockSize = 64;
    /// Tamaño del digest (256 bits).
    static constexpr std::size_t kDigestSize = 32;

    Sha256() noexcept;

    /// Alimenta @p data al cómputo (acumula y procesa bloques completos).
    void update(ByteSpan data) noexcept;

    /// @brief Finaliza el cómputo (padding + longitud) y devuelve el digest.
    /// @note Consume el estado; reutilizar exige `reset` (o un objeto nuevo).
    [[nodiscard]] Sha256Digest finish() noexcept;

    /// Reinicia el estado para empezar un nuevo cómputo.
    void reset() noexcept;

private:
    /// Procesa un bloque de 64 bytes apuntado por @p block (mezcla en `state_`).
    void process_block(const std::byte* block) noexcept;

    std::array<std::uint32_t, 8> state_{};        ///< vector de estado H0..H7.
    std::array<std::byte, kBlockSize> buffer_{};  ///< bloque parcial pendiente.
    std::size_t buffer_len_ = 0;                  ///< bytes válidos en `buffer_`.
    std::uint64_t total_len_ = 0;                 ///< total de bytes alimentados.
};

/// Calcula el digest SHA-256 de @p data en una sola llamada.
[[nodiscard]] Sha256Digest sha256(ByteSpan data) noexcept;

/// @brief Calcula HMAC-SHA256(@p key, @p message) (RFC 2104).
/// @details Si la clave excede el tamaño de bloque se sustituye por su SHA-256, como manda el RFC.
[[nodiscard]] Sha256Digest hmac_sha256(ByteSpan key, ByteSpan message) noexcept;

/// Codifica @p data en hexadecimal minúscula (2 caracteres por byte). Útil para *known-answer
/// tests*.
[[nodiscard]] std::string to_hex(ByteSpan data);

}  // namespace nexus
