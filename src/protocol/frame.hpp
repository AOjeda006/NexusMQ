/// @file   protocol/frame.hpp
/// @brief  FrameHeader + ApiKey: cabecera de trama del protocolo binario (§7.2).
/// @ingroup protocol

#pragma once

#include <cstddef>
#include <cstdint>

#include "common/error.hpp"

namespace nexus {

class Encoder;
class Decoder;

/// @brief Operación de la trama (`api_key:u16`). Afinidad: INMUTABLE.
// El tipo base u16 es el tamaño en el wire (apiKey:u16), no una elección de eficiencia.
// NOLINTNEXTLINE(performance-enum-size)
enum class ApiKey : std::uint16_t {
    ApiVersions,
    Metadata,
    Produce,
    Fetch,
    OffsetCommit,
    OffsetFetch,
    JoinGroup,
    SyncGroup,
    Heartbeat,
    LeaveGroup,
    CreateTopic,
    DeleteTopic,
};

/// @brief Cabecera de trama longitud-prefijo. Afinidad: INMUTABLE.
/// @details Wire (little-endian): `length:u32 | api_key:u16 | api_version:u16 |
///   correlation_id:u32 | flags:u16` (§7.2). `length` cuenta los bytes **tras** el propio campo
///   (resto de cabecera + payload), de modo que un lector lee 4 bytes y luego `length` más.
struct FrameHeader {
    /// Tamaño de la cabecera codificada: u32 + u16 + u16 + u32 + u16.
    static constexpr std::size_t kEncodedSize = 14;
    /// Bit de `flags`: la trama acarrea una actualización de créditos (control de flujo).
    static constexpr std::uint16_t kFlagCreditUpdate = 0x0001;

    std::uint32_t length = 0;  ///< Bytes tras el campo length (resto de cabecera + payload).
    ApiKey api_key = ApiKey::ApiVersions;
    std::uint16_t api_version = 0;
    std::uint32_t correlation_id = 0;
    std::uint16_t flags = 0;

    void encode(Encoder& enc) const;

    /// @brief Decodifica una cabecera; `InvalidArgument` si está truncada o `api_key` es
    /// desconocido.
    [[nodiscard]] static expected<FrameHeader> decode(Decoder& dec);

    [[nodiscard]] bool has_credit_update() const noexcept {
        return (flags & kFlagCreditUpdate) != 0;
    }

    /// Valor del campo `length` para un payload de @p payload_size bytes.
    [[nodiscard]] static constexpr std::uint32_t length_for(std::size_t payload_size) noexcept {
        return static_cast<std::uint32_t>((kEncodedSize - sizeof(std::uint32_t)) + payload_size);
    }
};

}  // namespace nexus
