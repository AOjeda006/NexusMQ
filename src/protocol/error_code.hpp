/// @file   protocol/error_code.hpp
/// @brief  WireError: códigos de error del protocolo binario (§7.2.2).
/// @ingroup protocol

#pragma once

#include <cstdint>

#include "common/error.hpp"

namespace nexus {

/// @brief Códigos de error transmitidos en el wire (`errorCode:i16`). Afinidad: INMUTABLE.
/// @details Es el contrato externo de errores (ADR-0009): el núcleo usa `Error`/`ErrorCode` y se
///   traduce a `WireError` **en el borde** (`from_error`). El cliente decide reintentos con
///   `is_retryable`. `None` = éxito.
// El tipo base es el tamaño en el wire (errorCode:i16), no una elección de eficiencia.
// NOLINTNEXTLINE(performance-enum-size)
enum class WireError : std::int16_t {
    None = 0,
    NotLeaderForPartition,
    LeaderNotAvailable,
    UnknownTopicOrPartition,
    OffsetOutOfRange,
    NotEnoughReplicas,
    RequestTimedOut,
    CorruptMessage,
    MessageTooLarge,
    OutOfOrderSequence,
    DuplicateSequence,
    Throttled,
    RebalanceInProgress,
    UnsupportedVersion,
    Unauthorized,
    InvalidRequest,
};

/// @brief ¿Debería el cliente reintentar ante @p error? (condiciones transitorias).
[[nodiscard]] bool is_retryable(WireError error) noexcept;

/// @brief Traduce un `Error` interno al código de wire (en el borde del protocolo).
[[nodiscard]] WireError from_error(const Error& error) noexcept;

/// @brief Traduce un código de wire a un `Error` interno (en el borde del **cliente**, ADR-0009).
/// @details Inverso aproximado de `from_error`: el cliente recibe `errorCode:i16` y lo reintroduce
///   en el modelo interno. `WireError::None` no es error (el llamante lo comprueba antes); se mapea
///   a `InvalidArgument` por robustez.
[[nodiscard]] Error to_error(WireError error);

}  // namespace nexus
