/// @file   common/control_record.hpp
/// @brief  Control records de transacción (marcadores COMMIT/ABORT) y flags de batch transaccional.
/// @ingroup common

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"

namespace nexus {

/// @name Flags transaccionales de `RecordBatchHeader::attrs`
/// @details Los 2 bits bajos de `attrs` son el códec de compresión (`kCodecMask`, ver
///   compression.hpp); estos flags viven en bits altos, disjuntos del códec. Un batch
///   **transaccional** pertenece a una transacción abierta (su visibilidad depende de su
///   marcador); un batch de **control** no lleva datos de usuario sino un único **marcador**
///   COMMIT/ABORT escrito por el coordinador de transacciones ([ADR-0033]).
/// @{
inline constexpr std::uint16_t kTransactionalAttr = 0x0010;  ///< bit 4: batch transaccional.
inline constexpr std::uint16_t kControlAttr = 0x0020;  ///< bit 5: batch de control (marcador).
/// @}

/// ¿El batch pertenece a una transacción (bit transaccional en @p attrs)?
[[nodiscard]] constexpr bool is_transactional(std::uint16_t attrs) noexcept {
    return (attrs & kTransactionalAttr) != 0;
}

/// ¿El batch es un batch de control (marcador COMMIT/ABORT)?
[[nodiscard]] constexpr bool is_control(std::uint16_t attrs) noexcept {
    return (attrs & kControlAttr) != 0;
}

/// Devuelve @p attrs con el bit transaccional fijado a @p on.
[[nodiscard]] constexpr std::uint16_t attrs_with_transactional(std::uint16_t attrs,
                                                               bool on) noexcept {
    return on ? static_cast<std::uint16_t>(attrs | kTransactionalAttr)
              : static_cast<std::uint16_t>(attrs & ~kTransactionalAttr);
}

/// Devuelve @p attrs con el bit de control fijado a @p on.
[[nodiscard]] constexpr std::uint16_t attrs_with_control(std::uint16_t attrs, bool on) noexcept {
    return on ? static_cast<std::uint16_t>(attrs | kControlAttr)
              : static_cast<std::uint16_t>(attrs & ~kControlAttr);
}

/// @brief Tipo de marcador de control (final de transacción). Afinidad: INMUTABLE.
/// @details Valores **de wire** compatibles con la convención Kafka: `Abort=0`, `Commit=1`. En el
///   wire el tipo viaja como `i16` (el codec ensancha explícitamente); en memoria basta `uint8_t`.
///   El consumidor `read_committed` entrega los records de una transacción **solo** al ver su
///   marcador `Commit`; ante `Abort` los descarta (C3).
enum class ControlRecordType : std::uint8_t {
    Abort = 0,   ///< La transacción se abortó: sus records no deben hacerse visibles.
    Commit = 1,  ///< La transacción confirmó: sus records se hacen visibles atómicamente.
};

/// Versión del formato de marcador de control (para evolución del wire).
inline constexpr std::int16_t kControlRecordVersion = 0;

/// Tamaño en bytes de la clave de un control record (`version:i16 | type:i16`).
inline constexpr std::size_t kControlKeySize = 4;
/// Tamaño en bytes del valor de un control record (`version:i16 | coordinator_epoch:i32`).
inline constexpr std::size_t kControlValueSize = 6;

/// @brief Marcador de fin de transacción (COMMIT/ABORT). Afinidad: INMUTABLE.
/// @details Es el contenido lógico del **único record** de un batch de control. El
///   `coordinator_epoch` es la época de liderazgo del coordinador que emitió el marcador; permite a
///   la partición **descartar** marcadores de un coordinador obsoleto (fencing en el failover, C2).
/// @invariant decode_end_txn_marker(encode_control_key(m), encode_control_value(m)) == m.
struct EndTxnMarker {
    ControlRecordType type = ControlRecordType::Abort;  ///< COMMIT o ABORT.
    Epoch coordinator_epoch = 0;                        ///< Época del coordinador emisor.
    std::int16_t version = kControlRecordVersion;       ///< Versión del formato.
    bool operator==(const EndTxnMarker&) const = default;
};

/// @brief Serializa la **clave** del control record de @p marker (`version:i16 | type:i16`).
[[nodiscard]] std::vector<std::byte> encode_control_key(const EndTxnMarker& marker);

/// @brief Serializa el **valor** del control record de @p marker
///   (`version:i16 | coordinator_epoch:i32`).
[[nodiscard]] std::vector<std::byte> encode_control_value(const EndTxnMarker& marker);

/// @brief Decodifica un marcador desde su @p key y @p value (decodificador defensivo).
/// @return El marcador, `Corrupt` si los tamaños o el tipo son inválidos, o `Unsupported` si la
///   versión no se reconoce.
[[nodiscard]] expected<EndTxnMarker> decode_end_txn_marker(ByteSpan key, ByteSpan value);

/// @brief Construye un batch de control de un solo marcador @p marker.
/// @details Fija los bits transaccional + control en `attrs`, `record_count = 1`, sin comprimir; el
///   `base_offset` lo asigna el log al anexar. Los campos de idempotencia @p producer_id /
///   @p producer_epoch identifican al productor dueño de la transacción.
[[nodiscard]] RecordBatch build_control_batch(const EndTxnMarker& marker, ProducerId producer_id,
                                              std::int16_t producer_epoch);

/// @brief Extrae el marcador de un batch de control.
/// @return El marcador, `InvalidArgument` si @p batch no es de control o no tiene exactamente un
///   record con clave y valor, o el error de `decode_end_txn_marker`.
[[nodiscard]] expected<EndTxnMarker> parse_control_batch(const RecordBatch& batch);

}  // namespace nexus
