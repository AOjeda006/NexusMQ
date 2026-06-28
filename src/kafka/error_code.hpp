/// @file   kafka/error_code.hpp
/// @brief  Códigos de error del protocolo de Apache Kafka usados por el subconjunto compatible.
/// @ingroup kafka

#pragma once

#include <cstdint>

namespace nexus::kafka {

/// @brief Códigos de error del **wire de Kafka** (contrato del protocolo, no del núcleo). Afinidad:
///   INMUTABLE.
/// @details Viajan en el cuerpo de las respuestas (`error_code:INT16`) y los interpreta el cliente
///   (`kcat`/librdkafka). Son el equivalente Kafka del `WireError` nativo; el adaptador traduce el
///   `ErrorCode` interno a uno de estos **en el borde** (ADR-0009). Valores oficiales del
///   protocolo.
/// El tipo base es `int16_t` porque así viaja `error_code` en el wire (INT16), no por su rango.
/// NOLINTNEXTLINE(performance-enum-size): el tamaño espeja el campo del protocolo (INT16).
enum class KafkaError : std::int16_t {
    None = 0,                     ///< Sin error.
    Unknown = -1,                 ///< Error de servidor no clasificado.
    OffsetOutOfRange = 1,         ///< El offset pedido está fuera del rango del log.
    CorruptMessage = 2,           ///< Un batch/mensaje está corrupto (CRC o formato).
    UnknownTopicOrPartition = 3,  ///< El topic o la partición no existen.
    InvalidRequest = 42,          ///< La petición está malformada.
};

/// Valor `INT16` de @p error para escribirlo en una respuesta.
[[nodiscard]] constexpr std::int16_t to_wire(KafkaError error) noexcept {
    return static_cast<std::int16_t>(error);
}

}  // namespace nexus::kafka
