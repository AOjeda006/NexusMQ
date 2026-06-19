/// @file   client/dead_letter.hpp
/// @brief  DeadLetterRouter: reencamina records irrecuperables a un topic DLQ (F4).
/// @ingroup client

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "client/consumer.hpp"  // ConsumedRecord
#include "common/error.hpp"
#include "common/record_codec.hpp"
#include "common/types.hpp"

namespace nexus {

class Producer;

/// @brief Contexto de un fallo de procesamiento, para el *dead-lettering*. Afinidad: INMUTABLE.
struct DeadLetterContext {
    std::string source_topic;          ///< Topic de origen del record fallido.
    PartitionId source_partition = 0;  ///< Partición de origen.
    Offset source_offset = 0;          ///< Offset de origen.
    std::string error;                 ///< Motivo del fallo (texto).
    std::int32_t attempts = 0;         ///< Intentos de procesamiento antes de rendirse.
};

/// Nombres de los headers de metadatos DLQ (prefijo `x-dlq-`).
inline constexpr std::string_view kDlqTopicHeader = "x-dlq-topic";
inline constexpr std::string_view kDlqPartitionHeader = "x-dlq-partition";
inline constexpr std::string_view kDlqOffsetHeader = "x-dlq-offset";
inline constexpr std::string_view kDlqErrorHeader = "x-dlq-error";
inline constexpr std::string_view kDlqAttemptsHeader = "x-dlq-attempts";

/// @brief Construye el record DLQ a partir del @p failed y su @p ctx. Función pura (sin E/S).
/// @details Conserva la `key` y el `value` originales (un tombstone sigue siéndolo) y **añade** los
///   headers de metadatos (`x-dlq-*`) tras los headers originales del record.
[[nodiscard]] Record make_dead_letter(const ConsumedRecord& failed, const DeadLetterContext& ctx);

/// @brief Publica records irrecuperables en un topic DLQ con metadatos de fallo. Afinidad:
///   REACTOR-LOCAL (usa el `Producer`, que no es thread-safe).
/// @details Patrón *dead-letter queue* del consumidor: un record que no se procesa tras varios
///   intentos se reencamina a un topic aparte (con `x-dlq-*` en headers) para inspección o
///   reproceso, en vez de bloquear la partición.
class DeadLetterRouter {
public:
    DeadLetterRouter(Producer& producer, std::string dlq_topic,
                     PartitionId dlq_partition = 0) noexcept;

    /// @brief Reencamina @p failed al topic DLQ con los metadatos de @p ctx.
    /// @return el offset asignado en la DLQ, o el error de publicación.
    [[nodiscard]] expected<Offset> route(const ConsumedRecord& failed,
                                         const DeadLetterContext& ctx);

private:
    Producer& producer_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::string dlq_topic_;
    PartitionId dlq_partition_;
};

}  // namespace nexus
