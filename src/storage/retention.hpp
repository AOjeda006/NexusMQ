/// @file   storage/retention.hpp
/// @brief  RetentionPolicy: criterio de retención del log (por tamaño y/o tiempo).
/// @ingroup storage

#pragma once

#include <cstdint>

namespace nexus {

/// @brief Política de retención de un `PartitionLog`. Afinidad: INMUTABLE.
/// @details Gobierna qué segmentos **sellados** se pueden descartar. La aplica
///   `PartitionLog::enforce_retention`, que nunca borra el segmento activo. La edad de un
///   segmento se toma del *mtime* de su `.log` (los records aún no llevan timestamp; cuando lo
///   lleven, la retención por tiempo podrá usar el timestamp máximo del segmento).
struct RetentionPolicy {
    /// Tamaño total máximo del log en bytes; al superarlo se borran los segmentos más
    /// antiguos. < 0: sin límite por tamaño.
    std::int64_t retention_bytes = -1;
    /// Antigüedad máxima de un segmento en milisegundos. < 0: sin límite por tiempo.
    std::int64_t retention_ms = -1;
};

}  // namespace nexus
