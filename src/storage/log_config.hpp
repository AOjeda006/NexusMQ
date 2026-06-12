/// @file   storage/log_config.hpp
/// @brief  LogConfig: parámetros de un log de partición (tamaño de segmento, índice).
/// @ingroup storage

#pragma once

#include <cstddef>

namespace nexus {

/// @brief Configuración (inmutable) de un `PartitionLog`. Afinidad: INMUTABLE.
/// @details Gobierna la rotación de segmentos y la densidad del índice. La política de
///   `fsync` (`recovery_point`) llega en M5 y la rotación por tiempo (`segment_ms`) y la
///   retención en M6; se añadirán aquí entonces.
struct LogConfig {
    /// Tamaño máximo del segmento activo: al superarlo, el siguiente `append` rota.
    std::size_t segment_bytes = 64UL * 1024 * 1024;
    /// Bytes de log entre anclas del índice disperso (SparseIndex).
    std::size_t index_interval_bytes = 4096;
};

}  // namespace nexus
