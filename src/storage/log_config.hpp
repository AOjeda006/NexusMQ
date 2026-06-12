/// @file   storage/log_config.hpp
/// @brief  LogConfig: parámetros de un log de partición (tamaño de segmento, índice).
/// @ingroup storage

#pragma once

#include <cstddef>
#include <cstdint>

namespace nexus {

/// @brief Política de `fsync` del log (compromiso durabilidad/latencia). Afinidad: INMUTABLE.
/// @details Determina cuándo se fuerza la persistencia a disco estable. Tras un fallo, solo lo
///   sincronizado (hasta `recovery_point`) está garantizado; el resto puede recuperarse si el
///   SO ya lo volcó, pero no se promete.
enum class FsyncPolicy : std::uint8_t {
    None,      ///< Sin `fsync` explícito: durabilidad solo por el SO. Máximo rendimiento.
    Interval,  ///< `fsync` al acumular `fsync_interval_bytes` desde el último. Equilibrado.
    Commit,    ///< `fsync` en cada `append`: durabilidad por escritura. El más lento.
};

/// @brief Configuración (inmutable) de un `PartitionLog`. Afinidad: INMUTABLE.
/// @details Gobierna la rotación de segmentos, la densidad del índice y la durabilidad. La
///   rotación por tiempo (`segment_ms`) y la retención llegan en M6; se añadirán aquí entonces.
struct LogConfig {
    /// Tamaño máximo del segmento activo: al superarlo, el siguiente `append` rota.
    std::size_t segment_bytes = 64UL * 1024 * 1024;
    /// Bytes de log entre anclas del índice disperso (SparseIndex).
    std::size_t index_interval_bytes = 4096;
    /// Cuándo forzar la durabilidad a disco.
    FsyncPolicy fsync_policy = FsyncPolicy::Interval;
    /// Bytes entre `fsync` bajo `FsyncPolicy::Interval`.
    std::size_t fsync_interval_bytes = 1UL * 1024 * 1024;
};

}  // namespace nexus
