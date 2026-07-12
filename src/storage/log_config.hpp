/// @file   storage/log_config.hpp
/// @brief  LogConfig: parámetros de un log de partición (tamaño de segmento, índice).
/// @ingroup storage

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace nexus {

class EncryptionKey;  // storage/segment_crypto.hpp (forward: solo se guarda un shared_ptr).
class StorageTier;  // storage/storage_tier.hpp (forward: solo se guarda un puntero no-propietario).

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
    /// Clave maestra de **cifrado en reposo** (ADR-0031). Compartida por todo el broker; si es
    /// `nullptr`, los segmentos se escriben en claro (comportamiento por defecto). Al crear un
    /// segmento con clave, se cifra con AES-256-GCM; al abrirlo, se descifra de forma transparente.
    std::shared_ptr<const EncryptionKey> encryption_key;

    /// **Almacenamiento por niveles** (ADR-0032): destino de la descarga de segmentos sellados.
    /// Puntero **no-propietario** (el tier lo posee el composition root, compartido por el nodo);
    /// si es `nullptr`, no hay tiering y el log se comporta como hoy (degradación limpia). La
    /// identidad de la partición (`tier_topic`/`tier_partition`) forma la clave de objeto en el
    /// tier.
    StorageTier* tier = nullptr;
    std::string tier_topic;           ///< Nombre del topic (clave de objeto del tier).
    std::int32_t tier_partition = 0;  ///< Id de partición (clave de objeto del tier).
};

}  // namespace nexus
