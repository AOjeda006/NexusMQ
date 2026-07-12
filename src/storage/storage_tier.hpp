/// @file   storage/storage_tier.hpp
/// @brief  Puerto `StorageTier` + DTO de clave de objeto: contrato de almacenamiento por niveles.
/// @ingroup storage

#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>

#include "common/error.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Tipo de fichero de un segmento (las dos piezas que se pueden mover al tier).
/// @details Un segmento son dos ficheros hermanos: el `.log` (los batches) y el `.index` (índice
///   disperso). El tier los guarda como objetos independientes bajo la misma identidad de segmento.
enum class SegmentFileKind : std::uint8_t {
    Log,    ///< Fichero `.log` (los `RecordBatch`, cifrados o en claro).
    Index,  ///< Fichero `.index` (índice disperso; reconstruible, pero se mueve junto al `.log`).
};

/// @brief Identidad de un objeto en el tier: partición (topic + id) + segmento (offset base) +
/// tipo.
///   Afinidad: INMUTABLE (valor).
/// @details DTO plano y **sin dependencia del broker**. `encode()` produce una clave de objeto
///   estable y jerárquica (`<topic>/<partition>/<base:020>.<ext>`) que refleja el layout del
///   `--data-dir`, de modo que el tier local es un espejo directorio-a-directorio y un tier de
///   objetos (S3, futuro) usa la misma clave como *object key*. La clave es determinista: el mismo
///   segmento produce siempre la misma clave, lo que hace el offload **idempotente**.
struct TierObjectKey {
    std::string topic;                            ///< Nombre del topic (no vacío, sin `/`).
    std::int32_t partition = 0;                   ///< Id de partición (>= 0).
    Offset base_offset = 0;                       ///< Offset base del segmento (>= 0).
    SegmentFileKind kind = SegmentFileKind::Log;  ///< Pieza del segmento (`.log` / `.index`).

    /// @brief Serializa la clave a `<topic>/<partition>/<base:020>.<ext>` (ext = `log`/`index`).
    [[nodiscard]] std::string encode() const;

    /// @brief Parsea una clave `encode()`ada de vuelta a un `TierObjectKey`.
    /// @return La clave, o `InvalidArgument` si el formato/partición/offset/extensión no son
    /// válidos.
    [[nodiscard]] static expected<TierObjectKey> decode(std::string_view key);

    [[nodiscard]] bool operator==(const TierObjectKey&) const = default;
};

/// @brief Puerto de almacenamiento por niveles (*tiered storage*, ADR-0032). Afinidad: THREAD-SAFE
///   (contrato).
/// @details Almacén de objetos, orientado a **fichero**, donde el broker descarga los segmentos
///   **sellados** para retención larga a bajo coste, reclamando el espacio local solo **tras
///   confirmar** la subida. Es un **puerto** (inversión de dependencias): el `PartitionLog` (y su
///   política) dependen de esta interfaz, no de una nube concreta. El adaptador por defecto
///   (`LocalStorageTier`) copia a un directorio objeto; un adaptador S3 (futuro) implementaría el
///   mismo contrato. Sin tier configurado, el broker se comporta como hoy (degradación limpia).
///
///   Es orientado a fichero (`put_file`/`fetch_file`) porque un segmento **es** un fichero: subir
///   y rehidratar son copias, sin cargar segmentos enteros en memoria. Las claves de objetos de
///   particiones distintas nunca colisionan (identidad topic/partición/offset), así que las
///   operaciones sobre claves distintas son seguras desde núcleos distintos.
/// @invariant `put_file` es idempotente (misma clave ⇒ mismo objeto); `remove`/`fetch` sobre una
///   clave inexistente dan, respectivamente, éxito (idempotente) y `NotFound`.
class StorageTier {
public:
    StorageTier() = default;
    StorageTier(const StorageTier&) = delete;
    StorageTier& operator=(const StorageTier&) = delete;
    StorageTier(StorageTier&&) = delete;
    StorageTier& operator=(StorageTier&&) = delete;
    virtual ~StorageTier() = default;

    /// @brief Sube a @p key el contenido del fichero local @p source (offload de un segmento).
    /// @details **Idempotente y atómico**: reescribir la misma clave deja el objeto íntegro (nunca
    ///   a medias). Sube los bytes **tal cual** (si el `.log` está cifrado, sube el ciphertext;
    ///   interopera con ADR-0031 sin descifrar). `NotFound` si @p source no existe; `IoError`/
    ///   `OutOfSpace` ante fallo de E/S.
    [[nodiscard]] virtual expected<void> put_file(const TierObjectKey& key,
                                                  const std::filesystem::path& source) = 0;

    /// @brief Descarga @p key al fichero local @p dest (rehidratación para lectura transparente).
    /// @details Crea/sobrescribe @p dest atómicamente con los bytes del objeto. `NotFound` si @p
    /// key
    ///   no está en el tier; `IoError`/`OutOfSpace` ante fallo de E/S.
    [[nodiscard]] virtual expected<void> fetch_file(const TierObjectKey& key,
                                                    const std::filesystem::path& dest) const = 0;

    /// @brief ¿Está @p key en el tier? (confirmación de offload / idempotencia). `IoError` ante
    ///   fallo del backend.
    [[nodiscard]] virtual expected<bool> contains(const TierObjectKey& key) const = 0;

    /// @brief Tamaño en bytes del objeto @p key. `NotFound` si no existe.
    [[nodiscard]] virtual expected<std::uint64_t> object_size(const TierObjectKey& key) const = 0;

    /// @brief Borra @p key del tier. **Idempotente**: no falla si no existe. `IoError` ante fallo.
    [[nodiscard]] virtual expected<void> remove(const TierObjectKey& key) = 0;
};

}  // namespace nexus
