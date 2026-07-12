/// @file   storage/local_storage_tier.hpp
/// @brief  LocalStorageTier: adaptador de `StorageTier` sobre un directorio objeto (ADR-0032).
/// @ingroup storage

#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <string_view>
#include <vector>

#include "common/error.hpp"
#include "common/types.hpp"
#include "storage/storage_tier.hpp"

namespace nexus {

/// @brief Adaptador por defecto de `StorageTier`: copia los objetos a un **directorio objeto**
///   local. Afinidad: THREAD-SAFE.
/// @details Materializa el puerto (ADR-0032) sin dependencia de nube: cada objeto es un fichero
/// bajo
///   `object_dir/<topic>/<partition>/<base:020>.<ext>` (la clave `encode()`ada), de modo que el
///   tier es un **espejo** del `--data-dir`. Es el sustituto de coste cero de un tier S3 (futuro,
///   `find_package` opcional), útil para validar todo el ciclo offload→reclamar→rehidratar en
///   local.
///
///   Las escrituras (`put_file`/`fetch_file`) son **atómicas**: copian a un fichero temporal
///   hermano y hacen `rename` sobre el destino, así un fallo a media copia nunca deja un objeto
///   truncado. Un lector jamás observa un objeto a medias. Los nombres temporales son únicos
///   (contador atómico), de modo que puts concurrentes no se pisan.
/// @invariant `object_dir()` es la raíz de todos los objetos; no cambia tras construir.
class LocalStorageTier : public StorageTier {
public:
    /// @brief Construye el tier con raíz @p object_dir (se crea al primer `put_file`).
    explicit LocalStorageTier(std::filesystem::path object_dir);

    [[nodiscard]] expected<void> put_file(const TierObjectKey& key,
                                          const std::filesystem::path& source) override;
    [[nodiscard]] expected<void> fetch_file(const TierObjectKey& key,
                                            const std::filesystem::path& dest) const override;
    [[nodiscard]] expected<bool> contains(const TierObjectKey& key) const override;
    [[nodiscard]] expected<std::uint64_t> object_size(const TierObjectKey& key) const override;
    [[nodiscard]] expected<void> remove(const TierObjectKey& key) override;
    [[nodiscard]] expected<std::vector<Offset>> list_segment_bases(
        std::string_view topic, std::int32_t partition) const override;

    /// Raíz del directorio objeto (observabilidad / pruebas).
    [[nodiscard]] const std::filesystem::path& object_dir() const noexcept { return object_dir_; }

private:
    /// Ruta absoluta del objeto de @p key bajo la raíz.
    [[nodiscard]] std::filesystem::path path_of(const TierObjectKey& key) const;
    /// Copia @p from a @p to de forma atómica (temporal hermano + `rename`).
    [[nodiscard]] expected<void> atomic_copy(const std::filesystem::path& from,
                                             const std::filesystem::path& to) const;

    std::filesystem::path object_dir_;                ///< Raíz de los objetos.
    mutable std::atomic<std::uint64_t> temp_seq_{0};  ///< Secuencia de nombres temporales únicos.
};

}  // namespace nexus
