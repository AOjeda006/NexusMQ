/// @file   broker/topic_manager.hpp
/// @brief  TopicManager: crea/borra topics y abre sus particiones (plano de control).
/// @ingroup broker

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "broker/topic.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "protocol/messages.hpp"

namespace nexus {

/// @brief Gestiona el ciclo de vida de los topics del nodo (plano de control). Afinidad:
///   THREAD-SAFE (un mutex protege el mapa; las creaciones/borrados son poco frecuentes).
/// @details Cada topic vive en `data_dir/<nombre>/<partición>/` (un `PartitionLog` por partición).
///   **Sharding por núcleo (ADR-0026):** una instancia atiende un único núcleo y abre **solo** las
///   particiones que le tocan (`partition % num_cores == owner_core`); con `num_cores == 1`
///   (por defecto) abre todas (equivalente al mono-reactor). Los **metadatos** del topic (nombre,
///   `partition_count`) se registran completos en cada instancia, de modo que `describe`/`Metadata`
///   y la validación son locales sin cross-core. `get` devuelve un puntero válido mientras el topic
///   no se borre (los topics se crean al arrancar, antes de servir).
class TopicManager {
public:
    /// @param data_dir Raíz de los logs de partición.
    /// @param num_cores Número de núcleos del nodo (>= 1; valores < 1 se tratan como 1).
    /// @param owner_core Núcleo que atiende esta instancia (se acota a `[0, num_cores)`).
    explicit TopicManager(std::filesystem::path data_dir, int num_cores = 1,
                          int owner_core = 0) noexcept
        : data_dir_(std::move(data_dir)),
          num_cores_(num_cores < 1 ? 1 : num_cores),
          owner_core_(owner_core < 0 || owner_core >= num_cores_ ? 0 : owner_core) {}
    TopicManager(const TopicManager&) = delete;
    TopicManager& operator=(const TopicManager&) = delete;
    TopicManager(TopicManager&&) = delete;
    TopicManager& operator=(TopicManager&&) = delete;
    ~TopicManager() = default;

    /// @brief Crea un topic con @p partition_count particiones (abre un log por cada una).
    /// @return Los metadatos del topic, o un error (`InvalidArgument` si ya existe o el conteo es
    ///   inválido; error de E/S si no se puede abrir algún log).
    [[nodiscard]] expected<TopicMetadata> create_topic(std::string name,
                                                       std::int32_t partition_count,
                                                       TopicConfig config = {});

    /// Borra un topic del registro (los ficheros en disco se conservan). `NotFound` si no existe.
    [[nodiscard]] expected<void> delete_topic(std::string_view name);

    /// @return El topic @p name, o `nullptr` si no existe.
    [[nodiscard]] Topic* get(std::string_view name);

    /// @brief ¿Atiende este núcleo la partición @p pid? (`pid % num_cores == owner_core`,
    /// ADR-0026).
    /// @details Misma regla que `PartitionRouter::owner_core`; el router decide local vs
    /// cross-core.
    [[nodiscard]] bool owns_partition(PartitionId pid) const noexcept {
        return static_cast<int>(static_cast<std::size_t>(pid) %
                                static_cast<std::size_t>(num_cores_)) == owner_core_;
    }

    /// Número de núcleos del nodo con el que se construyó (para el reparto de particiones).
    [[nodiscard]] int num_cores() const noexcept { return num_cores_; }
    /// Núcleo que atiende esta instancia.
    [[nodiscard]] int owner_core() const noexcept { return owner_core_; }

    /// Construye los `TopicMeta` para una `MetadataResponse` (líder = @p leader_node_id en 1b).
    [[nodiscard]] std::vector<TopicMeta> describe(NodeId leader_node_id) const;

    /// @brief Devuelve los metadatos de todos los topics (control-plane; admin/observabilidad).
    [[nodiscard]] std::vector<TopicMetadata> list_metadata() const;

    [[nodiscard]] std::size_t topic_count() const;

private:
    std::filesystem::path data_dir_;
    int num_cores_;   ///< Núcleos del nodo (>= 1).
    int owner_core_;  ///< Núcleo que atiende esta instancia (`[0, num_cores_)`).
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<Topic>> topics_;
};

}  // namespace nexus
