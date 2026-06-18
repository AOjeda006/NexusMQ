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
///   En Fase 1b (mono-nodo) todas las particiones las sirve este nodo; la asignación a núcleos
///   (routing cross-core) llega con el multi-reactor. `get` devuelve un puntero válido mientras el
///   topic no se borre (en 1b los topics se crean al arrancar, antes de servir).
class TopicManager {
public:
    explicit TopicManager(std::filesystem::path data_dir) noexcept
        : data_dir_(std::move(data_dir)) {}
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

    /// Construye los `TopicMeta` para una `MetadataResponse` (líder = @p leader_node_id en 1b).
    [[nodiscard]] std::vector<TopicMeta> describe(NodeId leader_node_id) const;

    /// @brief Devuelve los metadatos de todos los topics (control-plane; admin/observabilidad).
    [[nodiscard]] std::vector<TopicMetadata> list_metadata() const;

    [[nodiscard]] std::size_t topic_count() const;

private:
    std::filesystem::path data_dir_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<Topic>> topics_;
};

}  // namespace nexus
