/// @file   server/admin_api.hpp
/// @brief  AdminApi: adaptador del puerto `AdminService` sobre el broker (ADR-0018).
/// @ingroup server

#pragma once

#include <functional>
#include <string_view>
#include <vector>

#include "broker/topic_manager.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "ingress/admin_service.hpp"
#include "ingress/pagination.hpp"
#include "reactor/partition_router.hpp"
#include "reactor/reactor.hpp"

namespace nexus {

/// @brief Adaptador concreto de `AdminService` sobre el catálogo de topics y un *group lister*
///   inyectado. Afinidad: REACTOR-LOCAL (se sirve en el núcleo 0, donde se aceptan las conexiones
///   del puerto de operación).
/// @details Traduce los DTOs del puerto (ingress) a/desde los tipos del broker (ADR-0009:
///   traducción en el borde). Crear/borrar topic **propaga** el cambio a todos los núcleos por paso
///   de mensajes (`call_on`, ADR-0026) cuando está cableado al clúster (`bind_cluster`); sin
///   cablear (N=1 o tests) opera sobre el `TopicManager` del núcleo 0. La enumeración de grupos es
///   **reactor-local**, así que se inyecta como función desde el cableado del server (I14) en vez
///   de acoplar el adaptador a los `GroupCoordinator`. En Fase 1b el líder de toda partición es
///   este nodo (`node_id`).
/// @invariant `node_id_` y el *group lister* no cambian tras construir.
class AdminApi final : public AdminService {
public:
    /// Función que enumera los grupos (paginada) **agregando todos los núcleos**; la provee el
    /// cableado del server. Es una corrutina (`task`): la agregación cruza núcleos (`call_on`).
    using GroupLister = std::function<task<std::vector<GroupSummary>>(Page)>;

    AdminApi(TopicManager& topics, NodeId node_id, GroupLister group_lister = {});

    /// @brief Cablea el fan-out cross-core: a partir de aquí, crear/borrar topic se propaga a todos
    ///   los núcleos (ADR-0026) en vez de tocar solo el del núcleo 0.
    /// @param self Reactor donde se sirve el admin (el núcleo 0).
    /// @param partitions Router de particiones del nodo (no propietario; vive más que este).
    /// @param topics_by_core `TopicManager` de cada núcleo, indexado por `core_id`.
    /// @pre `self`, `partitions` y los punteros de `topics_by_core` viven más que este adaptador.
    void bind_cluster(Reactor& self, PartitionRouter& partitions,
                      std::vector<TopicManager*> topics_by_core) noexcept {
        self_ = &self;
        partitions_ = &partitions;
        topics_by_core_ = std::move(topics_by_core);
    }

    [[nodiscard]] task<expected<TopicSummary>> create_topic(const CreateTopicSpec& spec) override;
    [[nodiscard]] task<expected<void>> delete_topic(std::string_view name) override;
    [[nodiscard]] task<expected<TopicDescription>> describe_topic(std::string_view name) override;
    [[nodiscard]] std::vector<TopicSummary> list_topics(Page page) const override;
    [[nodiscard]] task<std::vector<GroupSummary>> list_groups(Page page) override;

private:
    /// @brief Estado de la partición @p pid del topic @p name (id/leader/high-watermark/epoch).
    /// @details El *high-watermark*/epoch viven en el núcleo dueño (`pid % N`): cableado, los lee
    /// por
    ///   `call_on`; sin cablear, localmente. El topic ya existe (lo comprueba `describe_topic`).
    [[nodiscard]] task<PartitionInfo> partition_info(std::string name, PartitionId pid);

    TopicManager& topics_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    NodeId node_id_;
    GroupLister group_lister_;
    /// Fan-out cross-core (cableado por `bind_cluster`; `nullptr` = operación local en el núcleo
    /// 0).
    Reactor* self_ = nullptr;
    PartitionRouter* partitions_ = nullptr;
    /// `TopicManager` por núcleo (indexado por `core_id`); destino del fan-out de crear/borrar.
    std::vector<TopicManager*> topics_by_core_;
};

}  // namespace nexus
