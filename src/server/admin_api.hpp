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

namespace nexus {

/// @brief Adaptador concreto de `AdminService` sobre `TopicManager` y un *group lister* inyectado.
/// @details Afinidad: THREAD-SAFE (delega en `TopicManager`, que tiene su mutex; el *group lister*
///   debe ser thread-safe). Traduce los DTOs del puerto (ingress) a/desde los tipos del broker
///   (ADR-0009: traducción en el borde). La enumeración de grupos es **reactor-local**, así que se
///   inyecta como función desde el cableado del server (I14) en vez de acoplar el adaptador a los
///   `GroupCoordinator`. En Fase 1b el líder de toda partición es este nodo (`node_id`).
/// @invariant `node_id_` y el *group lister* no cambian tras construir.
class AdminApi final : public AdminService {
public:
    /// Función que enumera los grupos (paginada); la provee el cableado del server.
    using GroupLister = std::function<std::vector<GroupSummary>(Page)>;

    AdminApi(TopicManager& topics, NodeId node_id, GroupLister group_lister = {});

    [[nodiscard]] task<expected<TopicSummary>> create_topic(const CreateTopicSpec& spec) override;
    [[nodiscard]] task<expected<void>> delete_topic(std::string_view name) override;
    [[nodiscard]] expected<TopicDescription> describe_topic(std::string_view name) const override;
    [[nodiscard]] std::vector<TopicSummary> list_topics(Page page) const override;
    [[nodiscard]] std::vector<GroupSummary> list_groups(Page page) const override;

private:
    TopicManager& topics_;
    NodeId node_id_;
    GroupLister group_lister_;
};

}  // namespace nexus
