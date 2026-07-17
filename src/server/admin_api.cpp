/// @file   server/admin_api.cpp
/// @brief  Implementación del adaptador AdminApi sobre el broker (ADR-0018).
/// @ingroup server

#include "server/admin_api.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "broker/partition_base.hpp"
#include "broker/topic.hpp"
#include "broker/topic_cluster.hpp"
#include "consensus/raft_carrier.hpp"
#include "consensus/raft_state.hpp"
#include "reactor/cross_core_call.hpp"

namespace nexus {

namespace {

/// Traduce los metadatos del broker al DTO del puerto (traducción en el borde, ADR-0009).
TopicSummary to_summary(const TopicMetadata& meta) {
    return TopicSummary{.name = meta.name,
                        .partition_count = meta.partition_count,
                        .replication_factor = meta.replication_factor,
                        .created_at_ms = meta.created_at_ms};
}

/// Recorta @p items a la página @p page (slice [offset, offset+size)).
template <class T>
std::vector<T> apply_page(std::vector<T> items, Page page) {
    const std::size_t offset = page.offset();
    if (offset >= items.size()) {
        return {};
    }
    const std::size_t end = std::min(items.size(), offset + page.size);
    return {std::make_move_iterator(items.begin() + static_cast<std::ptrdiff_t>(offset)),
            std::make_move_iterator(items.begin() + static_cast<std::ptrdiff_t>(end))};
}

}  // namespace

AdminApi::AdminApi(TopicManager& topics, NodeId node_id, GroupLister group_lister,
                   GroupDescriber group_describer)
    : topics_(topics),
      node_id_(node_id),
      group_lister_(std::move(group_lister)),
      group_describer_(std::move(group_describer)) {}

task<expected<TopicSummary>> AdminApi::create_topic(const CreateTopicSpec& spec) {
    // Validación de nombre centralizada en `TopicManager` (fuente única): REST y protocolo nativo
    // aplican las mismas reglas. El error tipado se traduce aquí a RFC 7807 (InvalidArgument →
    // 400).
    if (const expected<void> valid = TopicManager::validate_topic_name(spec.name); !valid) {
        co_return std::unexpected{valid.error()};
    }
    TopicConfig config;
    if (spec.segment_bytes > 0) {
        config.segment_bytes = static_cast<std::size_t>(spec.segment_bytes);
    }
    config.retention_ms = spec.retention_ms;
    config.retention_bytes = spec.retention_bytes;

    if (partitions_ != nullptr) {
        // Cableado: propaga la creación a todos los núcleos por paso de mensajes (ADR-0026).
        const expected<TopicMetadata> meta = co_await create_topic_on_cluster(
            *self_, *partitions_, topics_by_core_, spec.name, spec.partition_count, config);
        if (!meta) {
            co_return std::unexpected{meta.error()};
        }
        co_return to_summary(*meta);
    }
    const expected<TopicMetadata> meta =
        topics_.create_topic(spec.name, spec.partition_count, config);  // local (N=1 / tests).
    if (!meta) {
        co_return std::unexpected{meta.error()};
    }
    co_return to_summary(*meta);
}

task<expected<void>> AdminApi::delete_topic(std::string_view name) {
    if (partitions_ != nullptr) {
        co_return co_await delete_topic_on_cluster(*self_, *partitions_, topics_by_core_,
                                                   std::string{name});
    }
    co_return topics_.delete_topic(name);  // local (N=1 / tests).
}

task<expected<TopicSummary>> AdminApi::alter_topic_config(std::string_view name,
                                                          const AlterTopicSpec& spec) {
    if (partitions_ != nullptr) {
        // Cableado: publica la config a todos los núcleos por paso de mensajes (ADR-0037).
        const expected<TopicMetadata> meta = co_await update_topic_config_on_cluster(
            *self_, *partitions_, topics_by_core_, std::string{name}, spec.retention_ms,
            spec.retention_bytes);
        if (!meta) {
            co_return std::unexpected{meta.error()};
        }
        co_return to_summary(*meta);
    }
    const expected<TopicMetadata> meta = topics_.update_config(
        name, spec.retention_ms, spec.retention_bytes);  // local (N=1 / tests).
    if (!meta) {
        co_return std::unexpected{meta.error()};
    }
    co_return to_summary(*meta);
}

namespace {

/// Estado de la partición @p pid en @p manager (high-watermark/epoch), o ceros si no vive ahí.
PartitionInfo read_partition_info(TopicManager& manager, const std::string& name, PartitionId pid,
                                  NodeId leader) {
    PartitionInfo info{.id = pid, .leader = leader, .high_watermark = 0, .leader_epoch = 0};
    if (Topic* topic = manager.get(name); topic != nullptr) {
        if (const PartitionBase* partition = topic->partition(pid); partition != nullptr) {
            info.high_watermark = partition->high_watermark();
            info.leader_epoch = partition->leader_epoch();
        }
    }
    return info;
}

}  // namespace

task<PartitionInfo> AdminApi::partition_info(std::string name, PartitionId pid) {
    if (partitions_ == nullptr) {
        co_return read_partition_info(topics_, name, pid, node_id_);  // local (N=1 / tests).
    }
    const int owner = partitions_->owner_core(pid);
    TopicManager* manager = topics_by_core_[static_cast<std::size_t>(owner)];
    const NodeId leader = node_id_;
    // El high-watermark vive en el núcleo dueño: se lee en su hilo y vuelve por el cruce.
    co_return co_await call_on(*self_, partitions_->reactor(owner), [manager, name, pid, leader]() {
        return read_partition_info(*manager, name, pid, leader);
    });
}

task<expected<TopicDescription>> AdminApi::describe_topic(std::string_view name) {
    // El núcleo 0 tiene los metadatos completos (ADR-0026).
    const Topic* topic = topics_.get(name);
    if (topic == nullptr) {
        co_return make_error(ErrorCode::NotFound, "topic inexistente: " + std::string{name});
    }
    TopicDescription description;
    description.summary = to_summary(topic->meta());
    description.retention_ms = topic->meta().config.retention_ms;
    description.retention_bytes = topic->meta().config.retention_bytes;
    description.segment_bytes = static_cast<std::int64_t>(topic->meta().config.segment_bytes);
    const std::int32_t count = topic->meta().partition_count;
    description.partitions.reserve(static_cast<std::size_t>(count));
    const std::string topic_name{name};
    for (PartitionId pid = 0; pid < count; ++pid) {
        description.partitions.push_back(co_await partition_info(topic_name, pid));
    }
    co_return description;
}

std::vector<TopicSummary> AdminApi::list_topics(Page page) const {
    std::vector<TopicMetadata> metas = topics_.list_metadata();
    std::ranges::sort(metas, {}, &TopicMetadata::name);

    std::vector<TopicSummary> summaries;
    summaries.reserve(metas.size());
    for (const TopicMetadata& meta : metas) {
        summaries.push_back(to_summary(meta));
    }
    return apply_page(std::move(summaries), page);
}

task<std::vector<GroupSummary>> AdminApi::list_groups(Page page) {
    if (!group_lister_) {
        co_return std::vector<GroupSummary>{};
    }
    co_return co_await group_lister_(page);
}

task<expected<GroupDescription>> AdminApi::describe_group(std::string_view group_id) {
    if (!group_describer_) {
        co_return make_error(ErrorCode::NotFound, "grupo inexistente: " + std::string{group_id});
    }
    co_return co_await group_describer_(std::string{group_id});
}

namespace {

/// Traduce una observación de Raft (consensus) al DTO del REST admin (traducción en el borde).
PartitionRaftInfo to_raft_info(const RaftObservation& obs) {
    PartitionRaftInfo info;
    info.topic = obs.topic;
    info.partition = obs.partition;
    info.leader = obs.leader_hint.value_or(-1);
    info.role = std::string{raft_role_name(obs.role)};
    info.term = obs.term;
    info.commit_index = obs.commit_index;
    info.last_log_index = obs.last_log_index;
    info.leader_epoch = obs.leader_epoch;
    // El progreso por seguidor solo es significativo en el líder (match_index se sigue ahí).
    if (obs.role == RaftRole::Leader) {
        for (const RaftPeerObservation& peer : obs.peers) {
            const std::int64_t lag =
                obs.last_log_index > peer.match_index ? obs.last_log_index - peer.match_index : 0;
            info.followers.push_back(
                FollowerProgress{.node = peer.peer, .match_index = peer.match_index, .lag = lag});
        }
    }
    return info;
}

/// Observaciones de Raft de las réplicas que atiende @p manager (su núcleo).
std::vector<RaftObservation> observe_carriers(TopicManager& manager) {
    std::vector<RaftObservation> out;
    for (RaftCarrier* carrier : manager.carriers()) {
        out.push_back(carrier->observe());
    }
    return out;
}

/// @brief Sintetiza el DTO de una partición local **no replicada** (single-node/RF=1). Afinidad:
///   REACTOR-LOCAL (lee el log de la partición: debe correr en su núcleo dueño).
/// @details Sin réplicas de Raft no hay elección ni portador: este nodo es el **líder estático** de
///   la partición. Los índices se derivan del log local, honestos: como el *ack* es local, el
///   high-watermark es a la vez `commit_index` y `last_log_index` (todo lo escrito está confirmado
///   y es visible). `term = 0` (no hubo elección de Raft) y `followers` vacío (no hay réplicas).
///   Así la consola muestra el nodo y sus particiones con datos coherentes con el `describe` del
///   topic, aunque la topología sea de un solo nodo ([ADR-0040]).
PartitionRaftInfo to_single_node_info(const std::string& topic, PartitionId pid,
                                      const PartitionBase& part, NodeId node_id) {
    const auto high_watermark = static_cast<std::int64_t>(part.high_watermark());
    PartitionRaftInfo info;
    info.topic = topic;
    info.partition = pid;
    info.leader = node_id;  // único nodo: es el líder de todas sus particiones.
    info.role = std::string{raft_role_name(RaftRole::Leader)};
    info.term = 0;                         // mono-nodo: sin término de elección de Raft.
    info.commit_index = high_watermark;    // ack local: high-watermark == commit index.
    info.last_log_index = high_watermark;  // todo lo escrito está confirmado y visible.
    info.leader_epoch = static_cast<std::int64_t>(part.leader_epoch());
    return info;  // followers vacío: no hay réplicas.
}

/// @brief Estado Raft de **todas** las particiones locales que atiende @p manager, como DTOs.
///   Afinidad: REACTOR-LOCAL (debe correr en el núcleo dueño; lee portadores y logs).
/// @details Unifica los dos casos para que la consola vea la topología completa: las **replicadas**
///   (RF≥2) desde su portador de Raft y las **no replicadas** (single-node/RF=1) sintetizadas como
///   líder estático. Se recorren las particiones declaradas de cada topic y se sintetizan solo las
///   que viven en este núcleo y **no** están respaldadas por Raft (las replicadas ya las da el
///   portador), evitando duplicados.
std::vector<PartitionRaftInfo> observe_partitions(TopicManager& manager, NodeId node_id) {
    std::vector<PartitionRaftInfo> out;
    for (const RaftObservation& obs : observe_carriers(manager)) {
        out.push_back(to_raft_info(obs));
    }
    for (const TopicMetadata& meta : manager.list_metadata()) {
        Topic* topic = manager.get(meta.name);
        if (topic == nullptr) {
            continue;
        }
        for (PartitionId pid = 0; pid < meta.partition_count; ++pid) {
            const PartitionBase* part = topic->partition(pid);
            if (part == nullptr || part->is_replicated()) {
                continue;  // no vive en este núcleo, o ya la cubre el portador de Raft.
            }
            out.push_back(to_single_node_info(meta.name, pid, *part, node_id));
        }
    }
    return out;
}

}  // namespace

task<expected<ClusterInfo>> AdminApi::describe_cluster() {
    ClusterInfo info;
    info.node_id = node_id_;
    for (const NodeId node : cluster_nodes_) {
        info.nodes.push_back(NodeInfo{.node_id = node, .is_self = std::cmp_equal(node, node_id_)});
    }
    if (info.nodes.empty()) {
        info.nodes.push_back(NodeInfo{.node_id = node_id_, .is_self = true});  // nodo aislado.
    }

    if (partitions_ == nullptr) {
        info.partitions = observe_partitions(topics_, node_id_);  // local (N=1 / tests).
    } else {
        // Las particiones viven en el núcleo dueño (ADR-0026): se observan en su hilo, donde se
        // leen tanto los portadores de Raft (replicadas) como los logs locales (single-node).
        const NodeId node_id = node_id_;
        for (int core = 0; core < partitions_->core_count(); ++core) {
            TopicManager* manager = topics_by_core_[static_cast<std::size_t>(core)];
            std::vector<PartitionRaftInfo> part = co_await call_on(
                *self_, partitions_->reactor(core),
                [manager, node_id]() { return observe_partitions(*manager, node_id); });
            for (PartitionRaftInfo& entry : part) {
                info.partitions.push_back(std::move(entry));
            }
        }
    }
    std::ranges::sort(info.partitions, {}, [](const PartitionRaftInfo& part) {
        return std::tie(part.topic, part.partition);
    });
    co_return info;
}

}  // namespace nexus
