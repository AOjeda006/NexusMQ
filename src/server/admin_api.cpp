/// @file   server/admin_api.cpp
/// @brief  Implementación del adaptador AdminApi sobre el broker (ADR-0018).
/// @ingroup server

#include "server/admin_api.hpp"

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>

#include "broker/partition_base.hpp"
#include "broker/topic.hpp"
#include "broker/topic_cluster.hpp"
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

}  // namespace nexus
