/// @file   server/admin_api.cpp
/// @brief  Implementación del adaptador AdminApi sobre el broker (ADR-0018).
/// @ingroup server

#include "server/admin_api.hpp"

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>

#include "broker/partition.hpp"
#include "broker/topic.hpp"
#include "broker/topic_cluster.hpp"

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

AdminApi::AdminApi(TopicManager& topics, NodeId node_id, GroupLister group_lister)
    : topics_(topics), node_id_(node_id), group_lister_(std::move(group_lister)) {}

task<expected<TopicSummary>> AdminApi::create_topic(const CreateTopicSpec& spec) {
    if (spec.name.empty()) {
        co_return make_error(ErrorCode::InvalidArgument,
                             "el nombre del topic no puede estar vacío");
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

expected<TopicDescription> AdminApi::describe_topic(std::string_view name) const {
    Topic* topic = topics_.get(name);
    if (topic == nullptr) {
        return make_error(ErrorCode::NotFound, "topic inexistente: " + std::string{name});
    }
    TopicDescription description;
    description.summary = to_summary(topic->meta());
    const std::int32_t count = topic->meta().partition_count;
    description.partitions.reserve(static_cast<std::size_t>(count));
    for (PartitionId pid = 0; pid < count; ++pid) {
        PartitionInfo info{.id = pid, .leader = node_id_, .high_watermark = 0, .leader_epoch = 0};
        if (const Partition* partition = topic->partition(pid); partition != nullptr) {
            info.high_watermark = partition->high_watermark();
            info.leader_epoch = partition->leader_epoch();
        }
        description.partitions.push_back(info);
    }
    return description;
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

std::vector<GroupSummary> AdminApi::list_groups(Page page) const {
    if (!group_lister_) {
        return {};
    }
    return group_lister_(page);
}

}  // namespace nexus
