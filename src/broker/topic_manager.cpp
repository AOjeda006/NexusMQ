/// @file   broker/topic_manager.cpp
/// @brief  Implementación de TopicManager (crea topics y abre sus PartitionLog).
/// @ingroup broker

#include "broker/topic_manager.hpp"

#include <chrono>
#include <utility>

#include "broker/partition.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace nexus {

namespace {
/// Marca de tiempo de creación en milisegundos desde epoch (reloj de pared).
std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
}  // namespace

expected<TopicMetadata> TopicManager::create_topic(std::string name, std::int32_t partition_count,
                                                   TopicConfig config) {
    if (partition_count < 1) {
        return make_error(ErrorCode::InvalidArgument, "partition_count debe ser >= 1");
    }
    const std::scoped_lock lock{mutex_};
    if (topics_.contains(name)) {
        return make_error(ErrorCode::InvalidArgument, "el topic ya existe: " + name);
    }

    TopicMetadata meta;
    meta.name = name;
    meta.partition_count = partition_count;
    meta.config = config;
    meta.created_at_ms = now_ms();

    auto topic = std::make_unique<Topic>(meta);
    const LogConfig log_cfg{.segment_bytes = config.segment_bytes};
    for (PartitionId pid = 0; pid < partition_count; ++pid) {
        const std::filesystem::path dir = data_dir_ / name / std::to_string(pid);
        expected<PartitionLog> log = PartitionLog::open(dir, log_cfg);
        if (!log) {
            return std::unexpected(log.error());
        }
        topic->add_partition(pid, std::make_unique<Partition>(std::move(*log)));
    }

    topics_.emplace(std::move(name), std::move(topic));
    return meta;
}

expected<void> TopicManager::delete_topic(std::string_view name) {
    const std::scoped_lock lock{mutex_};
    const auto it = topics_.find(std::string{name});
    if (it == topics_.end()) {
        return make_error(ErrorCode::NotFound, "topic inexistente");
    }
    topics_.erase(it);
    return {};
}

Topic* TopicManager::get(std::string_view name) {
    const std::scoped_lock lock{mutex_};
    const auto it = topics_.find(std::string{name});
    return it == topics_.end() ? nullptr : it->second.get();
}

std::vector<TopicMeta> TopicManager::describe(NodeId leader_node_id) const {
    const std::scoped_lock lock{mutex_};
    std::vector<TopicMeta> result;
    result.reserve(topics_.size());
    for (const auto& [name, topic] : topics_) {
        TopicMeta topic_meta;
        topic_meta.name = name;
        const std::int32_t count = topic->meta().partition_count;
        topic_meta.partitions.reserve(static_cast<std::size_t>(count));
        for (PartitionId pid = 0; pid < count; ++pid) {
            topic_meta.partitions.push_back(PartitionMeta{.id = pid,
                                                          .leader_node_id = leader_node_id,
                                                          .replicas = {leader_node_id},
                                                          .leader_epoch = 0});
        }
        result.push_back(std::move(topic_meta));
    }
    return result;
}

std::size_t TopicManager::topic_count() const {
    const std::scoped_lock lock{mutex_};
    return topics_.size();
}

}  // namespace nexus
