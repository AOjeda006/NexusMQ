/// @file   broker/topic_manager.cpp
/// @brief  Implementación de TopicManager (crea topics y abre sus PartitionLog).
/// @ingroup broker

#include "broker/topic_manager.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "broker/partition.hpp"
#include "broker/replicated_partition.hpp"
#include "consensus/raft_carrier.hpp"
#include "consensus/raft_state_store.hpp"
#include "storage/log_config.hpp"
#include "storage/partition_log.hpp"

namespace nexus {

/// @brief Estado por réplica de una partición replicada. Afinidad: REACTOR-LOCAL.
/// @details Posee el almacén durable y el portador, **en ese orden**: el portador se destruye
///   primero (referencia al almacén, al sumidero compartido del manager y al `RaftNode` de su
///   partición), luego el almacén. El sumidero (`raft_sink_`) y la partición (el `RaftNode`, en
///   `topics_`) viven en el `TopicManager` y se destruyen después de `replicas_` (ver el orden de
///   miembros).
struct ReplicaContext {
    std::unique_ptr<RaftStateStore> store;
    std::unique_ptr<RaftCarrier> carrier;
};

namespace {
/// Marca de tiempo de creación en milisegundos desde epoch (reloj de pared).
std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
}  // namespace

TopicManager::TopicManager(std::filesystem::path data_dir, int num_cores, int owner_core,
                           NodeId node_id, RaftConfig raft_config, std::vector<NodeId> voter_peers,
                           CompactionPolicy compaction) noexcept
    : data_dir_(std::move(data_dir)),
      num_cores_(num_cores < 1 ? 1 : num_cores),
      owner_core_(owner_core < 0 || owner_core >= num_cores_ ? 0 : owner_core),
      node_id_(node_id),
      raft_config_(raft_config),
      voter_peers_(std::move(voter_peers)),
      compaction_(compaction) {}

TopicManager::~TopicManager() = default;

expected<TopicMetadata> TopicManager::create_topic(std::string name, std::int32_t partition_count,
                                                   TopicConfig config,
                                                   std::int16_t replication_factor) {
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
    meta.replication_factor = replication_factor;
    meta.config = config;
    meta.created_at_ms = now_ms();

    auto topic = std::make_unique<Topic>(meta);
    const LogConfig log_cfg{.segment_bytes = config.segment_bytes};
    const bool replicated = replication_factor > 1;
    // Portadores acumulados aparte: solo se confían a `replicas_` si el topic se crea entero (si
    // una partición falla, `new_replicas` se destruye —portadores antes que `topic`— sin tocar el
    // estado del manager). Sharding (ADR-0026): este núcleo abre solo sus particiones.
    std::vector<std::unique_ptr<ReplicaContext>> new_replicas;
    for (PartitionId pid = 0; pid < partition_count; ++pid) {
        if (!owns_partition(pid)) {
            continue;
        }
        const std::filesystem::path dir = data_dir_ / name / std::to_string(pid);
        expected<PartitionLog> log = PartitionLog::open(dir, log_cfg);
        if (!log) {
            return std::unexpected(log.error());
        }
        if (!replicated) {
            topic->add_partition(pid, std::make_unique<Partition>(std::move(*log)));
            continue;
        }
        // Partición replicada: el grupo Raft lo forman este nodo y `voter_peers_` (resto del
        // clúster); vacío = votante único. El portador Raft la conduce.
        expected<ReplicatedPartition> rp =
            ReplicatedPartition::create(node_id_, voter_peers_, std::move(*log), raft_config_);
        if (!rp) {
            return std::unexpected(rp.error());
        }
        auto rp_ptr = std::make_unique<ReplicatedPartition>(std::move(*rp));
        ReplicatedPartition* raw =
            rp_ptr.get();  // estable: la posee el `Topic` (heap), no se mueve
        topic->add_partition(pid, std::move(rp_ptr));

        expected<RaftStateStore> store = RaftStateStore::open((dir / "raft-state").string());
        if (!store) {
            return std::unexpected(store.error());
        }
        auto ctx = std::make_unique<ReplicaContext>();
        ctx->store = std::make_unique<RaftStateStore>(std::move(*store));
        // Todos los portadores del núcleo comparten `raft_sink_` (reenvía al transporte real cuando
        // se instale; hasta entonces descarta: votante único sin transporte).
        ctx->carrier = std::make_unique<RaftCarrier>(
            name, pid, raw->raft(), raft_sink_, ctx->store.get(), &raw->raft_log(), compaction_);
        if (const expected<void> recovered = ctx->carrier->recover(); !recovered) {
            return std::unexpected(recovered.error());
        }
        new_replicas.push_back(std::move(ctx));
    }

    topics_.emplace(std::move(name), std::move(topic));
    for (auto& ctx : new_replicas) {
        replicas_.push_back(std::move(ctx));
    }
    return meta;
}

std::vector<RaftCarrier*> TopicManager::carriers() const {
    const std::scoped_lock lock{mutex_};
    std::vector<RaftCarrier*> result;
    result.reserve(replicas_.size());
    for (const std::unique_ptr<ReplicaContext>& ctx : replicas_) {
        result.push_back(ctx->carrier.get());
    }
    return result;
}

RaftCarrier* TopicManager::carrier_for(std::string_view topic, PartitionId partition) const {
    const std::scoped_lock lock{mutex_};
    for (const std::unique_ptr<ReplicaContext>& ctx : replicas_) {
        if (ctx->carrier->partition() == partition && ctx->carrier->topic() == topic) {
            return ctx->carrier.get();
        }
    }
    return nullptr;
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

std::vector<TopicMetadata> TopicManager::list_metadata() const {
    const std::scoped_lock lock{mutex_};
    std::vector<TopicMetadata> result;
    result.reserve(topics_.size());
    for (const auto& [name, topic] : topics_) {
        result.push_back(topic->meta());
    }
    return result;
}

std::size_t TopicManager::topic_count() const {
    const std::scoped_lock lock{mutex_};
    return topics_.size();
}

}  // namespace nexus
