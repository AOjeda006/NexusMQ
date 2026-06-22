/// @file   broker/request_router.cpp
/// @brief  Implementación de RequestRouter (despacho protocolo↔dominio del broker).
/// @ingroup broker

#include "broker/request_router.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "broker/consumer_group.hpp"
#include "broker/group_coordinator.hpp"
#include "broker/offset_manager.hpp"
#include "broker/partition_base.hpp"
#include "broker/topic.hpp"
#include "broker/topic_cluster.hpp"
#include "common/fnv1a.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "protocol/codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/messages.hpp"
#include "reactor/cross_core_call.hpp"
#include "reactor/partition_router.hpp"

namespace nexus {

namespace {

/// Tope de lectura por defecto cuando el cliente no acota `max_bytes` (anti-respuesta-gigante).
constexpr std::size_t kDefaultFetchBytes = 1UL * 1024 * 1024;

/// @brief Ejecuta @p fn sobre el shard (`GroupCoordinator`/`OffsetManager`) del **núcleo
///   coordinador** de @p group (= `fnv1a_64(group) % N`, ADR-0026) por paso de mensajes y reanuda
///   al llamante con la respuesta que devuelve. Sin cablear (@p partitions `nullptr`, tests)
///   ejecuta sobre @p local en el propio hilo.
/// @details @p fn corre **en el hilo del núcleo coordinador**: solo debe tocar su shard (o estado
///   inmutable). El shard se resuelve aquí y se captura por referencia (vive en el `GroupCatalog`,
///   estable); @p fn (que captura la petición decodificada por valor) se mueve al *frame*.
template <class Shard, class Fn>
task<std::invoke_result_t<Fn, Shard&>> on_group_owner(Reactor* self, PartitionRouter* partitions,
                                                      std::span<Shard* const> by_core, Shard& local,
                                                      std::string_view group, Fn fn) {
    if (partitions == nullptr) {
        co_return fn(local);  // sin cablear (tests): operación local en el propio hilo.
    }
    const auto cores = static_cast<std::uint64_t>(partitions->core_count());
    const auto owner = static_cast<std::size_t>(fnv1a_64(group) % cores);
    Shard& shard = *by_core[owner];
    co_return co_await call_on(*self, partitions->reactor(static_cast<int>(owner)),
                               [&shard, fn = std::move(fn)]() mutable { return fn(shard); });
}

/// Localiza la partición destino; `nullptr` si el topic o la partición no existen.
PartitionBase* find_partition(TopicManager& topics, const std::string& topic,
                              PartitionId partition) {
    Topic* found = topics.get(topic);
    return found == nullptr ? nullptr : found->partition(partition);
}

ProduceResponse handle_produce(TopicManager& topics, const ProduceRequest& req) {
    ProduceResponse resp;
    PartitionBase* part = find_partition(topics, req.topic, req.partition);
    if (part == nullptr) {
        resp.error_code = WireError::UnknownTopicOrPartition;
        return resp;
    }
    const expected<RecordBatch> batch = RecordBatch::decode(req.batch);
    if (!batch) {
        resp.error_code = from_error(batch.error());
        return resp;
    }
    const expected<Offset> last = part->produce(*batch);
    if (!last) {
        resp.error_code = from_error(last.error());
        return resp;
    }
    resp.base_offset = *last - batch->header().record_count + 1;
    return resp;
}

/// Resultado de una lectura ya resuelta sobre la partición dueña: posee los bytes para que se
/// codifiquen en el núcleo de la conexión (se mueve a través del cruce de núcleos).
struct FetchOutcome {
    WireError error = WireError::None;
    FetchResult result;  ///< Bytes de los batches (vista zero-copy mientras viva el outcome).
    Offset high_watermark = 0;
    Offset log_start_offset = 0;
};

FetchOutcome handle_fetch(TopicManager& topics, const FetchRequest& req) {
    FetchOutcome out;
    const PartitionBase* part = find_partition(topics, req.topic, req.partition);
    if (part == nullptr) {
        out.error = WireError::UnknownTopicOrPartition;
        return out;
    }
    const std::size_t max_bytes =
        req.max_bytes > 0 ? static_cast<std::size_t>(req.max_bytes) : kDefaultFetchBytes;
    expected<FetchResult> result = part->fetch(req.fetch_offset, max_bytes);
    if (!result) {
        out.error = from_error(result.error());
        return out;
    }
    out.result = std::move(*result);
    out.high_watermark = part->high_watermark();
    out.log_start_offset = part->log().log_start_offset();
    return out;
}

OffsetCommitResponse handle_offset_commit(OffsetManager& offsets, const OffsetCommitRequest& req) {
    offsets.commit(req.group, req.topic, req.partition, req.offset, req.metadata);
    return OffsetCommitResponse{};
}

OffsetFetchResponse handle_offset_fetch(const OffsetManager& offsets,
                                        const OffsetFetchRequest& req) {
    OffsetFetchResponse resp;
    if (const expected<Offset> offset = offsets.fetch(req.group, req.topic, req.partition);
        offset) {
        resp.offset = *offset;
    } else {
        resp.offset = -1;  // Sin commit previo: no es error; el cliente decide el inicio.
    }
    return resp;
}

JoinGroupResponse handle_join_group(GroupCoordinator& groups, MonoTime now, JoinGroupRequest req) {
    JoinGroupResponse resp;
    const expected<JoinResult> result =
        groups.join(now, req.group, std::move(req.member_id), std::move(req.subscription),
                    std::chrono::milliseconds{req.session_timeout_ms});
    if (!result) {
        resp.error_code = from_error(result.error());
        return resp;
    }
    resp.generation = result->generation;
    resp.member_id = result->member_id;
    resp.leader_id = result->leader_id;
    resp.is_leader = result->is_leader;
    resp.members.reserve(result->members.size());
    for (const MemberInfo& member : result->members) {
        resp.members.push_back(
            GroupMember{.member_id = member.member_id, .subscription = member.subscription});
    }
    return resp;
}

SyncGroupResponse handle_sync_group(GroupCoordinator& groups, MonoTime now,
                                    const SyncGroupRequest& req) {
    SyncGroupResponse resp;
    std::vector<MemberAssignment> assignments;
    assignments.reserve(req.assignments.size());
    for (const GroupAssignment& entry : req.assignments) {
        assignments.push_back(
            MemberAssignment{.member_id = entry.member_id, .assignment = entry.assignment});
    }
    const expected<SyncResult> result =
        groups.sync(now, req.group, req.member_id, req.generation, assignments);
    if (!result) {
        resp.error_code = from_error(result.error());
        return resp;
    }
    if (!result->assigned) {
        resp.error_code = WireError::RebalanceInProgress;  // el líder aún no repartió: reintentar.
        return resp;
    }
    resp.assignment = result->assignment;
    return resp;
}

HeartbeatResponse handle_heartbeat(GroupCoordinator& groups, MonoTime now,
                                   const HeartbeatRequest& req) {
    HeartbeatResponse resp;
    const expected<HeartbeatStatus> status =
        groups.heartbeat(now, req.group, req.member_id, req.generation);
    if (!status) {
        resp.error_code = from_error(status.error());
        return resp;
    }
    if (*status == HeartbeatStatus::RebalanceInProgress) {
        resp.error_code = WireError::RebalanceInProgress;
    }
    return resp;
}

LeaveGroupResponse handle_leave_group(GroupCoordinator& groups, const LeaveGroupRequest& req) {
    LeaveGroupResponse resp;
    if (const expected<void> left = groups.leave(req.group, req.member_id); !left) {
        resp.error_code = from_error(left.error());
    }
    return resp;
}

MetadataResponse handle_metadata(TopicManager& topics, NodeId node_id, const std::string& host,
                                 std::uint16_t port, const MetadataRequest& req) {
    MetadataResponse resp;
    resp.brokers.push_back(BrokerMeta{.node_id = node_id, .host = host, .port = port});
    std::vector<TopicMeta> all = topics.describe(node_id);
    if (req.topics.empty()) {
        resp.topics = std::move(all);
        return resp;
    }
    // Filtra por los topics pedidos; los inexistentes se devuelven con su error.
    for (const std::string& wanted : req.topics) {
        bool matched = false;
        for (const TopicMeta& meta : all) {
            if (meta.name == wanted) {
                resp.topics.push_back(meta);
                matched = true;
                break;
            }
        }
        if (!matched) {
            resp.topics.push_back(TopicMeta{
                .name = wanted, .error = WireError::UnknownTopicOrPartition, .partitions = {}});
        }
    }
    return resp;
}

}  // namespace

std::vector<ApiVersionRange> RequestRouter::supported_versions() {
    return {
        ApiVersionRange{.key = ApiKey::ApiVersions, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::Metadata, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::Produce, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::Fetch, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::CreateTopic, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::DeleteTopic, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::OffsetCommit, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::OffsetFetch, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::JoinGroup, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::SyncGroup, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::Heartbeat, .min = 0, .max = 0},
        ApiVersionRange{.key = ApiKey::LeaveGroup, .min = 0, .max = 0},
    };
}

task<expected<void>> RequestRouter::create_topic_cluster(const std::string& name,
                                                         std::int32_t partition_count) {
    if (partitions_ == nullptr) {
        // Sin cablear (tests unitarios): creación local sobre el único `TopicManager`.
        const expected<TopicMetadata> meta = topics_.create_topic(name, partition_count);
        if (!meta) {
            co_return std::unexpected(meta.error());
        }
        co_return expected<void>{};
    }
    // Cableado: propaga a todos los núcleos por paso de mensajes (helper compartido, ADR-0026).
    const expected<TopicMetadata> meta = co_await create_topic_on_cluster(
        *self_, *partitions_, topics_by_core_, name, partition_count);
    if (!meta) {
        co_return std::unexpected(meta.error());
    }
    co_return expected<void>{};
}

task<expected<void>> RequestRouter::delete_topic_cluster(const std::string& name) {
    if (partitions_ == nullptr) {
        co_return topics_.delete_topic(name);  // sin cablear (tests): borrado local.
    }
    co_return co_await delete_topic_on_cluster(*self_, *partitions_, topics_by_core_, name);
}

task<expected<void>> RequestRouter::dispatch(ApiKey key, std::uint16_t /*api_version*/,
                                             Decoder& body, Buffer& out) {
    Encoder enc{out};
    switch (key) {
        case ApiKey::ApiVersions: {
            const ApiVersionsResponse resp{.ranges = supported_versions()};
            resp.encode(enc);
            co_return expected<void>{};
        }
        case ApiKey::Metadata: {
            const expected<MetadataRequest> req = MetadataRequest::decode(body);
            const MetadataResponse resp =
                req ? handle_metadata(topics_, node_id_, host_, port_, *req) : MetadataResponse{};
            resp.encode(enc);
            co_return expected<void>{};
        }
        case ApiKey::Produce: {
            const expected<ProduceRequest> req = ProduceRequest::decode(body);
            ProduceResponse resp;
            if (!req) {
                resp.error_code = WireError::InvalidRequest;
            } else if (partitions_ != nullptr) {
                // Enruta al reactor dueño de la partición; opera sobre su `TopicManager`.
                const int owner = partitions_->owner_core(req->partition);
                resp = co_await partitions_->route(*self_, req->partition, [this, owner, &req] {
                    return handle_produce(*topics_by_core_[static_cast<std::size_t>(owner)], *req);
                });
            } else {
                resp = handle_produce(topics_, *req);  // sin cablear: local (tests).
            }
            resp.encode(enc);
            co_return expected<void>{};
        }
        case ApiKey::Fetch: {
            const expected<FetchRequest> req = FetchRequest::decode(body);
            FetchResponse resp;
            if (!req) {
                resp.error_code = WireError::InvalidRequest;
                resp.encode(enc);
                co_return expected<void>{};
            }
            FetchOutcome outcome;
            if (partitions_ != nullptr) {
                const int owner = partitions_->owner_core(req->partition);
                outcome = co_await partitions_->route(*self_, req->partition, [this, owner, &req] {
                    return handle_fetch(*topics_by_core_[static_cast<std::size_t>(owner)], *req);
                });
            } else {
                outcome = handle_fetch(topics_, *req);  // sin cablear: local (tests).
            }
            if (outcome.error != WireError::None) {
                resp.error_code = outcome.error;
                resp.encode(enc);
                co_return expected<void>{};
            }
            // `outcome` posee los bytes; se codifican mientras sigue vivo (vista zero-copy).
            resp.batches = outcome.result.batches.as_span();
            resp.high_watermark = outcome.high_watermark;
            resp.log_start_offset = outcome.log_start_offset;
            resp.encode(enc);
            co_return expected<void>{};
        }
        case ApiKey::CreateTopic: {
            const expected<CreateTopicRequest> req = CreateTopicRequest::decode(body);
            CreateTopicResponse resp;
            if (!req) {
                resp.error_code = WireError::InvalidRequest;
            } else if (const expected<void> created =
                           co_await create_topic_cluster(req->name, req->partition_count);
                       !created) {
                resp.error_code = from_error(created.error());
            }
            resp.encode(enc);
            co_return expected<void>{};
        }
        case ApiKey::DeleteTopic: {
            const expected<DeleteTopicRequest> req = DeleteTopicRequest::decode(body);
            DeleteTopicResponse resp;
            if (!req) {
                resp.error_code = WireError::InvalidRequest;
            } else if (const expected<void> deleted = co_await delete_topic_cluster(req->name);
                       !deleted) {
                resp.error_code = from_error(deleted.error());
            }
            resp.encode(enc);
            co_return expected<void>{};
        }
        case ApiKey::OffsetCommit:
            co_return co_await dispatch_offset_commit(body, enc);
        case ApiKey::OffsetFetch:
            co_return co_await dispatch_offset_fetch(body, enc);
        case ApiKey::JoinGroup:
            co_return co_await dispatch_join_group(body, enc);
        case ApiKey::SyncGroup:
            co_return co_await dispatch_sync_group(body, enc);
        case ApiKey::Heartbeat:
            co_return co_await dispatch_heartbeat(body, enc);
        case ApiKey::LeaveGroup:
            co_return co_await dispatch_leave_group(body, enc);
    }
    co_return make_error(ErrorCode::InvalidArgument, "ApiKey desconocida");
}

task<expected<void>> RequestRouter::dispatch_offset_commit(Decoder& body, Encoder& enc) {
    const expected<OffsetCommitRequest> req = OffsetCommitRequest::decode(body);
    OffsetCommitResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
    } else {
        const std::string group =
            req->group;  // copia para el hash (la petición se mueve al lambda).
        resp = co_await on_group_owner<OffsetManager>(
            self_, partitions_, offsets_by_core_, offsets_, group,
            [req = *req](OffsetManager& offsets) { return handle_offset_commit(offsets, req); });
    }
    resp.encode(enc);
    co_return expected<void>{};
}

task<expected<void>> RequestRouter::dispatch_offset_fetch(Decoder& body, Encoder& enc) {
    const expected<OffsetFetchRequest> req = OffsetFetchRequest::decode(body);
    OffsetFetchResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
    } else {
        const std::string group = req->group;
        resp = co_await on_group_owner<OffsetManager>(
            self_, partitions_, offsets_by_core_, offsets_, group,
            [req = *req](OffsetManager& offsets) { return handle_offset_fetch(offsets, req); });
    }
    resp.encode(enc);
    co_return expected<void>{};
}

task<expected<void>> RequestRouter::dispatch_join_group(Decoder& body, Encoder& enc) {
    expected<JoinGroupRequest> req = JoinGroupRequest::decode(body);
    JoinGroupResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
    } else {
        const MonoTime now = std::chrono::steady_clock::now();
        const std::string group = req->group;
        resp = co_await on_group_owner<GroupCoordinator>(
            self_, partitions_, groups_by_core_, groups_, group,
            [now, req = std::move(*req)](GroupCoordinator& groups) mutable {
                return handle_join_group(groups, now, std::move(req));
            });
    }
    resp.encode(enc);
    co_return expected<void>{};
}

task<expected<void>> RequestRouter::dispatch_sync_group(Decoder& body, Encoder& enc) {
    const expected<SyncGroupRequest> req = SyncGroupRequest::decode(body);
    SyncGroupResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
    } else {
        const MonoTime now = std::chrono::steady_clock::now();
        const std::string group = req->group;
        resp = co_await on_group_owner<GroupCoordinator>(
            self_, partitions_, groups_by_core_, groups_, group,
            [now, req = *req](GroupCoordinator& groups) {
                return handle_sync_group(groups, now, req);
            });
    }
    resp.encode(enc);
    co_return expected<void>{};
}

task<expected<void>> RequestRouter::dispatch_heartbeat(Decoder& body, Encoder& enc) {
    const expected<HeartbeatRequest> req = HeartbeatRequest::decode(body);
    HeartbeatResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
    } else {
        const MonoTime now = std::chrono::steady_clock::now();
        const std::string group = req->group;
        resp = co_await on_group_owner<GroupCoordinator>(
            self_, partitions_, groups_by_core_, groups_, group,
            [now, req = *req](GroupCoordinator& groups) {
                return handle_heartbeat(groups, now, req);
            });
    }
    resp.encode(enc);
    co_return expected<void>{};
}

task<expected<void>> RequestRouter::dispatch_leave_group(Decoder& body, Encoder& enc) {
    const expected<LeaveGroupRequest> req = LeaveGroupRequest::decode(body);
    LeaveGroupResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
    } else {
        const std::string group = req->group;
        resp = co_await on_group_owner<GroupCoordinator>(
            self_, partitions_, groups_by_core_, groups_, group,
            [req = *req](GroupCoordinator& groups) { return handle_leave_group(groups, req); });
    }
    resp.encode(enc);
    co_return expected<void>{};
}

}  // namespace nexus
