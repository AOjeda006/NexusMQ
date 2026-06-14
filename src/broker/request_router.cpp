/// @file   broker/request_router.cpp
/// @brief  Implementación de RequestRouter (despacho protocolo↔dominio del broker).
/// @ingroup broker

#include "broker/request_router.hpp"

#include <chrono>
#include <cstddef>
#include <utility>
#include <vector>

#include "broker/consumer_group.hpp"
#include "broker/group_coordinator.hpp"
#include "broker/partition.hpp"
#include "broker/topic.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "protocol/codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/messages.hpp"

namespace nexus {

namespace {

/// Tope de lectura por defecto cuando el cliente no acota `max_bytes` (anti-respuesta-gigante).
constexpr std::size_t kDefaultFetchBytes = 1UL * 1024 * 1024;

/// Localiza la partición destino; `nullptr` si el topic o la partición no existen.
Partition* find_partition(TopicManager& topics, const std::string& topic, PartitionId partition) {
    Topic* found = topics.get(topic);
    return found == nullptr ? nullptr : found->partition(partition);
}

ProduceResponse handle_produce(TopicManager& topics, const ProduceRequest& req) {
    ProduceResponse resp;
    Partition* part = find_partition(topics, req.topic, req.partition);
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

OffsetCommitResponse handle_offset_commit(OffsetManager& offsets, Decoder& body) {
    const expected<OffsetCommitRequest> req = OffsetCommitRequest::decode(body);
    OffsetCommitResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
        return resp;
    }
    offsets.commit(req->group, req->topic, req->partition, req->offset, req->metadata);
    return resp;
}

OffsetFetchResponse handle_offset_fetch(const OffsetManager& offsets, Decoder& body) {
    const expected<OffsetFetchRequest> req = OffsetFetchRequest::decode(body);
    OffsetFetchResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
        return resp;
    }
    if (const expected<Offset> offset = offsets.fetch(req->group, req->topic, req->partition);
        offset) {
        resp.offset = *offset;
    } else {
        resp.offset = -1;  // Sin commit previo: no es error; el cliente decide el inicio.
    }
    return resp;
}

JoinGroupResponse handle_join_group(GroupCoordinator& groups, MonoTime now, Decoder& body) {
    expected<JoinGroupRequest> req = JoinGroupRequest::decode(body);
    JoinGroupResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
        return resp;
    }
    const expected<JoinResult> result =
        groups.join(now, req->group, std::move(req->member_id), std::move(req->subscription),
                    std::chrono::milliseconds{req->session_timeout_ms});
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

SyncGroupResponse handle_sync_group(GroupCoordinator& groups, MonoTime now, Decoder& body) {
    const expected<SyncGroupRequest> req = SyncGroupRequest::decode(body);
    SyncGroupResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
        return resp;
    }
    std::vector<MemberAssignment> assignments;
    assignments.reserve(req->assignments.size());
    for (const GroupAssignment& entry : req->assignments) {
        assignments.push_back(
            MemberAssignment{.member_id = entry.member_id, .assignment = entry.assignment});
    }
    const expected<SyncResult> result =
        groups.sync(now, req->group, req->member_id, req->generation, assignments);
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

HeartbeatResponse handle_heartbeat(GroupCoordinator& groups, MonoTime now, Decoder& body) {
    const expected<HeartbeatRequest> req = HeartbeatRequest::decode(body);
    HeartbeatResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
        return resp;
    }
    const expected<HeartbeatStatus> status =
        groups.heartbeat(now, req->group, req->member_id, req->generation);
    if (!status) {
        resp.error_code = from_error(status.error());
        return resp;
    }
    if (*status == HeartbeatStatus::RebalanceInProgress) {
        resp.error_code = WireError::RebalanceInProgress;
    }
    return resp;
}

LeaveGroupResponse handle_leave_group(GroupCoordinator& groups, Decoder& body) {
    const expected<LeaveGroupRequest> req = LeaveGroupRequest::decode(body);
    LeaveGroupResponse resp;
    if (!req) {
        resp.error_code = WireError::InvalidRequest;
        return resp;
    }
    if (const expected<void> left = groups.leave(req->group, req->member_id); !left) {
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

expected<void> RequestRouter::dispatch(ApiKey key, std::uint16_t /*api_version*/, Decoder& body,
                                       Buffer& out) {
    Encoder enc{out};
    switch (key) {
        case ApiKey::ApiVersions: {
            const ApiVersionsResponse resp{.ranges = supported_versions()};
            resp.encode(enc);
            return {};
        }
        case ApiKey::Metadata: {
            const expected<MetadataRequest> req = MetadataRequest::decode(body);
            const MetadataResponse resp =
                req ? handle_metadata(topics_, node_id_, host_, port_, *req) : MetadataResponse{};
            resp.encode(enc);
            return {};
        }
        case ApiKey::Produce: {
            const expected<ProduceRequest> req = ProduceRequest::decode(body);
            const ProduceResponse resp =
                req ? handle_produce(topics_, *req)
                    : ProduceResponse{.error_code = WireError::InvalidRequest};
            resp.encode(enc);
            return {};
        }
        case ApiKey::Fetch: {
            const expected<FetchRequest> req = FetchRequest::decode(body);
            FetchResponse resp;
            if (!req) {
                resp.error_code = WireError::InvalidRequest;
                resp.encode(enc);
                return {};
            }
            const Partition* part = find_partition(topics_, req->topic, req->partition);
            if (part == nullptr) {
                resp.error_code = WireError::UnknownTopicOrPartition;
                resp.encode(enc);
                return {};
            }
            const std::size_t max_bytes =
                req->max_bytes > 0 ? static_cast<std::size_t>(req->max_bytes) : kDefaultFetchBytes;
            const expected<FetchResult> result = part->fetch(req->fetch_offset, max_bytes);
            if (!result) {
                resp.error_code = from_error(result.error());
                resp.encode(enc);
                return {};
            }
            // `result` posee los bytes; se codifican mientras sigue vivo (vista zero-copy).
            resp.batches = result->batches.as_span();
            resp.high_watermark = part->high_watermark();
            resp.log_start_offset = part->log().log_start_offset();
            resp.encode(enc);
            return {};
        }
        case ApiKey::CreateTopic: {
            const expected<CreateTopicRequest> req = CreateTopicRequest::decode(body);
            CreateTopicResponse resp;
            if (!req) {
                resp.error_code = WireError::InvalidRequest;
            } else if (const expected<TopicMetadata> meta =
                           topics_.create_topic(req->name, req->partition_count);
                       !meta) {
                resp.error_code = from_error(meta.error());
            }
            resp.encode(enc);
            return {};
        }
        case ApiKey::DeleteTopic: {
            const expected<DeleteTopicRequest> req = DeleteTopicRequest::decode(body);
            DeleteTopicResponse resp;
            if (!req) {
                resp.error_code = WireError::InvalidRequest;
            } else if (const expected<void> deleted = topics_.delete_topic(req->name); !deleted) {
                resp.error_code = from_error(deleted.error());
            }
            resp.encode(enc);
            return {};
        }
        case ApiKey::OffsetCommit: {
            handle_offset_commit(offsets_, body).encode(enc);
            return {};
        }
        case ApiKey::OffsetFetch: {
            handle_offset_fetch(offsets_, body).encode(enc);
            return {};
        }
        case ApiKey::JoinGroup: {
            handle_join_group(groups_, std::chrono::steady_clock::now(), body).encode(enc);
            return {};
        }
        case ApiKey::SyncGroup: {
            handle_sync_group(groups_, std::chrono::steady_clock::now(), body).encode(enc);
            return {};
        }
        case ApiKey::Heartbeat: {
            handle_heartbeat(groups_, std::chrono::steady_clock::now(), body).encode(enc);
            return {};
        }
        case ApiKey::LeaveGroup: {
            handle_leave_group(groups_, body).encode(enc);
            return {};
        }
    }
    return make_error(ErrorCode::InvalidArgument, "ApiKey desconocida");
}

}  // namespace nexus
