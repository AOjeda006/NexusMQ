/// @file   broker/request_router.cpp
/// @brief  Implementación de RequestRouter (despacho protocolo↔dominio del broker).
/// @ingroup broker

#include "broker/request_router.hpp"

#include <cstddef>
#include <utility>

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
        case ApiKey::OffsetCommit:
        case ApiKey::OffsetFetch:
        case ApiKey::JoinGroup:
        case ApiKey::SyncGroup:
        case ApiKey::Heartbeat:
        case ApiKey::LeaveGroup:
            return make_error(ErrorCode::Unsupported, "ApiKey aún no soportada en este nodo");
    }
    return make_error(ErrorCode::InvalidArgument, "ApiKey desconocida");
}

}  // namespace nexus
