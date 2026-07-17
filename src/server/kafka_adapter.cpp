/// @file   server/kafka_adapter.cpp
/// @brief  Implementación del adaptador KafkaServerBroker sobre el broker real — F7f.
/// @ingroup server

#include "server/kafka_adapter.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "broker/partition_base.hpp"
#include "broker/topic.hpp"
#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "kafka/error_code.hpp"
#include "kafka/record_batch.hpp"
#include "reactor/cross_core_call.hpp"
#include "storage/fetch_result.hpp"
#include "telemetry/metrics.hpp"

namespace nexus {

namespace {

/// Tope de lectura por defecto cuando el cliente no acota `partition_max_bytes` (anti-respuesta
/// gigante); espeja el del camino nativo (`RequestRouter`).
constexpr std::size_t kDefaultFetchBytes = 1UL * 1024 * 1024;

/// Traduce un `ErrorCode` interno al código de error del wire de Kafka **en el borde** (ADR-0009).
std::int16_t to_kafka_error(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::Corrupt:
            return kafka::to_wire(kafka::KafkaError::CorruptMessage);
        case ErrorCode::OutOfRange:
            return kafka::to_wire(kafka::KafkaError::OffsetOutOfRange);
        case ErrorCode::NotFound:
            return kafka::to_wire(kafka::KafkaError::UnknownTopicOrPartition);
        case ErrorCode::InvalidArgument:
            return kafka::to_wire(kafka::KafkaError::InvalidRequest);
        case ErrorCode::IoError:
        case ErrorCode::OutOfSpace:
        case ErrorCode::Unsupported:
        case ErrorCode::Shutdown:
        case ErrorCode::Fenced:
            return kafka::to_wire(kafka::KafkaError::Unknown);
    }
    return kafka::to_wire(kafka::KafkaError::Unknown);
}

/// Localiza la partición destino; `nullptr` si el topic o la partición no existen.
PartitionBase* find_partition(TopicManager& topics, const std::string& topic,
                              PartitionId partition) {
    Topic* found = topics.get(topic);
    return found == nullptr ? nullptr : found->partition(partition);
}

/// @brief Cuenta los records de todos los RecordBatch v2 concatenados en @p blob.
/// @return El total, o `Corrupt` si algún batch está malformado.
expected<std::int32_t> count_records(ByteSpan blob) {
    std::int32_t total = 0;
    std::size_t pos = 0;
    while (pos < blob.size()) {
        const expected<kafka::RecordBatchInfo> info = kafka::peek_record_batch(blob.subspan(pos));
        if (!info) {
            return std::unexpected(info.error());
        }
        total += info->record_count;
        pos += info->encoded_size;
    }
    return total;
}

/// @brief Reescribe el `baseOffset` de cada RecordBatch v2 de @p blob partiendo de @p base.
/// @details Un envoltorio interno suele envolver un único batch Kafka, pero soportamos varios
///   concatenados: el primero arranca en @p base y cada siguiente en `base + records acumulados`.
void rebase_subbatches(std::span<std::byte> blob, std::int64_t base) noexcept {
    std::size_t pos = 0;
    std::int64_t offset = base;
    while (pos < blob.size()) {
        const expected<kafka::RecordBatchInfo> info = kafka::peek_record_batch(blob.subspan(pos));
        if (!info) {
            return;  // defensivo: blob ya validado al escribir; paramos si algo no cuadra.
        }
        kafka::set_base_offset(blob.subspan(pos), offset);
        offset += info->record_count;
        pos += info->encoded_size;
    }
}

/// @brief Reconstruye el blob de records de Kafka a partir de los batches **internos** leídos del
///   log: extrae el blob opaco de cada envoltorio y le reasigna el `baseOffset` autoritativo.
std::vector<std::byte> rebuild_kafka_records(ByteSpan internal_batches) {
    std::vector<std::byte> out;
    std::size_t pos = 0;
    while (pos + RecordBatch::kHeaderSize <= internal_batches.size()) {
        const expected<RecordBatchView> view = RecordBatch::peek(internal_batches.subspan(pos));
        if (!view || view->encoded_size < RecordBatch::kHeaderSize ||
            pos + view->encoded_size > internal_batches.size()) {
            break;  // defensivo: el log valida al escribir; paramos ante cualquier inconsistencia.
        }
        const std::size_t records_off = pos + RecordBatch::kHeaderSize;
        const std::size_t records_len = view->encoded_size - RecordBatch::kHeaderSize;
        const std::size_t out_start = out.size();
        out.insert(
            out.end(), internal_batches.begin() + static_cast<std::ptrdiff_t>(records_off),
            internal_batches.begin() + static_cast<std::ptrdiff_t>(records_off + records_len));
        rebase_subbatches(std::span<std::byte>{out.data() + out_start, records_len},
                          view->base_offset);
        pos += view->encoded_size;
    }
    return out;
}

/// Anexa una partición de un Produce: cuenta records, envuelve el blob y delega en la partición.
kafka::ProducePartitionResponse produce_partition(TopicManager& topics, const std::string& topic,
                                                  const kafka::ProducePartitionData& data) {
    kafka::ProducePartitionResponse resp;
    resp.index = data.index;
    PartitionBase* part = find_partition(topics, topic, data.index);
    if (part == nullptr) {
        resp.error_code = kafka::to_wire(kafka::KafkaError::UnknownTopicOrPartition);
        return resp;
    }
    const ByteSpan blob{data.records};
    const expected<std::int32_t> total = count_records(blob);
    if (!total) {
        resp.error_code = to_kafka_error(total.error().code());
        return resp;
    }
    if (*total == 0) {
        resp.base_offset = part->log().log_end_offset();  // nada que anexar.
        return resp;
    }
    // P2 (ADR-0030): reclama el protocolo Kafka de la partición antes de anexar; un produce de
    // Kafka sobre una partición ya escrita en nativo se rechaza (formatos de record incompatibles).
    if (const expected<void> claimed = part->claim_protocol(WireProtocol::Kafka); !claimed) {
        resp.error_code = to_kafka_error(claimed.error().code());
        return resp;
    }
    // Envoltorio interno: el blob de Kafka es opaco; solo el `record_count` gobierna los offsets.
    const RecordBatchHeader header{.record_count = *total};
    const RecordBatch envelope{header, std::vector<std::byte>{blob.begin(), blob.end()}};
    const expected<Offset> last = part->produce(envelope);
    if (!last) {
        resp.error_code = to_kafka_error(last.error().code());
        return resp;
    }
    resp.base_offset = *last - *total + 1;
    return resp;
}

/// Lee una partición de un Fetch: lee del log y reconstruye el blob de Kafka con offsets correctos.
kafka::FetchPartitionResponse fetch_partition(TopicManager& topics, const std::string& topic,
                                              const kafka::FetchPartitionRequest& req) {
    kafka::FetchPartitionResponse resp;
    resp.partition_index = req.partition;
    const PartitionBase* part = find_partition(topics, topic, req.partition);
    if (part == nullptr) {
        resp.error_code = kafka::to_wire(kafka::KafkaError::UnknownTopicOrPartition);
        return resp;
    }
    // P2 (ADR-0030): rechaza una lectura Kafka de una partición escrita en nativo; sus bytes no son
    // un `RecordBatch` v2, así que un error explícito evita devolver records ilegibles (o cero) en
    // silencio.
    if (part->protocol() != WireProtocol::Unset && part->protocol() != WireProtocol::Kafka) {
        resp.error_code = to_kafka_error(ErrorCode::InvalidArgument);
        return resp;
    }
    const std::size_t max_bytes = req.partition_max_bytes > 0
                                      ? static_cast<std::size_t>(req.partition_max_bytes)
                                      : kDefaultFetchBytes;
    const expected<FetchResult> result = part->fetch(req.fetch_offset, max_bytes);
    if (!result) {
        resp.error_code = to_kafka_error(result.error().code());
        return resp;
    }
    resp.high_watermark = part->high_watermark();
    resp.last_stable_offset = resp.high_watermark;  // sin transacciones: LSO = high-watermark.
    resp.log_start_offset = part->log().log_start_offset();
    resp.records = rebuild_kafka_records(result->batches.as_span());
    return resp;
}

/// Resuelve el offset de una partición en un ListOffsets: earliest = log start; el resto (latest o
/// una marca de tiempo concreta, que no indexamos) = high-watermark.
kafka::ListOffsetPartitionResponse list_offset_partition(TopicManager& topics,
                                                         const std::string& topic,
                                                         const kafka::ListOffsetPartition& req) {
    kafka::ListOffsetPartitionResponse resp;
    resp.partition_index = req.partition_index;
    const PartitionBase* part = find_partition(topics, topic, req.partition_index);
    if (part == nullptr) {
        resp.error_code = kafka::to_wire(kafka::KafkaError::UnknownTopicOrPartition);
        return resp;
    }
    resp.offset = req.timestamp == kafka::kListOffsetsEarliest ? part->log().log_start_offset()
                                                               : part->high_watermark();
    resp.leader_epoch = static_cast<std::int32_t>(part->leader_epoch());
    return resp;
}

/// Construye el `MetadataTopic` de @p meta con este nodo como líder de todas sus particiones.
kafka::MetadataTopic describe_topic(const TopicMetadata& meta, NodeId node_id) {
    kafka::MetadataTopic topic;
    topic.name = meta.name;
    topic.partitions.reserve(static_cast<std::size_t>(meta.partition_count));
    for (std::int32_t p = 0; p < meta.partition_count; ++p) {
        kafka::MetadataPartition partition;
        partition.partition_index = p;
        partition.leader_id = node_id;
        partition.replica_nodes = {node_id};
        partition.isr_nodes = {node_id};
        topic.partitions.push_back(std::move(partition));
    }
    return topic;
}

}  // namespace

TopicManager& KafkaServerBroker::owner_manager(PartitionId partition) noexcept {
    if (partitions_ == nullptr) {
        return topics_;
    }
    const int owner = partitions_->owner_core(partition);
    return *topics_by_core_[static_cast<std::size_t>(owner)];
}

void KafkaServerBroker::record_request(std::string_view api, std::uint64_t bytes,
                                       std::uint64_t records, bool had_error,
                                       std::chrono::steady_clock::time_point start) const {
    if (metrics_ == nullptr) {
        return;  // sin cablear (tests que no inyectan métricas): no se registra nada.
    }
    // Mismas familias que el plano nativo (P5e), con `protocol="kafka"` para poder agregar por
    // protocolo. Como la interoperabilidad Kafka no es el camino caliente, se resuelven las series
    // por llamada (sin cachear), a diferencia del `RequestRouter`.
    const Labels labels{{"api", std::string{api}}, {"protocol", "kafka"}};
    metrics_->counter("nexus_broker_requests_total", labels).inc();
    metrics_->counter("nexus_broker_request_bytes_total", labels).inc(bytes);
    metrics_->counter("nexus_broker_messages_total", labels).inc(records);
    if (had_error) {
        metrics_->counter("nexus_broker_request_errors_total", labels).inc();
    }
    const std::chrono::duration<double> elapsed = std::chrono::steady_clock::now() - start;
    metrics_->histogram("nexus_broker_request_duration_seconds", labels).observe(elapsed.count());
}

task<kafka::MetadataResponse> KafkaServerBroker::metadata(const kafka::MetadataRequest& req) {
    // Los metadatos están registrados completos en cada núcleo (ADR-0026): se sirven localmente.
    kafka::MetadataResponse resp;
    resp.controller_id = node_id_;
    resp.brokers.push_back(
        kafka::MetadataBroker{.node_id = node_id_, .host = host_, .port = port_, .rack = {}});

    const std::vector<TopicMetadata> all = topics_.list_metadata();
    if (!req.topics.has_value()) {  // `nullopt` = todos los topics.
        resp.topics.reserve(all.size());
        for (const TopicMetadata& meta : all) {
            resp.topics.push_back(describe_topic(meta, node_id_));
        }
        co_return resp;
    }
    for (const std::string& wanted : *req.topics) {
        bool matched = false;
        for (const TopicMetadata& meta : all) {
            if (meta.name == wanted) {
                resp.topics.push_back(describe_topic(meta, node_id_));
                matched = true;
                break;
            }
        }
        if (!matched) {
            kafka::MetadataTopic topic;
            topic.name = wanted;
            topic.error_code = kafka::to_wire(kafka::KafkaError::UnknownTopicOrPartition);
            resp.topics.push_back(std::move(topic));
        }
    }
    co_return resp;
}

task<kafka::ProduceResponse> KafkaServerBroker::produce(const kafka::ProduceRequest& req) {
    const std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
    kafka::ProduceResponse resp;
    resp.topics.reserve(req.topics.size());
    std::uint64_t bytes = 0;
    std::uint64_t records = 0;
    bool had_error = false;
    for (const kafka::ProduceTopicData& topic : req.topics) {
        kafka::ProduceTopicResponse topic_resp;
        topic_resp.name = topic.name;
        for (const kafka::ProducePartitionData& data : topic.partitions) {
            bytes += data.records.size();
            // Records recibidos (mensajes), distinto de la RPC; un blob corrupto lo rechaza el
            // append, así que aquí solo contamos lo legible (`value_or(0)`).
            records += static_cast<std::uint64_t>(count_records(data.records).value_or(0));
            kafka::ProducePartitionResponse part_resp;
            if (partitions_ == nullptr) {
                part_resp = produce_partition(topics_, topic.name, data);
            } else {
                TopicManager& owner = owner_manager(data.index);
                part_resp = co_await partitions_->route(
                    *self_, data.index,
                    [&owner, &topic, &data] { return produce_partition(owner, topic.name, data); });
            }
            had_error =
                had_error || part_resp.error_code != kafka::to_wire(kafka::KafkaError::None);
            topic_resp.partitions.push_back(std::move(part_resp));
        }
        resp.topics.push_back(std::move(topic_resp));
    }
    record_request("produce", bytes, records, had_error, start);
    co_return resp;
}

task<kafka::ListOffsetsResponse> KafkaServerBroker::list_offsets(
    const kafka::ListOffsetsRequest& req) {
    kafka::ListOffsetsResponse resp;
    resp.topics.reserve(req.topics.size());
    for (const kafka::ListOffsetTopic& topic : req.topics) {
        kafka::ListOffsetTopicResponse topic_resp;
        topic_resp.name = topic.name;
        for (const kafka::ListOffsetPartition& part : topic.partitions) {
            kafka::ListOffsetPartitionResponse part_resp;
            if (partitions_ == nullptr) {
                part_resp = list_offset_partition(topics_, topic.name, part);
            } else {
                TopicManager& owner = owner_manager(part.partition_index);
                part_resp = co_await partitions_->route(
                    *self_, part.partition_index, [&owner, &topic, &part] {
                        return list_offset_partition(owner, topic.name, part);
                    });
            }
            topic_resp.partitions.push_back(part_resp);
        }
        resp.topics.push_back(std::move(topic_resp));
    }
    co_return resp;
}

task<kafka::FetchResponse> KafkaServerBroker::fetch(const kafka::FetchRequest& req) {
    const std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
    kafka::FetchResponse resp;
    resp.topics.reserve(req.topics.size());
    std::uint64_t bytes = 0;
    std::uint64_t records = 0;
    bool had_error = false;
    for (const kafka::FetchTopicRequest& topic : req.topics) {
        kafka::FetchTopicResponse topic_resp;
        topic_resp.topic = topic.topic;
        for (const kafka::FetchPartitionRequest& part : topic.partitions) {
            kafka::FetchPartitionResponse part_resp;
            if (partitions_ == nullptr) {
                part_resp = fetch_partition(topics_, topic.topic, part);
            } else {
                TopicManager& owner = owner_manager(part.partition);
                part_resp = co_await partitions_->route(
                    *self_, part.partition,
                    [&owner, &topic, &part] { return fetch_partition(owner, topic.topic, part); });
            }
            bytes += part_resp.records.size();
            records += static_cast<std::uint64_t>(count_records(part_resp.records).value_or(0));
            had_error =
                had_error || part_resp.error_code != kafka::to_wire(kafka::KafkaError::None);
            topic_resp.partitions.push_back(std::move(part_resp));
        }
        resp.topics.push_back(std::move(topic_resp));
    }
    record_request("fetch", bytes, records, had_error, start);
    co_return resp;
}

}  // namespace nexus
