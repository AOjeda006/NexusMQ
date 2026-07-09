// Pruebas del adaptador KafkaServerBroker sobre el broker real (modo local, sync_wait) — F7f.
#include "server/kafka_adapter.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "broker/partition_base.hpp"
#include "broker/topic.hpp"
#include "common/bytes.hpp"
#include "common/task.hpp"
#include "common/types.hpp"
#include "kafka/error_code.hpp"
#include "kafka/fetch.hpp"
#include "kafka/list_offsets.hpp"
#include "kafka/metadata.hpp"
#include "kafka/produce.hpp"
#include "kafka/record_batch.hpp"
#include "telemetry/metrics.hpp"

namespace {

using nexus::sync_wait;

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_kafka_adapter_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

/// Construye un RecordBatch v2 de Kafka mínimo con @p base_offset, @p record_count y @p payload.
std::vector<std::byte> make_kafka_batch(std::int64_t base_offset, std::int32_t record_count,
                                        nexus::ByteSpan payload) {
    nexus::Buffer buf;
    nexus::kafka::Encoder enc{buf};
    enc.put_i64(base_offset);
    enc.put_i32(static_cast<std::int32_t>(49 + payload.size()));  // batchLength
    enc.put_i32(0);                                               // partitionLeaderEpoch
    enc.put_i8(nexus::kafka::kRecordBatchMagicV2);                // magic
    enc.put_u32(0);                                               // crc (el broker no lo valida)
    enc.put_i16(0);                                               // attributes
    enc.put_i32(record_count > 0 ? record_count - 1 : 0);         // lastOffsetDelta
    enc.put_i64(0);                                               // baseTimestamp
    enc.put_i64(0);                                               // maxTimestamp
    enc.put_i64(-1);                                              // producerId
    enc.put_i16(-1);                                              // producerEpoch
    enc.put_i32(-1);                                              // baseSequence
    enc.put_i32(record_count);                                    // recordCount
    enc.put_raw(payload);
    const nexus::ByteSpan bytes = buf.as_span();
    return {bytes.begin(), bytes.end()};
}

nexus::kafka::ProduceRequest one_partition_produce(const std::string& topic, std::int32_t partition,
                                                   std::vector<std::byte> records) {
    nexus::kafka::ProduceRequest req;
    nexus::kafka::ProduceTopicData topic_data;
    topic_data.name = topic;
    topic_data.partitions.push_back(
        nexus::kafka::ProducePartitionData{.index = partition, .records = std::move(records)});
    req.topics.push_back(std::move(topic_data));
    return req;
}

nexus::kafka::FetchRequest one_partition_fetch(const std::string& topic, std::int32_t partition,
                                               std::int64_t offset) {
    nexus::kafka::FetchRequest req;
    nexus::kafka::FetchTopicRequest topic_req;
    topic_req.topic = topic;
    topic_req.partitions.push_back(
        nexus::kafka::FetchPartitionRequest{.partition = partition, .fetch_offset = offset});
    req.topics.push_back(std::move(topic_req));
    return req;
}

TEST(KafkaServerBroker, Metadata_ListsTopicsWithThisNodeAsLeader) {
    TempDir dir{"meta"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("orders", 2).has_value());
    nexus::KafkaServerBroker broker{topics, /*node_id=*/7, "127.0.0.1", 9099};

    nexus::kafka::MetadataRequest req;  // topics = nullopt → todos.
    const nexus::kafka::MetadataResponse resp = sync_wait(broker.metadata(req));

    ASSERT_EQ(resp.brokers.size(), 1U);
    EXPECT_EQ(resp.brokers[0].node_id, 7);
    EXPECT_EQ(resp.brokers[0].port, 9099);
    EXPECT_EQ(resp.controller_id, 7);
    ASSERT_EQ(resp.topics.size(), 1U);
    EXPECT_EQ(resp.topics[0].name, "orders");
    ASSERT_EQ(resp.topics[0].partitions.size(), 2U);
    EXPECT_EQ(resp.topics[0].partitions[0].leader_id, 7);
    EXPECT_EQ(resp.topics[0].partitions[1].partition_index, 1);
}

TEST(KafkaServerBroker, Metadata_UnknownTopic_ReportsError) {
    TempDir dir{"meta_unknown"};
    nexus::TopicManager topics{dir.path()};
    nexus::KafkaServerBroker broker{topics, 1, "127.0.0.1", 9099};

    nexus::kafka::MetadataRequest req;
    req.topics = std::vector<std::string>{"missing"};
    const nexus::kafka::MetadataResponse resp = sync_wait(broker.metadata(req));

    ASSERT_EQ(resp.topics.size(), 1U);
    EXPECT_EQ(resp.topics[0].name, "missing");
    EXPECT_EQ(resp.topics[0].error_code,
              nexus::kafka::to_wire(nexus::kafka::KafkaError::UnknownTopicOrPartition));
}

TEST(KafkaServerBroker, ProduceThenFetch_RoundTripsRecordsAndRebasesOffset) {
    TempDir dir{"roundtrip"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("orders", 1).has_value());
    nexus::KafkaServerBroker broker{topics, 1, "127.0.0.1", 9099};

    // El productor envía baseOffset=999 (como hace kcat con un valor arbitrario); el log debe
    // reasignarlo a 0 al anexar y reflejarlo en el batch devuelto por fetch.
    const std::vector<std::byte> payload{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}};
    const std::vector<std::byte> batch = make_kafka_batch(999, 1, nexus::ByteSpan{payload});

    const nexus::kafka::ProduceResponse pr =
        sync_wait(broker.produce(one_partition_produce("orders", 0, batch)));
    ASSERT_EQ(pr.topics.size(), 1U);
    ASSERT_EQ(pr.topics[0].partitions.size(), 1U);
    EXPECT_EQ(pr.topics[0].partitions[0].error_code,
              nexus::kafka::to_wire(nexus::kafka::KafkaError::None));
    EXPECT_EQ(pr.topics[0].partitions[0].base_offset, 0);

    const nexus::kafka::FetchResponse fr =
        sync_wait(broker.fetch(one_partition_fetch("orders", 0, 0)));
    ASSERT_EQ(fr.topics.size(), 1U);
    ASSERT_EQ(fr.topics[0].partitions.size(), 1U);
    const nexus::kafka::FetchPartitionResponse& part = fr.topics[0].partitions[0];
    EXPECT_EQ(part.error_code, nexus::kafka::to_wire(nexus::kafka::KafkaError::None));
    EXPECT_EQ(part.high_watermark, 1);

    // El blob devuelto es el batch de Kafka con su baseOffset reescrito a 0 (el asignado por el
    // log).
    ASSERT_EQ(part.records.size(), batch.size());
    const auto info = nexus::kafka::peek_record_batch(nexus::ByteSpan{part.records});
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->base_offset, 0);
    EXPECT_EQ(info->record_count, 1);
    // El payload de records (tras la cabecera fija de 61 bytes) viaja intacto.
    const nexus::ByteSpan returned{part.records};
    EXPECT_EQ(returned[nexus::kafka::kRecordBatchHeaderSize], std::byte{0xDE});
}

TEST(KafkaServerBroker, ProduceThenFetch_SecondBatchGetsNextOffset) {
    TempDir dir{"offsets"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("orders", 1).has_value());
    nexus::KafkaServerBroker broker{topics, 1, "127.0.0.1", 9099};

    const std::vector<std::byte> empty;
    sync_wait(broker.produce(one_partition_produce("orders", 0, make_kafka_batch(0, 1, empty))));
    const nexus::kafka::ProduceResponse pr2 = sync_wait(
        broker.produce(one_partition_produce("orders", 0, make_kafka_batch(0, 1, empty))));
    EXPECT_EQ(pr2.topics[0].partitions[0].base_offset, 1);  // segundo record → offset 1.

    const nexus::kafka::FetchResponse fr =
        sync_wait(broker.fetch(one_partition_fetch("orders", 0, 0)));
    EXPECT_EQ(fr.topics[0].partitions[0].high_watermark, 2);
}

TEST(KafkaServerBroker, ListOffsets_EarliestAndLatest) {
    TempDir dir{"list_offsets"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("orders", 1).has_value());
    nexus::KafkaServerBroker broker{topics, 1, "127.0.0.1", 9099};

    const std::vector<std::byte> empty;
    sync_wait(broker.produce(one_partition_produce("orders", 0, make_kafka_batch(0, 2, empty))));

    nexus::kafka::ListOffsetsRequest req;
    nexus::kafka::ListOffsetTopic topic;
    topic.name = "orders";
    topic.partitions.push_back(nexus::kafka::ListOffsetPartition{
        .partition_index = 0, .timestamp = nexus::kafka::kListOffsetsEarliest});
    topic.partitions.push_back(nexus::kafka::ListOffsetPartition{
        .partition_index = 0, .timestamp = nexus::kafka::kListOffsetsLatest});
    req.topics.push_back(std::move(topic));

    const nexus::kafka::ListOffsetsResponse resp = sync_wait(broker.list_offsets(req));
    ASSERT_EQ(resp.topics.size(), 1U);
    ASSERT_EQ(resp.topics[0].partitions.size(), 2U);
    EXPECT_EQ(resp.topics[0].partitions[0].offset, 0);  // earliest = log start
    EXPECT_EQ(resp.topics[0].partitions[1].offset, 2);  // latest = high-watermark (2 records)
}

TEST(KafkaServerBroker, Produce_UnknownPartition_ReturnsError) {
    TempDir dir{"prod_unknown"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("orders", 1).has_value());
    nexus::KafkaServerBroker broker{topics, 1, "127.0.0.1", 9099};

    const std::vector<std::byte> empty;
    const nexus::kafka::ProduceResponse pr = sync_wait(
        broker.produce(one_partition_produce("orders", 9, make_kafka_batch(0, 1, empty))));
    EXPECT_EQ(pr.topics[0].partitions[0].error_code,
              nexus::kafka::to_wire(nexus::kafka::KafkaError::UnknownTopicOrPartition));
}

TEST(KafkaServerBroker, Fetch_UnknownTopic_ReturnsError) {
    TempDir dir{"fetch_unknown"};
    nexus::TopicManager topics{dir.path()};
    nexus::KafkaServerBroker broker{topics, 1, "127.0.0.1", 9099};

    const nexus::kafka::FetchResponse fr =
        sync_wait(broker.fetch(one_partition_fetch("ghost", 0, 0)));
    EXPECT_EQ(fr.topics[0].partitions[0].error_code,
              nexus::kafka::to_wire(nexus::kafka::KafkaError::UnknownTopicOrPartition));
}

// --- P2 (ADR-0030): guarda cross-protocol nativo/Kafka en una misma partición ---

TEST(KafkaServerBroker, Produce_ReclamaProtocoloKafka) {
    TempDir dir{"claim_kafka"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("orders", 1).has_value());
    nexus::KafkaServerBroker broker{topics, 1, "127.0.0.1", 9099};

    const std::vector<std::byte> empty;
    sync_wait(broker.produce(one_partition_produce("orders", 0, make_kafka_batch(0, 1, empty))));
    EXPECT_EQ(topics.get("orders")->partition(0)->protocol(), nexus::WireProtocol::Kafka);
}

TEST(KafkaServerBroker, CrossProtocol_ParticionNativa_RechazaProduceKafka) {
    TempDir dir{"cross_produce"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("orders", 1).has_value());
    // Un productor nativo reclama la partición antes de que llegue Kafka.
    ASSERT_TRUE(topics.get("orders")->partition(0)->claim_protocol(nexus::WireProtocol::Native));
    nexus::KafkaServerBroker broker{topics, 1, "127.0.0.1", 9099};

    const std::vector<std::byte> empty;
    const nexus::kafka::ProduceResponse pr = sync_wait(
        broker.produce(one_partition_produce("orders", 0, make_kafka_batch(0, 1, empty))));
    EXPECT_EQ(pr.topics[0].partitions[0].error_code,
              nexus::kafka::to_wire(nexus::kafka::KafkaError::InvalidRequest));
}

TEST(KafkaServerBroker, CrossProtocol_ParticionNativa_RechazaFetchKafka) {
    TempDir dir{"cross_fetch"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("orders", 1).has_value());
    ASSERT_TRUE(topics.get("orders")->partition(0)->claim_protocol(nexus::WireProtocol::Native));
    nexus::KafkaServerBroker broker{topics, 1, "127.0.0.1", 9099};

    const nexus::kafka::FetchResponse fr =
        sync_wait(broker.fetch(one_partition_fetch("orders", 0, 0)));
    EXPECT_EQ(fr.topics[0].partitions[0].error_code,
              nexus::kafka::to_wire(nexus::kafka::KafkaError::InvalidRequest));
}

// --- P5e: métricas del plano de datos etiquetadas por protocolo (protocol="kafka") ---

TEST(KafkaServerBroker, Metrics_Produce_RegistraProtocoloKafka) {
    TempDir dir{"metrics_kafka"};
    nexus::TopicManager topics{dir.path()};
    ASSERT_TRUE(topics.create_topic("orders", 1).has_value());
    nexus::KafkaServerBroker broker{topics, 1, "127.0.0.1", 9099};
    nexus::MetricsRegistry metrics;
    broker.set_metrics(metrics);

    const std::vector<std::byte> empty;
    const std::vector<std::byte> batch = make_kafka_batch(0, 1, empty);
    sync_wait(broker.produce(one_partition_produce("orders", 0, batch)));

    const nexus::Labels kafka_produce{{"api", "produce"}, {"protocol", "kafka"}};
    EXPECT_EQ(metrics.counter("nexus_broker_requests_total", kafka_produce).value(), 1U);
    EXPECT_EQ(metrics.counter("nexus_broker_request_errors_total", kafka_produce).value(), 0U);
    EXPECT_EQ(metrics.counter("nexus_broker_request_bytes_total", kafka_produce).value(),
              batch.size());
    // El plano nativo no se ha tocado: su serie `protocol="native"` no existe aún.
    const nexus::Labels native_produce{{"api", "produce"}, {"protocol", "native"}};
    EXPECT_EQ(metrics.counter("nexus_broker_requests_total", native_produce).value(), 0U);
}

}  // namespace
