/// @file   client/producer.cpp
/// @brief  Implementación del Producer.
/// @ingroup client

#include "client/producer.hpp"

#include <array>
#include <cstddef>
#include <utility>
#include <vector>

#include "client/client.hpp"
#include "common/record.hpp"
#include "common/record_codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/messages.hpp"

namespace nexus {
namespace {

/// Bytes propietarios a partir de una vista (copia).
std::vector<std::byte> own(ByteSpan data) {
    return {data.begin(), data.end()};
}

}  // namespace

expected<Offset> Producer::send(const std::string& topic, PartitionId partition, ByteSpan value) {
    Record rec;
    rec.value = own(value);
    const std::array<Record, 1> records{std::move(rec)};
    return send_records(topic, partition, records);
}

expected<Offset> Producer::send_keyed(const std::string& topic, PartitionId partition, ByteSpan key,
                                      ByteSpan value) {
    Record rec;
    rec.key = own(key);
    rec.value = own(value);
    const std::array<Record, 1> records{std::move(rec)};
    return send_records(topic, partition, records);
}

expected<Offset> Producer::send_tombstone(const std::string& topic, PartitionId partition,
                                          ByteSpan key) {
    Record rec;
    rec.key = own(key);
    rec.value = std::nullopt;  // tombstone: borra la clave en la compactación.
    const std::array<Record, 1> records{std::move(rec)};
    return send_records(topic, partition, records);
}

expected<Offset> Producer::send_batch(const std::string& topic, PartitionId partition,
                                      std::span<const ByteSpan> values) {
    std::vector<Record> records;
    records.reserve(values.size());
    for (const ByteSpan value : values) {
        Record rec;
        rec.value = own(value);
        records.push_back(std::move(rec));
    }
    return send_records(topic, partition, records);
}

expected<Offset> Producer::send_records(const std::string& topic, PartitionId partition,
                                        std::span<const Record> records) {
    RecordBatchBuilder builder;
    for (const Record& rec : records) {
        builder.add(rec);
    }
    const RecordBatch batch = builder.build(/*header=*/{}, codec_);
    Buffer batch_buf;
    batch.encode(batch_buf);

    ProduceRequest req;
    req.topic = topic;
    req.partition = partition;
    req.batch = batch_buf.as_span();

    const expected<ProduceResponse> resp = client_.produce(req);
    if (!resp) {
        return std::unexpected(resp.error());
    }
    if (resp->error_code != WireError::None) {
        return std::unexpected(to_error(resp->error_code));
    }
    return resp->base_offset;
}

}  // namespace nexus
