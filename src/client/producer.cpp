/// @file   client/producer.cpp
/// @brief  Implementación del Producer.
/// @ingroup client

#include "client/producer.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "client/client.hpp"
#include "common/record.hpp"
#include "protocol/codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/messages.hpp"

namespace nexus {

expected<Offset> Producer::send(const std::string& topic, PartitionId partition, ByteSpan value) {
    const std::array<ByteSpan, 1> values{value};
    return send_batch(topic, partition, values);
}

expected<Offset> Producer::send_batch(const std::string& topic, PartitionId partition,
                                      std::span<const ByteSpan> values) {
    // Records longitud-prefijo dentro del blob opaco del batch (el broker no los interpreta).
    Buffer records;
    Encoder enc{records};
    for (const ByteSpan value : values) {
        enc.put_bytes(value);
    }
    const ByteSpan records_span = records.as_span();

    RecordBatchHeader header;
    header.record_count = static_cast<std::int32_t>(values.size());
    const RecordBatch batch{header,
                            std::vector<std::byte>{records_span.begin(), records_span.end()}};
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
