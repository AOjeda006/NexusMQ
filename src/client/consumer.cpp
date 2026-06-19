/// @file   client/consumer.cpp
/// @brief  Implementación del Consumer.
/// @ingroup client

#include "client/consumer.hpp"

#include <utility>
#include <vector>

#include "client/client.hpp"
#include "common/bytes.hpp"
#include "common/record.hpp"
#include "common/record_codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/messages.hpp"

namespace nexus {

expected<std::vector<ConsumedRecord>> Consumer::poll(std::int32_t max_bytes) {
    FetchRequest req;
    req.topic = topic_;
    req.partition = partition_;
    req.fetch_offset = position_;
    req.max_bytes = max_bytes;

    const expected<FetchResponse> resp = client_.fetch(req);
    if (!resp) {
        return std::unexpected(resp.error());
    }
    if (resp->error_code != WireError::None) {
        return std::unexpected(to_error(resp->error_code));
    }

    std::vector<ConsumedRecord> out;
    ByteSpan remaining = resp->batches;
    while (!remaining.empty()) {
        // Acota el batch antes de decodificarlo: una cola parcial (recortada por max_bytes) se
        // ignora limpiamente en lugar de fallar.
        const expected<RecordBatchView> view = RecordBatch::peek(remaining);
        if (!view || view->encoded_size > remaining.size()) {
            break;
        }
        const expected<RecordBatch> batch =
            RecordBatch::decode(remaining.subspan(0, view->encoded_size));
        if (!batch) {
            break;
        }
        const expected<std::vector<Record>> records = decode_records(*batch);
        if (!records) {
            break;
        }
        for (const Record& rec : *records) {
            out.push_back(ConsumedRecord{
                .offset = rec.offset, .key = rec.key, .value = rec.value, .headers = rec.headers});
        }
        position_ = batch->last_offset() + 1;
        remaining = remaining.subspan(view->encoded_size);
    }
    return out;
}

}  // namespace nexus
