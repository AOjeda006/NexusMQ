/// @file   client/dead_letter.cpp
/// @brief  Implementación del DeadLetterRouter (F4).
/// @ingroup client

#include "client/dead_letter.hpp"

#include <array>
#include <cstddef>
#include <cstring>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "client/producer.hpp"

namespace nexus {
namespace {

/// Bytes (copia) a partir de una cadena.
std::vector<std::byte> bytes_of(std::string_view text) {
    std::vector<std::byte> out(text.size());
    if (!text.empty()) {
        std::memcpy(out.data(), text.data(), text.size());
    }
    return out;
}

/// Añade un header `name=value` (value como UTF-8) a @p headers.
void add_header(std::vector<RecordHeader>& headers, std::string_view name, std::string_view value) {
    headers.push_back(RecordHeader{.key = std::string{name}, .value = bytes_of(value)});
}

}  // namespace

Record make_dead_letter(const ConsumedRecord& failed, const DeadLetterContext& ctx) {
    Record rec;
    rec.key = failed.key;
    rec.value = failed.value;  // conserva el payload (un tombstone sigue siéndolo).
    rec.headers = failed.headers;
    add_header(rec.headers, kDlqTopicHeader, ctx.source_topic);
    add_header(rec.headers, kDlqPartitionHeader, std::to_string(ctx.source_partition));
    add_header(rec.headers, kDlqOffsetHeader, std::to_string(ctx.source_offset));
    add_header(rec.headers, kDlqErrorHeader, ctx.error);
    add_header(rec.headers, kDlqAttemptsHeader, std::to_string(ctx.attempts));
    return rec;
}

DeadLetterRouter::DeadLetterRouter(Producer& producer, std::string dlq_topic,
                                   PartitionId dlq_partition) noexcept
    : producer_(producer), dlq_topic_(std::move(dlq_topic)), dlq_partition_(dlq_partition) {}

expected<Offset> DeadLetterRouter::route(const ConsumedRecord& failed,
                                         const DeadLetterContext& ctx) {
    const std::array<Record, 1> records{make_dead_letter(failed, ctx)};
    return producer_.send_records(dlq_topic_, dlq_partition_, records);
}

}  // namespace nexus
