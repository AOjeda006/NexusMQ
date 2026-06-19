// Pruebas de make_dead_letter: construcción del record DLQ (clave/valor + metadatos en headers).
#include "client/dead_letter.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "client/consumer.hpp"
#include "common/record_codec.hpp"

namespace {

using nexus::ConsumedRecord;
using nexus::DeadLetterContext;
using nexus::Record;
using nexus::RecordHeader;

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> out(text.size());
    if (!text.empty()) {
        std::memcpy(out.data(), text.data(), text.size());
    }
    return out;
}

std::string_view text(const std::optional<std::vector<std::byte>>& field) {
    return std::string_view{
        reinterpret_cast<const char*>(field->data()),  // NOLINT(*-reinterpret-*)
        field->size()};
}

// Valor del header con clave @p name (vacío si no está).
std::string_view header(const std::vector<RecordHeader>& headers, std::string_view name) {
    for (const RecordHeader& candidate : headers) {
        if (candidate.key == name && candidate.value.has_value()) {
            return text(candidate.value);
        }
    }
    return {};
}

TEST(MakeDeadLetter, ConservaKeyValueYAnadeMetadatos) {
    ConsumedRecord failed;
    failed.key = bytes("order-42");
    failed.value = bytes("payload");
    failed.offset = 7;
    failed.headers.push_back(RecordHeader{.key = "orig", .value = bytes("h")});

    const DeadLetterContext ctx{.source_topic = "orders",
                                .source_partition = 3,
                                .source_offset = 7,
                                .error = "deserialización fallida",
                                .attempts = 5};
    const Record dlq = nexus::make_dead_letter(failed, ctx);

    EXPECT_EQ(dlq.key, failed.key);
    EXPECT_EQ(dlq.value, failed.value);
    // Header original conservado.
    EXPECT_EQ(header(dlq.headers, "orig"), "h");
    // Metadatos x-dlq-*.
    EXPECT_EQ(header(dlq.headers, nexus::kDlqTopicHeader), "orders");
    EXPECT_EQ(header(dlq.headers, nexus::kDlqPartitionHeader), "3");
    EXPECT_EQ(header(dlq.headers, nexus::kDlqOffsetHeader), "7");
    EXPECT_EQ(header(dlq.headers, nexus::kDlqErrorHeader), "deserialización fallida");
    EXPECT_EQ(header(dlq.headers, nexus::kDlqAttemptsHeader), "5");
}

TEST(MakeDeadLetter, TombstoneSigueSiendoTombstone) {
    ConsumedRecord failed;
    failed.key = bytes("k");
    failed.value = std::nullopt;  // tombstone
    const DeadLetterContext ctx{.source_topic = "t",
                                .source_partition = 0,
                                .source_offset = 1,
                                .error = "x",
                                .attempts = 1};
    const Record dlq = nexus::make_dead_letter(failed, ctx);

    EXPECT_EQ(dlq.key, failed.key);
    EXPECT_FALSE(dlq.value.has_value());
    EXPECT_EQ(header(dlq.headers, nexus::kDlqErrorHeader), "x");
}

}  // namespace
