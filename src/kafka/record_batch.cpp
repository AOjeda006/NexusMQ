/// @file   kafka/record_batch.cpp
/// @brief  Implementación de la inspección/reescritura de un RecordBatch v2 de Kafka — F7f.
/// @ingroup kafka

#include "kafka/record_batch.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "kafka/codec.hpp"

namespace nexus::kafka {

namespace {

/// Bytes de los dos primeros campos (`baseOffset:i64 | batchLength:i32`); `batchLength` cuenta los
/// bytes que vienen **tras** él, de modo que el batch completo ocupa `kBatchPrefixSize +
/// batchLength`.
constexpr std::size_t kBatchPrefixSize = 12;

/// Mínimo de `batchLength`: los campos fijos tras él (`= kRecordBatchHeaderSize -
/// kBatchPrefixSize`), aún sin records.
constexpr std::int32_t kMinBatchLength = kRecordBatchHeaderSize - kBatchPrefixSize;

}  // namespace

expected<RecordBatchInfo> peek_record_batch(ByteSpan batch) {
    Decoder dec{batch};
    const expected<std::int64_t> base_offset = dec.get_i64();
    if (!base_offset) {
        return std::unexpected(base_offset.error());
    }
    const expected<std::int32_t> batch_length = dec.get_i32();
    if (!batch_length) {
        return std::unexpected(batch_length.error());
    }
    if (*batch_length < kMinBatchLength) {
        return make_error(ErrorCode::Corrupt,
                          "RecordBatch: batchLength menor que la cabecera fija");
    }
    const std::size_t encoded_size = kBatchPrefixSize + static_cast<std::size_t>(*batch_length);
    if (encoded_size > batch.size()) {
        return make_error(ErrorCode::Corrupt,
                          "RecordBatch: truncado (batchLength excede los bytes)");
    }

    if (const expected<void> ok = dec.skip(sizeof(std::int32_t)); !ok) {  // partitionLeaderEpoch.
        return std::unexpected(ok.error());
    }
    const expected<std::int8_t> magic = dec.get_i8();
    if (!magic) {
        return std::unexpected(magic.error());
    }
    if (*magic != kRecordBatchMagicV2) {
        return make_error(ErrorCode::Corrupt, "RecordBatch: magic no es v2");
    }
    // crc (lo validó el productor) + attributes: el broker no los reinterpreta.
    if (const expected<void> ok = dec.skip(sizeof(std::uint32_t) + sizeof(std::int16_t)); !ok) {
        return std::unexpected(ok.error());
    }
    const expected<std::int32_t> last_offset_delta = dec.get_i32();
    if (!last_offset_delta) {
        return std::unexpected(last_offset_delta.error());
    }
    // baseTimestamp + maxTimestamp + producerId + producerEpoch + baseSequence (no necesarios).
    constexpr std::size_t kSkipBeforeCount =
        (3 * sizeof(std::int64_t)) + sizeof(std::int16_t) + sizeof(std::int32_t);
    if (const expected<void> ok = dec.skip(kSkipBeforeCount); !ok) {
        return std::unexpected(ok.error());
    }
    const expected<std::int32_t> record_count = dec.get_i32();
    if (!record_count) {
        return std::unexpected(record_count.error());
    }
    if (*record_count < 0) {
        return make_error(ErrorCode::Corrupt, "RecordBatch: recordCount negativo");
    }
    return RecordBatchInfo{.base_offset = *base_offset,
                           .record_count = *record_count,
                           .last_offset_delta = *last_offset_delta,
                           .encoded_size = encoded_size};
}

void set_base_offset(std::span<std::byte> batch, std::int64_t base_offset) noexcept {
    const auto value = static_cast<std::uint64_t>(base_offset);
    for (std::size_t i = 0; i < sizeof(std::uint64_t); ++i) {
        const auto shift = static_cast<unsigned>(8 * (sizeof(std::uint64_t) - 1 - i));
        batch[i] = static_cast<std::byte>((value >> shift) & 0xFFU);
    }
}

}  // namespace nexus::kafka
