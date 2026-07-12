/// @file   common/control_record.cpp
/// @brief  Implementación del codec de control records (marcadores COMMIT/ABORT).

#include "common/control_record.hpp"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "common/record_codec.hpp"

namespace nexus {

namespace {

/// ¿@p type es un valor de marcador reconocido (Abort/Commit)?
[[nodiscard]] bool is_known_type(std::int16_t type) noexcept {
    return type == static_cast<std::int16_t>(ControlRecordType::Abort) ||
           type == static_cast<std::int16_t>(ControlRecordType::Commit);
}

}  // namespace

std::vector<std::byte> encode_control_key(const EndTxnMarker& marker) {
    std::vector<std::byte> key(kControlKeySize);
    const MutByteSpan out{key};
    store_le<std::int16_t>(marker.version, out.subspan(0));
    store_le<std::int16_t>(static_cast<std::int16_t>(marker.type), out.subspan(2));
    return key;
}

std::vector<std::byte> encode_control_value(const EndTxnMarker& marker) {
    std::vector<std::byte> value(kControlValueSize);
    const MutByteSpan out{value};
    store_le<std::int16_t>(marker.version, out.subspan(0));
    store_le<std::int32_t>(marker.coordinator_epoch, out.subspan(2));
    return value;
}

expected<EndTxnMarker> decode_end_txn_marker(ByteSpan key, ByteSpan value) {
    if (key.size() < kControlKeySize) {
        return make_error(ErrorCode::Corrupt, "control record: clave truncada");
    }
    if (value.size() < kControlValueSize) {
        return make_error(ErrorCode::Corrupt, "control record: valor truncado");
    }
    const auto key_version = load_le<std::int16_t>(key.subspan(0));
    const auto type = load_le<std::int16_t>(key.subspan(2));
    const auto value_version = load_le<std::int16_t>(value.subspan(0));
    const auto coordinator_epoch = load_le<std::int32_t>(value.subspan(2));

    if (key_version != value_version) {
        return make_error(ErrorCode::Corrupt,
                          "control record: versiones de clave/valor discordantes");
    }
    if (key_version != kControlRecordVersion) {
        return make_error(ErrorCode::Unsupported, "control record: versión no soportada");
    }
    if (!is_known_type(type)) {
        return make_error(ErrorCode::Corrupt, "control record: tipo desconocido");
    }
    return EndTxnMarker{.type = static_cast<ControlRecordType>(type),
                        .coordinator_epoch = coordinator_epoch,
                        .version = key_version};
}

RecordBatch build_control_batch(const EndTxnMarker& marker, ProducerId producer_id,
                                std::int16_t producer_epoch) {
    Record rec;
    rec.key = encode_control_key(marker);
    rec.value = encode_control_value(marker);

    RecordBatchBuilder builder;
    builder.add(std::move(rec));

    RecordBatchHeader header;
    header.attrs = attrs_with_control(attrs_with_transactional(0, true), true);
    header.producer_id = producer_id;
    header.producer_epoch = producer_epoch;
    header.base_sequence = -1;  // el marcador no consume secuencia del productor.
    return builder.build(header, Codec::None);
}

expected<EndTxnMarker> parse_control_batch(const RecordBatch& batch) {
    if (!is_control(batch.header().attrs)) {
        return make_error(ErrorCode::InvalidArgument, "batch no es de control");
    }
    const expected<std::vector<Record>> records = decode_records(batch);
    if (!records) {
        return std::unexpected(records.error());
    }
    if (records->size() != 1) {
        return make_error(ErrorCode::InvalidArgument,
                          "batch de control debe tener un único record");
    }
    const Record& rec = records->front();
    if (!rec.key || !rec.value) {
        return make_error(ErrorCode::InvalidArgument, "control record sin clave o valor");
    }
    return decode_end_txn_marker(ByteSpan{*rec.key}, ByteSpan{*rec.value});
}

}  // namespace nexus
