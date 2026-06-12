#include "common/record.hpp"

#include <array>
#include <cstddef>
#include <utility>

#include "common/crc32c.hpp"

namespace nexus {
namespace {

// Posiciones de los campos de la cabecera fija (little-endian).
constexpr std::size_t kBaseOffsetPos = 0;      // i64
constexpr std::size_t kLengthPos = 8;          // i32 (bytes tras este campo)
constexpr std::size_t kCrcPos = 12;            // u32
constexpr std::size_t kAttrsPos = 16;          // u16 (inicio de la región cubierta por el CRC)
constexpr std::size_t kProducerIdPos = 18;     // i64
constexpr std::size_t kProducerEpochPos = 26;  // i16
constexpr std::size_t kBaseSequencePos = 28;   // i32
constexpr std::size_t kRecordCountPos = 32;    // i32
// kHeaderSize (= 36, inicio de los records) vive en RecordBatch::kHeaderSize (cabecera pública).
constexpr std::size_t kLengthFieldEnd = kLengthPos + 4;  // length cuenta a partir de aquí

}  // namespace

RecordBatch::RecordBatch(RecordBatchHeader header, std::vector<std::byte> records)
    : header_(header), records_(std::move(records)) {}

void RecordBatch::encode(Buffer& out) const {
    std::array<std::byte, kHeaderSize> hdr{};
    const MutByteSpan h{hdr};

    // Campos cubiertos por el CRC (desde kAttrsPos).
    store_le<std::uint16_t>(header_.attrs, h.subspan(kAttrsPos));
    store_le<std::int64_t>(header_.producer_id, h.subspan(kProducerIdPos));
    store_le<std::int16_t>(header_.producer_epoch, h.subspan(kProducerEpochPos));
    store_le<std::int32_t>(header_.base_sequence, h.subspan(kBaseSequencePos));
    store_le<std::int32_t>(header_.record_count, h.subspan(kRecordCountPos));

    // CRC32C sobre [attrs .. fin] = cola de cabecera + records (encadenado).
    const Crc crc = crc32c(records_, crc32c(h.subspan(kAttrsPos)));

    // Campos no cubiertos por el CRC.
    store_le<std::int64_t>(header_.base_offset, h.subspan(kBaseOffsetPos));
    const auto length =
        static_cast<std::int32_t>((kHeaderSize - kLengthFieldEnd) + records_.size());
    store_le<std::int32_t>(length, h.subspan(kLengthPos));
    store_le<std::uint32_t>(crc, h.subspan(kCrcPos));

    out.append(h);
    out.append(records_);
}

expected<RecordBatch> RecordBatch::decode(ByteSpan data) {
    if (data.size() < kHeaderSize) {
        return make_error(ErrorCode::Corrupt, "batch truncado: cabecera incompleta");
    }
    // Decodificador defensivo: no confiar en `length` sin acotarlo (§7.9).
    const auto length = load_le<std::int32_t>(data.subspan(kLengthPos));
    if (length < 0) {
        return make_error(ErrorCode::Corrupt, "length negativo");
    }
    const std::size_t total = kLengthFieldEnd + static_cast<std::size_t>(length);
    if (total < kHeaderSize || total > data.size()) {
        return make_error(ErrorCode::Corrupt, "batch truncado: length inconsistente");
    }

    const auto stored_crc = load_le<std::uint32_t>(data.subspan(kCrcPos));
    const ByteSpan covered = data.subspan(kAttrsPos, total - kAttrsPos);
    if (crc32c(covered) != stored_crc) {
        return make_error(ErrorCode::Corrupt, "CRC32C no coincide");
    }

    RecordBatchHeader header;
    header.base_offset = load_le<std::int64_t>(data.subspan(kBaseOffsetPos));
    header.attrs = load_le<std::uint16_t>(data.subspan(kAttrsPos));
    header.producer_id = load_le<std::int64_t>(data.subspan(kProducerIdPos));
    header.producer_epoch = load_le<std::int16_t>(data.subspan(kProducerEpochPos));
    header.base_sequence = load_le<std::int32_t>(data.subspan(kBaseSequencePos));
    header.record_count = load_le<std::int32_t>(data.subspan(kRecordCountPos));

    const ByteSpan rec = data.subspan(kHeaderSize, total - kHeaderSize);
    return RecordBatch{header, std::vector<std::byte>{rec.begin(), rec.end()}};
}

expected<RecordBatchView> RecordBatch::peek(ByteSpan data) {
    if (data.size() < kHeaderSize) {
        return make_error(ErrorCode::Corrupt, "batch truncado: cabecera incompleta");
    }
    // Decodificador defensivo: acotar `length` antes de derivar el tamaño on-disk (§7.9).
    const auto length = load_le<std::int32_t>(data.subspan(kLengthPos));
    if (length < 0) {
        return make_error(ErrorCode::Corrupt, "length negativo");
    }
    const std::size_t total = kLengthFieldEnd + static_cast<std::size_t>(length);
    if (total < kHeaderSize) {
        return make_error(ErrorCode::Corrupt, "length inconsistente");
    }
    return RecordBatchView{
        .base_offset = load_le<std::int64_t>(data.subspan(kBaseOffsetPos)),
        .record_count = load_le<std::int32_t>(data.subspan(kRecordCountPos)),
        .encoded_size = total,
    };
}

Offset RecordBatch::last_offset() const noexcept {
    return header_.base_offset + header_.record_count - 1;
}

}  // namespace nexus
