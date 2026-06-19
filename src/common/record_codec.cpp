/// @file   common/record_codec.cpp
/// @brief  Implementación del codec por record (varint/zigzag, decodificador defensivo).
/// @ingroup common

#include "common/record_codec.hpp"

#include <array>
#include <cstring>
#include <span>
#include <utility>

#include "common/varint.hpp"

namespace nexus {
namespace {

/// Escribe @p value como varint zigzag al final de @p out.
void put_zig(std::int64_t value, Buffer& out) {
    std::array<std::byte, kMaxVarintBytes> tmp{};
    const std::size_t n = put_varint(zigzag_encode(value), MutByteSpan{tmp});
    out.append(ByteSpan{tmp.data(), n});
}

/// Escribe un campo de bytes anulable: `len:varint(-1=nulo)` + contenido.
void put_nullable(const std::optional<std::vector<std::byte>>& field, Buffer& out) {
    if (!field) {
        put_zig(-1, out);
        return;
    }
    put_zig(static_cast<std::int64_t>(field->size()), out);
    out.append(ByteSpan{*field});
}

/// Lee un varint zigzag desde @p cursor (lo **avanza**).
[[nodiscard]] expected<std::int64_t> get_zig(ByteSpan& cursor) {
    const expected<std::uint64_t> raw = get_varint(cursor);
    if (!raw) {
        return std::unexpected(raw.error());
    }
    return zigzag_decode(*raw);
}

/// Toma @p n bytes del frente de @p cursor (lo **avanza**); `Corrupt` si no alcanzan.
[[nodiscard]] expected<ByteSpan> take(ByteSpan& cursor, std::size_t n) {
    if (n > cursor.size()) {
        return make_error(ErrorCode::Corrupt, "record truncado");
    }
    const ByteSpan out = cursor.subspan(0, n);
    cursor = cursor.subspan(n);
    return out;
}

/// Lee un campo de bytes anulable desde @p cursor (lo **avanza**).
[[nodiscard]] expected<std::optional<std::vector<std::byte>>> get_nullable(ByteSpan& cursor) {
    const expected<std::int64_t> len = get_zig(cursor);
    if (!len) {
        return std::unexpected(len.error());
    }
    if (*len < 0) {
        return std::optional<std::vector<std::byte>>{};  // nulo
    }
    const expected<ByteSpan> bytes = take(cursor, static_cast<std::size_t>(*len));
    if (!bytes) {
        return std::unexpected(bytes.error());
    }
    return std::optional<std::vector<std::byte>>{
        std::vector<std::byte>{bytes->begin(), bytes->end()}};
}

}  // namespace

void encode_record(const Record& rec, std::int64_t offset_delta, Buffer& out) {
    // Cuerpo en un búfer temporal para conocer su longitud (prefijo varint).
    Buffer body;
    const std::array<std::byte, 1> attributes{std::byte{0}};  // reservado (compresión/flags)
    body.append(ByteSpan{attributes});
    put_zig(rec.timestamp_delta, body);
    put_zig(offset_delta, body);
    put_nullable(rec.key, body);
    put_nullable(rec.value, body);

    put_zig(static_cast<std::int64_t>(rec.headers.size()), body);
    for (const RecordHeader& header : rec.headers) {
        put_zig(static_cast<std::int64_t>(header.key.size()), body);
        body.append(std::as_bytes(std::span{header.key}));
        put_nullable(header.value, body);
    }

    const ByteSpan body_span = body.as_span();
    put_zig(static_cast<std::int64_t>(body_span.size()), out);
    out.append(body_span);
}

expected<Record> decode_record(ByteSpan& cursor, Offset base_offset) {
    const expected<std::int64_t> body_len = get_zig(cursor);
    if (!body_len) {
        return std::unexpected(body_len.error());
    }
    if (*body_len < 0) {
        return make_error(ErrorCode::Corrupt, "longitud de record negativa");
    }
    const expected<ByteSpan> body_span = take(cursor, static_cast<std::size_t>(*body_len));
    if (!body_span) {
        return std::unexpected(body_span.error());
    }

    ByteSpan body = *body_span;
    const expected<ByteSpan> attrs = take(body, 1);  // attributes (reservado)
    if (!attrs) {
        return std::unexpected(attrs.error());
    }
    const expected<std::int64_t> ts_delta = get_zig(body);
    if (!ts_delta) {
        return std::unexpected(ts_delta.error());
    }
    const expected<std::int64_t> offset_delta = get_zig(body);
    if (!offset_delta) {
        return std::unexpected(offset_delta.error());
    }

    Record rec;
    rec.timestamp_delta = *ts_delta;
    rec.offset = base_offset + *offset_delta;

    expected<std::optional<std::vector<std::byte>>> key = get_nullable(body);
    if (!key) {
        return std::unexpected(key.error());
    }
    rec.key = std::move(*key);
    expected<std::optional<std::vector<std::byte>>> value = get_nullable(body);
    if (!value) {
        return std::unexpected(value.error());
    }
    rec.value = std::move(*value);

    const expected<std::int64_t> header_count = get_zig(body);
    if (!header_count) {
        return std::unexpected(header_count.error());
    }
    if (*header_count < 0 || *header_count > kMaxHeadersPerRecord) {
        return make_error(ErrorCode::Corrupt, "nº de headers inválido");
    }
    rec.headers.reserve(static_cast<std::size_t>(*header_count));
    for (std::int64_t i = 0; i < *header_count; ++i) {
        const expected<std::int64_t> key_len = get_zig(body);
        if (!key_len || *key_len < 0) {
            return make_error(ErrorCode::Corrupt, "longitud de clave de header inválida");
        }
        const expected<ByteSpan> key_bytes = take(body, static_cast<std::size_t>(*key_len));
        if (!key_bytes) {
            return std::unexpected(key_bytes.error());
        }
        RecordHeader header;
        header.key.resize(key_bytes->size());
        if (!key_bytes->empty()) {
            std::memcpy(header.key.data(), key_bytes->data(), key_bytes->size());
        }
        expected<std::optional<std::vector<std::byte>>> hval = get_nullable(body);
        if (!hval) {
            return std::unexpected(hval.error());
        }
        header.value = std::move(*hval);
        rec.headers.push_back(std::move(header));
    }
    return rec;
}

expected<std::vector<Record>> decode_records(const RecordBatch& batch) {
    const std::int32_t count = batch.header().record_count;
    if (count < 0) {
        return make_error(ErrorCode::Corrupt, "record_count negativo");
    }
    std::vector<Record> out;
    out.reserve(static_cast<std::size_t>(count));
    ByteSpan cursor = batch.records();
    for (std::int32_t i = 0; i < count; ++i) {
        expected<Record> rec = decode_record(cursor, batch.header().base_offset);
        if (!rec) {
            return std::unexpected(rec.error());
        }
        out.push_back(std::move(*rec));
    }
    return out;
}

RecordBatchBuilder& RecordBatchBuilder::add(Record rec) {
    records_.push_back(std::move(rec));
    return *this;
}

RecordBatch RecordBatchBuilder::build(RecordBatchHeader header) const {
    Buffer records;
    std::int64_t delta = 0;
    for (const Record& rec : records_) {
        encode_record(rec, delta, records);
        ++delta;
    }
    header.record_count = static_cast<std::int32_t>(records_.size());
    const ByteSpan span = records.as_span();
    return RecordBatch{header, std::vector<std::byte>{span.begin(), span.end()}};
}

}  // namespace nexus
