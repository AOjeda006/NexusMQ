/// @file   kafka/codec.cpp
/// @brief  Implementación del codec del protocolo Kafka (big-endian, decodificador defensivo).
/// @ingroup kafka

#include "kafka/codec.hpp"

#include <array>
#include <limits>

#include "common/varint.hpp"

namespace nexus::kafka {
namespace {

/// Escribe @p value en orden **big-endian** (red) sobre @p out (sizeof(T) bytes).
template <class T>
void store_be(T value, MutByteSpan out) noexcept {
    using Unsigned = std::make_unsigned_t<T>;
    const auto bits = static_cast<Unsigned>(value);
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out[sizeof(T) - 1 - i] = static_cast<std::byte>((bits >> (8 * i)) & 0xFFU);
    }
}

/// Lee un T en orden **big-endian** desde @p in (sizeof(T) bytes).
template <class T>
[[nodiscard]] T load_be(ByteSpan in) noexcept {
    using Unsigned = std::make_unsigned_t<T>;
    Unsigned bits = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        bits = static_cast<Unsigned>((bits << 8U) |
                                     static_cast<Unsigned>(std::to_integer<std::uint8_t>(in[i])));
    }
    return static_cast<T>(bits);
}

/// Bytes opacos de una cadena (UTF-8) para el codec.
[[nodiscard]] ByteSpan as_bytes(std::string_view text) {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};  // NOLINT(*reinterpret*)
}

/// Convierte una vista de bytes en `std::string` (copia).
[[nodiscard]] std::string to_string(ByteSpan bytes) {
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};  // NOLINT(*reinterpret*)
}

template <class T>
void append_be(Buffer& out, T value) {
    std::array<std::byte, sizeof(T)> tmp{};
    store_be(value, tmp);
    out.append(tmp);
}

}  // namespace

void Encoder::put_i8(std::int8_t value) {
    const std::array<std::byte, 1> tmp{static_cast<std::byte>(value)};
    out_.append(tmp);
}

void Encoder::put_i16(std::int16_t value) {
    append_be(out_, value);
}

void Encoder::put_i32(std::int32_t value) {
    append_be(out_, value);
}

void Encoder::put_i64(std::int64_t value) {
    append_be(out_, value);
}

void Encoder::put_u32(std::uint32_t value) {
    append_be(out_, value);
}

void Encoder::put_bool(bool value) {
    put_i8(value ? 1 : 0);
}

void Encoder::put_unsigned_varint(std::uint32_t value) {
    std::array<std::byte, kMaxVarintBytes> tmp{};
    const std::size_t written = nexus::put_varint(value, tmp);
    out_.append(ByteSpan{tmp.data(), written});
}

void Encoder::put_varint(std::int32_t value) {
    std::array<std::byte, kMaxVarintBytes> tmp{};
    const std::size_t written = nexus::put_varint(zigzag_encode(value), tmp);
    out_.append(ByteSpan{tmp.data(), written});
}

void Encoder::put_varlong(std::int64_t value) {
    std::array<std::byte, kMaxVarintBytes> tmp{};
    const std::size_t written = nexus::put_varint(zigzag_encode(value), tmp);
    out_.append(ByteSpan{tmp.data(), written});
}

void Encoder::put_string(std::string_view text) {
    put_i16(static_cast<std::int16_t>(text.size()));
    out_.append(as_bytes(text));
}

void Encoder::put_nullable_string(const std::optional<std::string_view>& text) {
    if (!text) {
        put_i16(-1);
        return;
    }
    put_string(*text);
}

void Encoder::put_compact_string(std::string_view text) {
    put_unsigned_varint(static_cast<std::uint32_t>(text.size() + 1));
    out_.append(as_bytes(text));
}

void Encoder::put_compact_nullable_string(const std::optional<std::string_view>& text) {
    if (!text) {
        put_unsigned_varint(0);
        return;
    }
    put_compact_string(*text);
}

void Encoder::put_bytes(ByteSpan data) {
    put_i32(static_cast<std::int32_t>(data.size()));
    out_.append(data);
}

void Encoder::put_nullable_bytes(const std::optional<ByteSpan>& data) {
    if (!data) {
        put_i32(-1);
        return;
    }
    put_bytes(*data);
}

void Encoder::put_compact_bytes(ByteSpan data) {
    put_unsigned_varint(static_cast<std::uint32_t>(data.size() + 1));
    out_.append(data);
}

void Encoder::put_compact_nullable_bytes(const std::optional<ByteSpan>& data) {
    if (!data) {
        put_unsigned_varint(0);
        return;
    }
    put_compact_bytes(*data);
}

void Encoder::put_array_len(std::int32_t count) {
    put_i32(count);
}

void Encoder::put_compact_array_len(std::int32_t count) {
    if (count < 0) {
        put_unsigned_varint(0);  // nulo
        return;
    }
    put_unsigned_varint(static_cast<std::uint32_t>(count) + 1);
}

void Encoder::put_empty_tagged_fields() {
    put_unsigned_varint(0);
}

void Encoder::put_raw(ByteSpan data) {
    out_.append(data);
}

expected<ByteSpan> Decoder::take(std::size_t n) {
    if (n > remaining()) {
        return make_error(ErrorCode::InvalidArgument, "decode Kafka fuera de límites");
    }
    const ByteSpan span = in_.subspan(pos_, n);
    pos_ += n;
    return span;
}

expected<void> Decoder::skip(std::size_t n) {
    const expected<ByteSpan> span = take(n);
    if (!span) {
        return std::unexpected(span.error());
    }
    return {};
}

expected<std::int8_t> Decoder::get_i8() {
    const expected<ByteSpan> span = take(1);
    if (!span) {
        return std::unexpected(span.error());
    }
    return static_cast<std::int8_t>(std::to_integer<std::uint8_t>((*span)[0]));
}

expected<std::int16_t> Decoder::get_i16() {
    const expected<ByteSpan> span = take(sizeof(std::int16_t));
    if (!span) {
        return std::unexpected(span.error());
    }
    return load_be<std::int16_t>(*span);
}

expected<std::int32_t> Decoder::get_i32() {
    const expected<ByteSpan> span = take(sizeof(std::int32_t));
    if (!span) {
        return std::unexpected(span.error());
    }
    return load_be<std::int32_t>(*span);
}

expected<std::int64_t> Decoder::get_i64() {
    const expected<ByteSpan> span = take(sizeof(std::int64_t));
    if (!span) {
        return std::unexpected(span.error());
    }
    return load_be<std::int64_t>(*span);
}

expected<std::uint32_t> Decoder::get_u32() {
    const expected<ByteSpan> span = take(sizeof(std::uint32_t));
    if (!span) {
        return std::unexpected(span.error());
    }
    return load_be<std::uint32_t>(*span);
}

expected<bool> Decoder::get_bool() {
    const expected<std::int8_t> value = get_i8();
    if (!value) {
        return std::unexpected(value.error());
    }
    return *value != 0;
}

expected<std::uint32_t> Decoder::get_unsigned_varint() {
    ByteSpan rest = in_.subspan(pos_);
    const expected<std::uint64_t> value = nexus::get_varint(rest);  // avanza `rest`
    if (!value) {
        return std::unexpected(value.error());
    }
    if (*value > std::numeric_limits<std::uint32_t>::max()) {
        return make_error(ErrorCode::Corrupt, "unsigned varint excede 32 bits");
    }
    pos_ = in_.size() - rest.size();
    return static_cast<std::uint32_t>(*value);
}

expected<std::int32_t> Decoder::get_varint() {
    ByteSpan rest = in_.subspan(pos_);
    const expected<std::uint64_t> value = nexus::get_varint(rest);
    if (!value) {
        return std::unexpected(value.error());
    }
    pos_ = in_.size() - rest.size();
    return static_cast<std::int32_t>(zigzag_decode(*value));
}

expected<std::int64_t> Decoder::get_varlong() {
    ByteSpan rest = in_.subspan(pos_);
    const expected<std::uint64_t> value = nexus::get_varint(rest);
    if (!value) {
        return std::unexpected(value.error());
    }
    pos_ = in_.size() - rest.size();
    return zigzag_decode(*value);
}

expected<std::string> Decoder::get_string() {
    const expected<std::int16_t> len = get_i16();
    if (!len) {
        return std::unexpected(len.error());
    }
    if (*len < 0) {
        return make_error(ErrorCode::Corrupt, "STRING no admite longitud negativa");
    }
    const expected<ByteSpan> bytes = take(static_cast<std::size_t>(*len));
    if (!bytes) {
        return std::unexpected(bytes.error());
    }
    return to_string(*bytes);
}

expected<std::optional<std::string>> Decoder::get_nullable_string() {
    const expected<std::int16_t> len = get_i16();
    if (!len) {
        return std::unexpected(len.error());
    }
    if (*len < 0) {
        return std::optional<std::string>{};
    }
    const expected<ByteSpan> bytes = take(static_cast<std::size_t>(*len));
    if (!bytes) {
        return std::unexpected(bytes.error());
    }
    return std::optional<std::string>{to_string(*bytes)};
}

expected<std::string> Decoder::get_compact_string() {
    const expected<std::uint32_t> len = get_unsigned_varint();
    if (!len) {
        return std::unexpected(len.error());
    }
    if (*len == 0) {
        return make_error(ErrorCode::Corrupt, "COMPACT_STRING no admite nulo");
    }
    const expected<ByteSpan> bytes = take(*len - 1);
    if (!bytes) {
        return std::unexpected(bytes.error());
    }
    return to_string(*bytes);
}

expected<std::optional<std::string>> Decoder::get_compact_nullable_string() {
    const expected<std::uint32_t> len = get_unsigned_varint();
    if (!len) {
        return std::unexpected(len.error());
    }
    if (*len == 0) {
        return std::optional<std::string>{};
    }
    const expected<ByteSpan> bytes = take(*len - 1);
    if (!bytes) {
        return std::unexpected(bytes.error());
    }
    return std::optional<std::string>{to_string(*bytes)};
}

expected<ByteSpan> Decoder::get_bytes() {
    const expected<std::int32_t> len = get_i32();
    if (!len) {
        return std::unexpected(len.error());
    }
    if (*len < 0) {
        return make_error(ErrorCode::Corrupt, "BYTES no admite longitud negativa");
    }
    return take(static_cast<std::size_t>(*len));
}

expected<std::optional<ByteSpan>> Decoder::get_nullable_bytes() {
    const expected<std::int32_t> len = get_i32();
    if (!len) {
        return std::unexpected(len.error());
    }
    if (*len < 0) {
        return std::optional<ByteSpan>{};
    }
    const expected<ByteSpan> bytes = take(static_cast<std::size_t>(*len));
    if (!bytes) {
        return std::unexpected(bytes.error());
    }
    return std::optional<ByteSpan>{*bytes};
}

expected<ByteSpan> Decoder::get_compact_bytes() {
    const expected<std::uint32_t> len = get_unsigned_varint();
    if (!len) {
        return std::unexpected(len.error());
    }
    if (*len == 0) {
        return make_error(ErrorCode::Corrupt, "COMPACT_BYTES no admite nulo");
    }
    return take(*len - 1);
}

expected<std::optional<ByteSpan>> Decoder::get_compact_nullable_bytes() {
    const expected<std::uint32_t> len = get_unsigned_varint();
    if (!len) {
        return std::unexpected(len.error());
    }
    if (*len == 0) {
        return std::optional<ByteSpan>{};
    }
    const expected<ByteSpan> bytes = take(*len - 1);
    if (!bytes) {
        return std::unexpected(bytes.error());
    }
    return std::optional<ByteSpan>{*bytes};
}

expected<std::int32_t> Decoder::get_array_len() {
    return get_i32();
}

expected<std::int32_t> Decoder::get_compact_array_len() {
    const expected<std::uint32_t> raw = get_unsigned_varint();
    if (!raw) {
        return std::unexpected(raw.error());
    }
    if (*raw == 0) {
        return std::int32_t{-1};  // nulo
    }
    return static_cast<std::int32_t>(*raw - 1);
}

expected<void> Decoder::skip_tagged_fields() {
    const expected<std::uint32_t> count = get_unsigned_varint();
    if (!count) {
        return std::unexpected(count.error());
    }
    for (std::uint32_t i = 0; i < *count; ++i) {
        const expected<std::uint32_t> tag = get_unsigned_varint();
        if (!tag) {
            return std::unexpected(tag.error());
        }
        const expected<std::uint32_t> size = get_unsigned_varint();
        if (!size) {
            return std::unexpected(size.error());
        }
        const expected<ByteSpan> data = take(*size);
        if (!data) {
            return std::unexpected(data.error());
        }
    }
    return {};
}

}  // namespace nexus::kafka
