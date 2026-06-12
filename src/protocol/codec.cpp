#include "protocol/codec.hpp"

#include <array>

#include "common/types.hpp"
#include "common/varint.hpp"

namespace nexus {

void Encoder::put_u8(std::uint8_t value) {
    const std::array<std::byte, 1> tmp{std::byte{value}};
    out_.append(tmp);
}

void Encoder::put_u16(std::uint16_t value) {
    std::array<std::byte, sizeof(value)> tmp{};
    store_le(value, tmp);
    out_.append(tmp);
}

void Encoder::put_u32(std::uint32_t value) {
    std::array<std::byte, sizeof(value)> tmp{};
    store_le(value, tmp);
    out_.append(tmp);
}

void Encoder::put_i16(std::int16_t value) {
    std::array<std::byte, sizeof(value)> tmp{};
    store_le(value, tmp);
    out_.append(tmp);
}

void Encoder::put_i32(std::int32_t value) {
    std::array<std::byte, sizeof(value)> tmp{};
    store_le(value, tmp);
    out_.append(tmp);
}

void Encoder::put_i64(std::int64_t value) {
    std::array<std::byte, sizeof(value)> tmp{};
    store_le(value, tmp);
    out_.append(tmp);
}

void Encoder::put_varint(std::uint64_t value) {
    std::array<std::byte, kMaxVarintBytes> tmp{};
    const std::size_t written = nexus::put_varint(value, tmp);
    out_.append(ByteSpan{tmp.data(), written});
}

void Encoder::put_bytes(ByteSpan data) {
    put_varint(data.size());
    out_.append(data);
}

void Encoder::put_string(std::string_view text) {
    put_varint(text.size());
    // Las cadenas viajan como UTF-8: bytes opacos para el codec.
    out_.append(ByteSpan{reinterpret_cast<const std::byte*>(text.data()),  // NOLINT
                         text.size()});
}

expected<ByteSpan> Decoder::take(std::size_t n) {
    if (n > remaining()) {
        return make_error(ErrorCode::InvalidArgument, "decode fuera de límites");
    }
    const ByteSpan span = in_.subspan(pos_, n);
    pos_ += n;
    return span;
}

expected<std::uint8_t> Decoder::get_u8() {
    auto span = take(1);
    if (!span) {
        return std::unexpected(span.error());
    }
    return std::to_integer<std::uint8_t>((*span)[0]);
}

expected<std::uint16_t> Decoder::get_u16() {
    auto span = take(sizeof(std::uint16_t));
    if (!span) {
        return std::unexpected(span.error());
    }
    return load_le<std::uint16_t>(*span);
}

expected<std::uint32_t> Decoder::get_u32() {
    auto span = take(sizeof(std::uint32_t));
    if (!span) {
        return std::unexpected(span.error());
    }
    return load_le<std::uint32_t>(*span);
}

expected<std::int16_t> Decoder::get_i16() {
    auto span = take(sizeof(std::int16_t));
    if (!span) {
        return std::unexpected(span.error());
    }
    return load_le<std::int16_t>(*span);
}

expected<std::int32_t> Decoder::get_i32() {
    auto span = take(sizeof(std::int32_t));
    if (!span) {
        return std::unexpected(span.error());
    }
    return load_le<std::int32_t>(*span);
}

expected<std::int64_t> Decoder::get_i64() {
    auto span = take(sizeof(std::int64_t));
    if (!span) {
        return std::unexpected(span.error());
    }
    return load_le<std::int64_t>(*span);
}

expected<std::uint64_t> Decoder::get_varint() {
    ByteSpan rest = in_.subspan(pos_);
    auto value = nexus::get_varint(rest);
    if (!value) {
        return std::unexpected(value.error());
    }
    pos_ = in_.size() - rest.size();
    return *value;
}

expected<ByteSpan> Decoder::get_bytes() {
    auto length = get_varint();
    if (!length) {
        return std::unexpected(length.error());
    }
    return take(static_cast<std::size_t>(*length));
}

expected<std::string_view> Decoder::get_string() {
    auto bytes = get_bytes();
    if (!bytes) {
        return std::unexpected(bytes.error());
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): byte→char es seguro (alias).
    return std::string_view{reinterpret_cast<const char*>(bytes->data()), bytes->size()};
}

}  // namespace nexus
