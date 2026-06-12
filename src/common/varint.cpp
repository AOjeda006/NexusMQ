#include "common/varint.hpp"

namespace nexus {
namespace {

constexpr std::uint8_t kContinuationBit = 0x80;  // bit alto: hay más bytes
constexpr std::uint8_t kPayloadMask = 0x7F;      // 7 bits útiles por byte
constexpr unsigned kPayloadBits = 7;

}  // namespace

std::size_t put_varint(std::uint64_t value, MutByteSpan out) noexcept {
    std::size_t i = 0;
    while (value >= kContinuationBit) {
        out[i] = static_cast<std::byte>((value & kPayloadMask) | kContinuationBit);
        value >>= kPayloadBits;
        ++i;
    }
    out[i] = static_cast<std::byte>(value);
    return i + 1;
}

expected<std::uint64_t> get_varint(ByteSpan& in) {
    std::uint64_t result = 0;
    unsigned shift = 0;
    for (std::size_t i = 0; i < in.size() && i < kMaxVarintBytes; ++i) {
        const auto byte = std::to_integer<std::uint8_t>(in[i]);
        result |= static_cast<std::uint64_t>(byte & kPayloadMask) << shift;
        if ((byte & kContinuationBit) == 0) {
            in = in.subspan(i + 1);
            return result;
        }
        shift += kPayloadBits;
    }
    return make_error(ErrorCode::InvalidArgument, "varint truncado o demasiado largo");
}

}  // namespace nexus
