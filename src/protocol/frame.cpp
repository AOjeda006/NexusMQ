#include "protocol/frame.hpp"

#include "protocol/codec.hpp"

namespace nexus {

void FrameHeader::encode(Encoder& enc) const {
    enc.put_u32(length);
    enc.put_u16(static_cast<std::uint16_t>(api_key));
    enc.put_u16(api_version);
    enc.put_u32(correlation_id);
    enc.put_u16(flags);
}

expected<FrameHeader> FrameHeader::decode(Decoder& dec) {
    FrameHeader header;

    auto length = dec.get_u32();
    if (!length) {
        return std::unexpected(length.error());
    }
    header.length = *length;

    auto api_key = dec.get_u16();
    if (!api_key) {
        return std::unexpected(api_key.error());
    }
    if (*api_key > static_cast<std::uint16_t>(ApiKey::DeleteTopic)) {
        return make_error(ErrorCode::InvalidArgument, "api_key desconocido");
    }
    header.api_key = static_cast<ApiKey>(*api_key);

    auto api_version = dec.get_u16();
    if (!api_version) {
        return std::unexpected(api_version.error());
    }
    header.api_version = *api_version;

    auto correlation_id = dec.get_u32();
    if (!correlation_id) {
        return std::unexpected(correlation_id.error());
    }
    header.correlation_id = *correlation_id;

    auto flags = dec.get_u16();
    if (!flags) {
        return std::unexpected(flags.error());
    }
    header.flags = *flags;

    return header;
}

}  // namespace nexus
