/// @file   consensus/raft_wire.cpp
/// @brief  Implementación de RaftEnvelope: (de)serialización del sobre inter-nodo (ADR-0025).
/// @ingroup consensus

#include "consensus/raft_wire.hpp"

#include <cstdint>
#include <string>
#include <utility>
#include <variant>

#include "protocol/codec.hpp"

namespace nexus {

namespace {

/// @brief Discriminante de wire del RPC: coincide con el orden del `variant` de `RaftMessage`.
/// @details Lo emite `payload.index()` al codificar y lo consume el `switch` al decodificar; el
///   orden debe mantenerse alineado con la declaración del `variant` en `raft_rpc.hpp`.
enum class RaftMessageType : std::uint8_t {
    RequestVoteArgs = 0,
    RequestVoteReply = 1,
    AppendEntriesArgs = 2,
    AppendEntriesReply = 3,
    TimeoutNowArgs = 4,
    InstallSnapshotArgs = 5,
    InstallSnapshotReply = 6,
};

/// Decodifica un RPC de tipo @p T y lo deja como payload de @p message; propaga el error si falla.
template <class T>
[[nodiscard]] expected<void> decode_payload(Decoder& dec, RaftMessage& message) {
    expected<T> rpc = T::decode(dec);
    if (!rpc) {
        return std::unexpected(rpc.error());
    }
    message.payload = std::move(*rpc);
    return {};
}

}  // namespace

void RaftEnvelope::encode(Encoder& enc) const {
    enc.put_string(topic);
    enc.put_i32(partition);
    enc.put_i32(message.from);
    enc.put_i32(message.to);
    enc.put_u8(static_cast<std::uint8_t>(message.payload.index()));
    std::visit([&enc](const auto& rpc) { rpc.encode(enc); }, message.payload);
}

expected<RaftEnvelope> RaftEnvelope::decode(Decoder& dec) {
    expected<std::string_view> topic = dec.get_string();
    if (!topic) {
        return std::unexpected(topic.error());
    }
    expected<std::int32_t> partition = dec.get_i32();
    if (!partition) {
        return std::unexpected(partition.error());
    }
    expected<std::int32_t> from = dec.get_i32();
    if (!from) {
        return std::unexpected(from.error());
    }
    expected<std::int32_t> to = dec.get_i32();
    if (!to) {
        return std::unexpected(to.error());
    }
    expected<std::uint8_t> type = dec.get_u8();
    if (!type) {
        return std::unexpected(type.error());
    }

    RaftEnvelope envelope{.topic = std::string{*topic},
                          .partition = *partition,
                          .message = RaftMessage{.from = *from, .to = *to, .payload = {}}};

    expected<void> payload;
    switch (static_cast<RaftMessageType>(*type)) {
        case RaftMessageType::RequestVoteArgs:
            payload = decode_payload<RequestVoteArgs>(dec, envelope.message);
            break;
        case RaftMessageType::RequestVoteReply:
            payload = decode_payload<RequestVoteReply>(dec, envelope.message);
            break;
        case RaftMessageType::AppendEntriesArgs:
            payload = decode_payload<AppendEntriesArgs>(dec, envelope.message);
            break;
        case RaftMessageType::AppendEntriesReply:
            payload = decode_payload<AppendEntriesReply>(dec, envelope.message);
            break;
        case RaftMessageType::TimeoutNowArgs:
            payload = decode_payload<TimeoutNowArgs>(dec, envelope.message);
            break;
        case RaftMessageType::InstallSnapshotArgs:
            payload = decode_payload<InstallSnapshotArgs>(dec, envelope.message);
            break;
        case RaftMessageType::InstallSnapshotReply:
            payload = decode_payload<InstallSnapshotReply>(dec, envelope.message);
            break;
        default:
            return make_error(ErrorCode::InvalidArgument, "tipo de RPC de Raft desconocido");
    }
    if (!payload) {
        return std::unexpected(payload.error());
    }
    return envelope;
}

}  // namespace nexus
