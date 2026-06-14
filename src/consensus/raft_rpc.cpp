/// @file   consensus/raft_rpc.cpp
/// @brief  (De)serialización de los RPC de Raft sobre el codec de `nexus-protocol`.
/// @ingroup consensus

#include "consensus/raft_rpc.hpp"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "common/bytes.hpp"
#include "protocol/codec.hpp"

namespace nexus {
namespace {

// Lee un contador de elementos acotado por los bytes restantes (anti-DoS), igual que en messages.
[[nodiscard]] expected<std::size_t> get_count(Decoder& dec) {
    auto count = dec.get_varint();
    if (!count) {
        return std::unexpected(count.error());
    }
    if (*count > dec.remaining()) {
        return make_error(ErrorCode::InvalidArgument, "contador de elementos excede el buffer");
    }
    return static_cast<std::size_t>(*count);
}

void encode_entry(Encoder& enc, const RaftLogEntry& entry) {
    enc.put_i64(entry.term);
    enc.put_i64(entry.index);
    enc.put_u8(static_cast<std::uint8_t>(entry.type));
    enc.put_bytes(ByteSpan{entry.payload});
}

[[nodiscard]] expected<RaftLogEntry> decode_entry(Decoder& dec) {
    auto term = dec.get_i64();
    if (!term) {
        return std::unexpected(term.error());
    }
    auto index = dec.get_i64();
    if (!index) {
        return std::unexpected(index.error());
    }
    auto type = dec.get_u8();
    if (!type) {
        return std::unexpected(type.error());
    }
    if (*type > static_cast<std::uint8_t>(RaftEntryType::Config)) {
        return make_error(ErrorCode::InvalidArgument, "tipo de entrada de Raft inválido");
    }
    auto payload = dec.get_bytes();
    if (!payload) {
        return std::unexpected(payload.error());
    }
    RaftLogEntry entry;
    entry.term = *term;
    entry.index = *index;
    entry.type = static_cast<RaftEntryType>(*type);
    entry.payload.assign(payload->begin(), payload->end());
    return entry;
}

void encode_snapshot(Encoder& enc, const Snapshot& snapshot) {
    enc.put_i64(snapshot.last_included_index);
    enc.put_i64(snapshot.last_included_term);
    enc.put_bytes(ByteSpan{snapshot.state});
}

[[nodiscard]] expected<Snapshot> decode_snapshot(Decoder& dec) {
    auto last_included_index = dec.get_i64();
    if (!last_included_index) {
        return std::unexpected(last_included_index.error());
    }
    auto last_included_term = dec.get_i64();
    if (!last_included_term) {
        return std::unexpected(last_included_term.error());
    }
    auto state = dec.get_bytes();
    if (!state) {
        return std::unexpected(state.error());
    }
    Snapshot snapshot;
    snapshot.last_included_index = *last_included_index;
    snapshot.last_included_term = *last_included_term;
    snapshot.state.assign(state->begin(), state->end());
    return snapshot;
}

}  // namespace

void RequestVoteArgs::encode(Encoder& enc) const {
    enc.put_i64(term);
    enc.put_i32(candidate_id);
    enc.put_i64(last_log_index);
    enc.put_i64(last_log_term);
    enc.put_u8(static_cast<std::uint8_t>(pre_vote ? 1 : 0));
}

expected<RequestVoteArgs> RequestVoteArgs::decode(Decoder& dec) {
    auto term = dec.get_i64();
    if (!term) {
        return std::unexpected(term.error());
    }
    auto candidate_id = dec.get_i32();
    if (!candidate_id) {
        return std::unexpected(candidate_id.error());
    }
    auto last_log_index = dec.get_i64();
    if (!last_log_index) {
        return std::unexpected(last_log_index.error());
    }
    auto last_log_term = dec.get_i64();
    if (!last_log_term) {
        return std::unexpected(last_log_term.error());
    }
    auto pre_vote = dec.get_u8();
    if (!pre_vote) {
        return std::unexpected(pre_vote.error());
    }
    return RequestVoteArgs{.term = *term,
                           .candidate_id = *candidate_id,
                           .last_log_index = *last_log_index,
                           .last_log_term = *last_log_term,
                           .pre_vote = *pre_vote != 0};
}

void RequestVoteReply::encode(Encoder& enc) const {
    enc.put_i64(term);
    enc.put_u8(static_cast<std::uint8_t>(vote_granted ? 1 : 0));
}

expected<RequestVoteReply> RequestVoteReply::decode(Decoder& dec) {
    auto term = dec.get_i64();
    if (!term) {
        return std::unexpected(term.error());
    }
    auto vote_granted = dec.get_u8();
    if (!vote_granted) {
        return std::unexpected(vote_granted.error());
    }
    return RequestVoteReply{.term = *term, .vote_granted = *vote_granted != 0};
}

void AppendEntriesArgs::encode(Encoder& enc) const {
    enc.put_i64(term);
    enc.put_i32(leader_id);
    enc.put_i64(prev_log_index);
    enc.put_i64(prev_log_term);
    enc.put_varint(entries.size());
    for (const RaftLogEntry& entry : entries) {
        encode_entry(enc, entry);
    }
    enc.put_i64(leader_commit);
    enc.put_i32(leader_epoch);
}

expected<AppendEntriesArgs> AppendEntriesArgs::decode(Decoder& dec) {
    auto term = dec.get_i64();
    if (!term) {
        return std::unexpected(term.error());
    }
    auto leader_id = dec.get_i32();
    if (!leader_id) {
        return std::unexpected(leader_id.error());
    }
    auto prev_log_index = dec.get_i64();
    if (!prev_log_index) {
        return std::unexpected(prev_log_index.error());
    }
    auto prev_log_term = dec.get_i64();
    if (!prev_log_term) {
        return std::unexpected(prev_log_term.error());
    }
    auto count = get_count(dec);
    if (!count) {
        return std::unexpected(count.error());
    }
    AppendEntriesArgs args;
    args.term = *term;
    args.leader_id = *leader_id;
    args.prev_log_index = *prev_log_index;
    args.prev_log_term = *prev_log_term;
    args.entries.reserve(*count);
    for (std::size_t i = 0; i < *count; ++i) {
        auto entry = decode_entry(dec);
        if (!entry) {
            return std::unexpected(entry.error());
        }
        args.entries.push_back(std::move(*entry));
    }
    auto leader_commit = dec.get_i64();
    if (!leader_commit) {
        return std::unexpected(leader_commit.error());
    }
    auto leader_epoch = dec.get_i32();
    if (!leader_epoch) {
        return std::unexpected(leader_epoch.error());
    }
    args.leader_commit = *leader_commit;
    args.leader_epoch = *leader_epoch;
    return args;
}

void AppendEntriesReply::encode(Encoder& enc) const {
    enc.put_i64(term);
    enc.put_u8(static_cast<std::uint8_t>(success ? 1 : 0));
    enc.put_i64(conflict_index);
}

expected<AppendEntriesReply> AppendEntriesReply::decode(Decoder& dec) {
    auto term = dec.get_i64();
    if (!term) {
        return std::unexpected(term.error());
    }
    auto success = dec.get_u8();
    if (!success) {
        return std::unexpected(success.error());
    }
    auto conflict_index = dec.get_i64();
    if (!conflict_index) {
        return std::unexpected(conflict_index.error());
    }
    return AppendEntriesReply{
        .term = *term, .success = *success != 0, .conflict_index = *conflict_index};
}

void InstallSnapshotArgs::encode(Encoder& enc) const {
    enc.put_i64(term);
    enc.put_i32(leader_id);
    encode_snapshot(enc, snapshot);
}

expected<InstallSnapshotArgs> InstallSnapshotArgs::decode(Decoder& dec) {
    auto term = dec.get_i64();
    if (!term) {
        return std::unexpected(term.error());
    }
    auto leader_id = dec.get_i32();
    if (!leader_id) {
        return std::unexpected(leader_id.error());
    }
    auto snapshot = decode_snapshot(dec);
    if (!snapshot) {
        return std::unexpected(snapshot.error());
    }
    InstallSnapshotArgs args;
    args.term = *term;
    args.leader_id = *leader_id;
    args.snapshot = std::move(*snapshot);
    return args;
}

void InstallSnapshotReply::encode(Encoder& enc) const {
    enc.put_i64(term);
}

expected<InstallSnapshotReply> InstallSnapshotReply::decode(Decoder& dec) {
    auto term = dec.get_i64();
    if (!term) {
        return std::unexpected(term.error());
    }
    return InstallSnapshotReply{.term = *term};
}

}  // namespace nexus
