// Round-trip de los RPC de Raft sobre el codec del protocolo: decode(encode(x)) == x. Incluye un
// caso de entrada truncada (decodificador defensivo) y de tipo de entrada inválido.
#include "consensus/raft_rpc.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <vector>

#include "common/bytes.hpp"
#include "consensus/raft_state.hpp"
#include "protocol/codec.hpp"

namespace {

// Codifica `msg` y lo vuelve a decodificar con T::decode; devuelve el resultado.
template <class T>
nexus::expected<T> round_trip(const T& msg) {
    nexus::Buffer buffer;
    nexus::Encoder enc{buffer};
    msg.encode(enc);
    nexus::Decoder dec{buffer.as_span()};
    return T::decode(dec);
}

TEST(RequestVoteArgs, RoundTrip_PreservaCampos) {
    const nexus::RequestVoteArgs original{
        .term = 7, .candidate_id = 3, .last_log_index = 42, .last_log_term = 5, .pre_vote = true};
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(RequestVoteReply, RoundTrip_PreservaCampos) {
    const nexus::RequestVoteReply original{.term = 9, .vote_granted = true};
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(AppendEntriesArgs, RoundTrip_HeartbeatSinEntradas) {
    const nexus::AppendEntriesArgs original{.term = 4,
                                            .leader_id = 1,
                                            .prev_log_index = 10,
                                            .prev_log_term = 4,
                                            .entries = {},
                                            .leader_commit = 8,
                                            .leader_epoch = 2};
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(AppendEntriesArgs, RoundTrip_ConEntradas) {
    nexus::AppendEntriesArgs original;
    original.term = 6;
    original.leader_id = 2;
    original.prev_log_index = 3;
    original.prev_log_term = 5;
    original.leader_commit = 4;
    original.leader_epoch = 1;
    original.entries.push_back(nexus::RaftLogEntry{.term = 6,
                                                   .index = 4,
                                                   .type = nexus::RaftEntryType::Data,
                                                   .payload = {std::byte{0xDE}, std::byte{0xAD}}});
    original.entries.push_back(nexus::RaftLogEntry{
        .term = 6, .index = 5, .type = nexus::RaftEntryType::Config, .payload = {std::byte{0x01}}});
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
    ASSERT_EQ(decoded->entries.size(), 2U);
    EXPECT_EQ(decoded->entries[0].index, 4);
    EXPECT_EQ(decoded->entries[1].type, nexus::RaftEntryType::Config);
}

TEST(AppendEntriesReply, RoundTrip_PreservaConflictIndex) {
    const nexus::AppendEntriesReply original{.term = 3, .success = false, .conflict_index = 11};
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(InstallSnapshotArgs, RoundTrip_PreservaSnapshot) {
    nexus::InstallSnapshotArgs original;
    original.term = 8;
    original.leader_id = 1;
    original.snapshot = nexus::Snapshot{.last_included_index = 100,
                                        .last_included_term = 7,
                                        .state = {std::byte{0xAB}, std::byte{0xCD}}};
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(InstallSnapshotReply, RoundTrip_PreservaTermino) {
    const nexus::InstallSnapshotReply original{.term = 12};
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(AppendEntriesArgs, Decode_EntradaTruncada_DevuelveError) {
    nexus::Buffer buffer;
    nexus::Encoder enc{buffer};
    nexus::AppendEntriesArgs original{.term = 1,
                                      .leader_id = 1,
                                      .prev_log_index = 0,
                                      .prev_log_term = 0,
                                      .entries = {},
                                      .leader_commit = 0,
                                      .leader_epoch = 0};
    original.encode(enc);
    // Recorta el último byte: el decodificador defensivo debe rechazar, no leer fuera de rango.
    nexus::ByteSpan full = buffer.as_span();
    nexus::Decoder dec{full.subspan(0, full.size() - 1)};
    EXPECT_FALSE(nexus::AppendEntriesArgs::decode(dec).has_value());
}

TEST(AppendEntriesArgs, Decode_TipoDeEntradaInvalido_DevuelveError) {
    nexus::Buffer buffer;
    nexus::Encoder enc{buffer};
    // Cabecera + 1 entrada con un `type` fuera de rango (99).
    enc.put_i64(1);                    // term
    enc.put_i32(1);                    // leader_id
    enc.put_i64(0);                    // prev_log_index
    enc.put_i64(0);                    // prev_log_term
    enc.put_varint(1);                 // count
    enc.put_i64(1);                    // entry.term
    enc.put_i64(1);                    // entry.index
    enc.put_u8(99);                    // entry.type inválido
    enc.put_bytes(nexus::ByteSpan{});  // entry.payload vacío
    enc.put_i64(0);                    // leader_commit
    enc.put_i32(0);                    // leader_epoch
    nexus::Decoder dec{buffer.as_span()};
    EXPECT_FALSE(nexus::AppendEntriesArgs::decode(dec).has_value());
}

}  // namespace
