// Round-trip del sobre inter-nodo de Raft (ADR-0025): decode(encode(x)) == x para cada tipo de
// RPC, preservando topic/partition/from/to. Incluye casos del decodificador defensivo (entrada
// truncada y tipo de RPC desconocido).
#include "consensus/raft_wire.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <variant>

#include "common/bytes.hpp"
#include "consensus/raft_rpc.hpp"
#include "consensus/raft_state.hpp"
#include "protocol/codec.hpp"

namespace {

// Codifica `env` y lo vuelve a decodificar; devuelve el resultado.
nexus::expected<nexus::RaftEnvelope> round_trip(const nexus::RaftEnvelope& env) {
    nexus::Buffer buffer;
    nexus::Encoder enc{buffer};
    env.encode(enc);
    nexus::Decoder dec{buffer.as_span()};
    return nexus::RaftEnvelope::decode(dec);
}

// Construye un sobre con `payload` dirigido a (topic="orders", partition=3, from=1, to=2).
template <class Payload>
nexus::RaftEnvelope envelope_with(Payload payload) {
    return nexus::RaftEnvelope{
        .topic = "orders",
        .partition = 3,
        .message = nexus::RaftMessage{.from = 1, .to = 2, .payload = std::move(payload)}};
}

TEST(RaftEnvelope, RoundTrip_RequestVoteArgs_PreservaEnrutadoYPayload) {
    const auto original = envelope_with(nexus::RequestVoteArgs{
        .term = 7, .candidate_id = 1, .last_log_index = 42, .last_log_term = 5, .pre_vote = true});
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
    EXPECT_EQ(decoded->topic, "orders");
    EXPECT_EQ(decoded->partition, 3);
    EXPECT_EQ(decoded->message.from, 1);
    EXPECT_EQ(decoded->message.to, 2);
    EXPECT_TRUE(std::holds_alternative<nexus::RequestVoteArgs>(decoded->message.payload));
}

TEST(RaftEnvelope, RoundTrip_RequestVoteReply) {
    const auto original = envelope_with(nexus::RequestVoteReply{.term = 9, .vote_granted = true});
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(RaftEnvelope, RoundTrip_AppendEntriesArgs_ConEntradas) {
    nexus::AppendEntriesArgs args;
    args.term = 6;
    args.leader_id = 1;
    args.prev_log_index = 3;
    args.prev_log_term = 5;
    args.leader_commit = 4;
    args.leader_epoch = 1;
    args.entries.push_back(nexus::RaftLogEntry{.term = 6,
                                               .index = 4,
                                               .type = nexus::RaftEntryType::Data,
                                               .payload = {std::byte{0xDE}, std::byte{0xAD}}});
    const auto original = envelope_with(std::move(args));
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
    const auto& got = std::get<nexus::AppendEntriesArgs>(decoded->message.payload);
    ASSERT_EQ(got.entries.size(), 1U);
    EXPECT_EQ(got.entries[0].index, 4);
}

TEST(RaftEnvelope, RoundTrip_AppendEntriesReply) {
    const auto original =
        envelope_with(nexus::AppendEntriesReply{.term = 3, .success = false, .conflict_index = 11});
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(RaftEnvelope, RoundTrip_TimeoutNowArgs) {
    const auto original = envelope_with(nexus::TimeoutNowArgs{.term = 5, .leader_id = 2});
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(RaftEnvelope, RoundTrip_InstallSnapshotArgs_PreservaBase) {
    nexus::InstallSnapshotArgs args;
    args.term = 8;
    args.leader_id = 1;
    args.snapshot = nexus::Snapshot{.last_included_index = 100,
                                    .last_included_term = 7,
                                    .last_included_offset = 99,
                                    .state = {std::byte{0xAB}}};
    const auto original = envelope_with(std::move(args));
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(RaftEnvelope, RoundTrip_InstallSnapshotReply) {
    const auto original = envelope_with(nexus::InstallSnapshotReply{.term = 12});
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(RaftEnvelope, RoundTrip_TopicVacio_EsValido) {
    const auto original = nexus::RaftEnvelope{
        .topic = "",
        .partition = 0,
        .message = nexus::RaftMessage{
            .from = 4, .to = 5, .payload = nexus::RequestVoteReply{.term = 1, .vote_granted = false}}};
    const auto decoded = round_trip(original);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(RaftEnvelope, Decode_EntradaTruncada_DevuelveError) {
    nexus::Buffer buffer;
    nexus::Encoder enc{buffer};
    envelope_with(nexus::RequestVoteArgs{
        .term = 1, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0, .pre_vote = false})
        .encode(enc);
    // Recorta el último byte: el decodificador defensivo debe rechazar, no leer fuera de rango.
    const nexus::ByteSpan full = buffer.as_span();
    nexus::Decoder dec{full.subspan(0, full.size() - 1)};
    EXPECT_FALSE(nexus::RaftEnvelope::decode(dec).has_value());
}

TEST(RaftEnvelope, Decode_TipoDeRpcDesconocido_DevuelveError) {
    nexus::Buffer buffer;
    nexus::Encoder enc{buffer};
    enc.put_string("orders");  // topic
    enc.put_i32(3);            // partition
    enc.put_i32(1);            // from
    enc.put_i32(2);            // to
    enc.put_u8(99);            // type fuera del rango [0, 6]
    nexus::Decoder dec{buffer.as_span()};
    EXPECT_FALSE(nexus::RaftEnvelope::decode(dec).has_value());
}

}  // namespace
