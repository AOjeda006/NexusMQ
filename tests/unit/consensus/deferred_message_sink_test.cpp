// Pruebas de DeferredMessageSink: reenvía a un objetivo fijable; sin objetivo descarta (ADR-0025).
#include "consensus/deferred_message_sink.hpp"

#include <gtest/gtest.h>

#include <vector>

#include "consensus/raft_carrier.hpp"  // RaftMessageSink
#include "consensus/raft_rpc.hpp"
#include "consensus/raft_wire.hpp"

namespace {

// Sumidero de prueba que registra los sobres recibidos.
class RecordingSink final : public nexus::RaftMessageSink {
public:
    void send(const nexus::RaftEnvelope& envelope) override { received.push_back(envelope); }
    std::vector<nexus::RaftEnvelope> received;
};

nexus::RaftEnvelope sample() {
    return nexus::RaftEnvelope{.topic = "t",
                               .partition = 0,
                               .message = nexus::RaftMessage{.from = 1,
                                                             .to = 2,
                                                             .payload = nexus::RequestVoteReply{
                                                                 .term = 1, .vote_granted = true}}};
}

TEST(DeferredMessageSink, SinObjetivo_DescartaElSobre) {
    nexus::DeferredMessageSink sink;
    EXPECT_FALSE(sink.has_target());
    sink.send(sample());  // no debe hacer nada ni romper
}

TEST(DeferredMessageSink, ConObjetivo_ReenviaElSobre) {
    nexus::DeferredMessageSink sink;
    RecordingSink target;
    sink.set_target(&target);
    EXPECT_TRUE(sink.has_target());

    const nexus::RaftEnvelope env = sample();
    sink.send(env);

    ASSERT_EQ(target.received.size(), 1U);
    EXPECT_EQ(target.received[0], env);
}

TEST(DeferredMessageSink, ObjetivoNulo_VuelveADescartar) {
    nexus::DeferredMessageSink sink;
    RecordingSink target;
    sink.set_target(&target);
    sink.set_target(nullptr);

    sink.send(sample());
    EXPECT_TRUE(target.received.empty());
}

}  // namespace
