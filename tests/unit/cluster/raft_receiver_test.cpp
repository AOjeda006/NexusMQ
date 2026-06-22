// Pruebas de serve_raft_connection: bucle de recepción de sobres de Raft (ADR-0025). Deterministas
// con FakeProactor: alimenta el wire `length:u32 | sobre` y verifica los sobres entregados.
#include "cluster/raft_receiver.hpp"

#include <gtest/gtest.h>
#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/bytes.hpp"
#include "common/task.hpp"
#include "consensus/raft_rpc.hpp"
#include "consensus/raft_wire.hpp"
#include "protocol/codec.hpp"
#include "support/fake_proactor.hpp"

namespace {

nexus::RaftEnvelope vote_envelope(nexus::PartitionId partition, nexus::Term term) {
    return nexus::RaftEnvelope{
        .topic = "orders",
        .partition = partition,
        .message = nexus::RaftMessage{.from = 2,
                                      .to = 1,
                                      .payload = nexus::RequestVoteArgs{.term = term,
                                                                        .candidate_id = 2,
                                                                        .last_log_index = 0,
                                                                        .last_log_term = 0,
                                                                        .pre_vote = false}}};
}

// Serializa @p envelope al wire inter-nodo (length:u32 | sobre) y lo añade a @p out.
void append_wire(const nexus::RaftEnvelope& envelope, nexus::Buffer& out) {
    nexus::Buffer body;
    nexus::Encoder body_enc{body};
    envelope.encode(body_enc);
    nexus::Encoder out_enc{out};
    out_enc.put_u32(static_cast<std::uint32_t>(body.as_span().size()));
    out.append(body.as_span());
}

int make_fd() {
    const int raw = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(raw, 0);
    return raw;
}

// Entrega `wire` a las recv del receptor y luego EOF, hasta que la corrutina termine.
void feed(nexus::FakeProactor& fake, const nexus::task<void>& driver, nexus::ByteSpan wire) {
    driver.handle().resume();  // arranca: primer recv pendiente
    std::size_t off = 0;
    int guard = 0;
    while (!driver.done() && fake.pending() > 0 && guard++ < 1000) {
        off += fake.deliver_recv_front(wire.subspan(off));  // 0 bytes al agotarse => EOF
        fake.run_completions(1);
    }
}

TEST(ServeRaftConnection, EntregaCadaSobreYTerminaEnEof) {
    const nexus::RaftEnvelope first = vote_envelope(0, 1);
    const nexus::RaftEnvelope second = vote_envelope(3, 2);
    nexus::Buffer wire;
    append_wire(first, wire);
    append_wire(second, wire);

    nexus::FakeProactor fake;
    nexus::Socket sock{make_fd()};
    std::vector<nexus::RaftEnvelope> received;
    auto driver = nexus::serve_raft_connection(
        fake, std::move(sock),
        [&received](const nexus::RaftEnvelope& env) { received.push_back(env); }, 64U * 1024U);

    feed(fake, driver, wire.as_span());

    ASSERT_TRUE(driver.done());
    ASSERT_EQ(received.size(), 2U);
    EXPECT_EQ(received[0], first);
    EXPECT_EQ(received[1], second);
}

TEST(ServeRaftConnection, EofInmediato_NoEntregaNada) {
    nexus::FakeProactor fake;
    nexus::Socket sock{make_fd()};
    std::vector<nexus::RaftEnvelope> received;
    auto driver = nexus::serve_raft_connection(
        fake, std::move(sock),
        [&received](const nexus::RaftEnvelope& env) { received.push_back(env); }, 64U * 1024U);

    feed(fake, driver, nexus::ByteSpan{});  // sin bytes: la primera recv ve EOF

    ASSERT_TRUE(driver.done());
    EXPECT_TRUE(received.empty());
}

TEST(ServeRaftConnection, SobreInvalido_CierraSinEntregar) {
    // length declara más bytes de los que llegan: el reader ve EOF a media trama y cierra.
    nexus::Buffer wire;
    nexus::Encoder enc{wire};
    enc.put_u32(64);  // promete 64 bytes de sobre que nunca llegan completos

    nexus::FakeProactor fake;
    nexus::Socket sock{make_fd()};
    std::vector<nexus::RaftEnvelope> received;
    auto driver = nexus::serve_raft_connection(
        fake, std::move(sock),
        [&received](const nexus::RaftEnvelope& env) { received.push_back(env); }, 64U * 1024U);

    feed(fake, driver, wire.as_span());

    ASSERT_TRUE(driver.done());
    EXPECT_TRUE(received.empty());
}

}  // namespace
