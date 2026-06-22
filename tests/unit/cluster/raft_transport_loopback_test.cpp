// Integración del transporte inter-nodo sobre un socket REAL por loopback (io_uring): un
// RaftTransport conecta y envía un sobre; el otro extremo lo recibe y decodifica (ADR-0025). Valida
// el camino salida -> wire -> entrada de extremo a extremo (lo que el FakeProactor no cubre).
#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <unordered_map>
#include <vector>

#include "cluster/peer_directory.hpp"
#include "cluster/raft_link.hpp"
#include "cluster/raft_transport.hpp"
#include "common/task.hpp"
#include "consensus/raft_rpc.hpp"
#include "consensus/raft_wire.hpp"
#include "io/socket.hpp"

#ifdef NEXUS_HAVE_IOURING
#include "io/io_uring_backend.hpp"
#endif

namespace {

#ifdef NEXUS_HAVE_IOURING

nexus::RaftEnvelope vote_envelope() {
    return nexus::RaftEnvelope{
        .topic = "orders",
        .partition = 0,
        .message = nexus::RaftMessage{.from = 1,
                                      .to = 2,
                                      .payload = nexus::RequestVoteArgs{.term = 7,
                                                                        .candidate_id = 1,
                                                                        .last_log_index = 4,
                                                                        .last_log_term = 2,
                                                                        .pre_vote = false}}};
}

// Acepta una conexión y lee exactamente un sobre (luego termina, cerrando su socket): así la
// corrutina queda `done` sin ops pendientes al final del test (apagado limpio bajo io_uring/ASan).
nexus::task<nexus::expected<nexus::RaftEnvelope>> accept_and_read_one(nexus::Proactor& proactor,
                                                                      nexus::Listener& listener) {
    nexus::expected<nexus::Socket> client = co_await listener.async_accept(proactor);
    if (!client) {
        co_return std::unexpected(client.error());
    }
    nexus::RaftEnvelopeReader reader{*client};
    co_return co_await reader.read(proactor, 64U * 1024U);
}

TEST(RaftTransportLoopback, EnviaSobreYLoRecibePorSocketReal) {
    try {
        (void)nexus::IoUringBackend{8};
    } catch (const std::system_error&) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    auto listener = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener.has_value());
    const std::uint16_t port = listener->local_port();
    ASSERT_GT(port, 0);

    nexus::IoUringBackend rx_proactor{32};  // extremo receptor (node 2)
    nexus::IoUringBackend tx_proactor{32};  // extremo emisor (node 1)

    auto receiver = accept_and_read_one(rx_proactor, *listener);
    receiver.handle().resume();  // accept pendiente

    std::unordered_map<nexus::NodeId, nexus::PeerAddress> peer_map;
    peer_map.emplace(2, nexus::PeerAddress{.host = "127.0.0.1", .port = port});
    const nexus::PeerDirectory peers{std::move(peer_map)};

    std::vector<nexus::task<void>> spawned;
    nexus::RaftTransport transport{
        /*self=*/1, peers, tx_proactor,
        [&spawned](nexus::task<void> task) { spawned.push_back(std::move(task)); }};

    const nexus::RaftEnvelope original = vote_envelope();
    transport.send(original);
    ASSERT_EQ(spawned.size(), 1U);
    spawned[0].handle().resume();  // arranca la emisora: connect pendiente

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!receiver.done() && std::chrono::steady_clock::now() < deadline) {
        tx_proactor.run_completions(16);
        rx_proactor.run_completions(16);
    }
    ASSERT_TRUE(receiver.done());

    const nexus::expected<nexus::RaftEnvelope> received = receiver.handle().promise().result();
    ASSERT_TRUE(received.has_value());
    EXPECT_EQ(*received, original);

    // Drena la emisora para que termine sin ops pendientes (apagado limpio).
    while (!spawned[0].done() && std::chrono::steady_clock::now() < deadline) {
        tx_proactor.run_completions(16);
    }
    EXPECT_TRUE(spawned[0].done());
}

#endif  // NEXUS_HAVE_IOURING

}  // namespace
