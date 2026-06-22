// Pruebas de RaftTransport: sumidero saliente de Raft sobre TCP a peers (ADR-0025). Deterministas
// con FakeProactor + un spawner que captura las corrutinas emisoras para conducirlas a mano.
#include "cluster/raft_transport.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "cluster/peer_directory.hpp"
#include "common/bytes.hpp"
#include "common/task.hpp"
#include "consensus/raft_rpc.hpp"
#include "consensus/raft_wire.hpp"
#include "protocol/codec.hpp"
#include "support/fake_proactor.hpp"

namespace {

constexpr nexus::NodeId kSelf = 1;
constexpr nexus::NodeId kPeer = 2;

nexus::PeerDirectory single_peer_directory() {
    std::unordered_map<nexus::NodeId, nexus::PeerAddress> peers;
    peers.emplace(kPeer, nexus::PeerAddress{.host = "127.0.0.1", .port = 9000});
    return nexus::PeerDirectory{std::move(peers)};
}

nexus::RaftEnvelope vote_envelope(nexus::NodeId to, nexus::Term term) {
    return nexus::RaftEnvelope{
        .topic = "orders",
        .partition = 0,
        .message = nexus::RaftMessage{.from = kSelf,
                                      .to = to,
                                      .payload = nexus::RequestVoteArgs{.term = term,
                                                                        .candidate_id = kSelf,
                                                                        .last_log_index = 0,
                                                                        .last_log_term = 0,
                                                                        .pre_vote = false}}};
}

// Resultado de conducir una corrutina emisora: los bytes enviados y si llegó a conectar.
struct PumpResult {
    std::vector<std::byte> wire;
    bool connected = false;  // ¿se procesó alguna op Connect?
};

// Conduce la corrutina emisora @p sender con @p fake: la arranca, arma cada Connect con éxito y
// cada Send con su tamaño completo (acumulando los bytes enviados), hasta que la corrutina termine.
PumpResult pump(nexus::FakeProactor& fake, const nexus::task<void>& sender) {
    PumpResult result;
    sender.handle().resume();  // arranca la corrutina (lazy): primer co_await la suspende.
    int guard = 0;
    while (!sender.done() && fake.pending() > 0 && guard++ < 1000) {
        const nexus::FakeProactor::Op& op = fake.peek(0);
        if (op.kind == nexus::FakeProactor::OpKind::Send) {
            const nexus::ByteSpan bytes = op.write_buffer;
            result.wire.insert(result.wire.end(), bytes.begin(), bytes.end());
            fake.arm_front(static_cast<std::int32_t>(bytes.size()));
        } else {
            result.connected = true;  // Connect: éxito.
            fake.arm_front(0);
        }
        fake.run_completions(1);
    }
    return result;
}

// Decodifica todos los sobres del wire `length:u32 | sobre` concatenado.
std::vector<nexus::RaftEnvelope> decode_all(const std::vector<std::byte>& wire) {
    std::vector<nexus::RaftEnvelope> envelopes;
    nexus::Decoder dec{nexus::ByteSpan{wire.data(), wire.size()}};
    while (dec.remaining() > 0) {
        const nexus::expected<std::uint32_t> length = dec.get_u32();
        EXPECT_TRUE(length.has_value());
        const nexus::expected<nexus::RaftEnvelope> env = nexus::RaftEnvelope::decode(dec);
        EXPECT_TRUE(env.has_value());
        if (!env) {
            break;
        }
        envelopes.push_back(*env);
    }
    return envelopes;
}

TEST(RaftTransport, Send_DestinoDesconocido_NoEmite) {
    const nexus::PeerDirectory directory = single_peer_directory();
    nexus::FakeProactor fake;
    std::vector<nexus::task<void>> spawned;
    nexus::RaftTransport transport{kSelf, directory, fake, [&spawned](nexus::task<void> task) {
                                       spawned.push_back(std::move(task));
                                   }};

    transport.send(vote_envelope(99, 1));  // 99 no está en el directorio

    EXPECT_TRUE(spawned.empty());
    EXPECT_EQ(fake.pending(), 0U);
}

TEST(RaftTransport, Send_ASiMismo_NoEmite) {
    const nexus::PeerDirectory directory = single_peer_directory();
    nexus::FakeProactor fake;
    std::vector<nexus::task<void>> spawned;
    nexus::RaftTransport transport{kSelf, directory, fake, [&spawned](nexus::task<void> task) {
                                       spawned.push_back(std::move(task));
                                   }};

    transport.send(vote_envelope(kSelf, 1));

    EXPECT_TRUE(spawned.empty());
}

TEST(RaftTransport, Send_APeer_ConectaYTransmiteElSobre) {
    const nexus::PeerDirectory directory = single_peer_directory();
    nexus::FakeProactor fake;
    std::vector<nexus::task<void>> spawned;
    nexus::RaftTransport transport{kSelf, directory, fake, [&spawned](nexus::task<void> task) {
                                       spawned.push_back(std::move(task));
                                   }};

    const nexus::RaftEnvelope original = vote_envelope(kPeer, 7);
    transport.send(original);

    ASSERT_EQ(spawned.size(), 1U);
    const PumpResult result = pump(fake, spawned[0]);
    ASSERT_TRUE(spawned[0].done());
    EXPECT_TRUE(result.connected);  // primera vez: conecta

    const std::vector<nexus::RaftEnvelope> decoded = decode_all(result.wire);
    ASSERT_EQ(decoded.size(), 1U);
    EXPECT_EQ(decoded[0], original);
}

TEST(RaftTransport, Send_VariosSobres_LosTransmiteEnOrden) {
    const nexus::PeerDirectory directory = single_peer_directory();
    nexus::FakeProactor fake;
    std::vector<nexus::task<void>> spawned;
    nexus::RaftTransport transport{kSelf, directory, fake, [&spawned](nexus::task<void> task) {
                                       spawned.push_back(std::move(task));
                                   }};

    // Dos sobres encolados antes de drenar: el primero arranca la emisora; el segundo se encola.
    const nexus::RaftEnvelope first = vote_envelope(kPeer, 1);
    const nexus::RaftEnvelope second = vote_envelope(kPeer, 2);
    transport.send(first);
    transport.send(second);

    ASSERT_EQ(spawned.size(), 1U);  // un solo emisor por peer
    const PumpResult result = pump(fake, spawned[0]);
    ASSERT_TRUE(spawned[0].done());

    const std::vector<nexus::RaftEnvelope> decoded = decode_all(result.wire);
    ASSERT_EQ(decoded.size(), 2U);
    EXPECT_EQ(decoded[0], first);
    EXPECT_EQ(decoded[1], second);
}

TEST(RaftTransport, Send_FalloDeConexion_DescartaYPermiteReintento) {
    const nexus::PeerDirectory directory = single_peer_directory();
    nexus::FakeProactor fake;
    std::vector<nexus::task<void>> spawned;
    nexus::RaftTransport transport{kSelf, directory, fake, [&spawned](nexus::task<void> task) {
                                       spawned.push_back(std::move(task));
                                   }};

    transport.send(vote_envelope(kPeer, 1));
    ASSERT_EQ(spawned.size(), 1U);

    // Falla la conexión: la emisora descarta la cola y termina (sin enviar nada).
    spawned[0].handle().resume();
    ASSERT_EQ(fake.pending(), 1U);
    EXPECT_EQ(fake.peek(0).kind, nexus::FakeProactor::OpKind::Connect);
    fake.arm_front(-111);  // ECONNREFUSED
    fake.run_completions(1);
    ASSERT_TRUE(spawned[0].done());

    // Tras el fallo, un nuevo send relanza un emisor (el flag de envío se ha reseteado).
    transport.send(vote_envelope(kPeer, 2));
    EXPECT_EQ(spawned.size(), 2U);
}

TEST(RaftTransport, Send_TrasVaciar_ReusaLaConexion) {
    const nexus::PeerDirectory directory = single_peer_directory();
    nexus::FakeProactor fake;
    std::vector<nexus::task<void>> spawned;
    nexus::RaftTransport transport{kSelf, directory, fake, [&spawned](nexus::task<void> task) {
                                       spawned.push_back(std::move(task));
                                   }};

    transport.send(vote_envelope(kPeer, 1));
    ASSERT_EQ(spawned.size(), 1U);
    const PumpResult first = pump(fake, spawned[0]);  // conecta y transmite el primero
    ASSERT_TRUE(spawned[0].done());
    EXPECT_TRUE(first.connected);

    // Segundo envío: relanza emisor, pero la conexión ya está abierta → NO vuelve a conectar.
    transport.send(vote_envelope(kPeer, 2));
    ASSERT_EQ(spawned.size(), 2U);
    const PumpResult second = pump(fake, spawned[1]);
    ASSERT_TRUE(spawned[1].done());
    EXPECT_FALSE(second.connected);  // reusa la conexión: send directo, sin Connect

    const std::vector<nexus::RaftEnvelope> decoded = decode_all(second.wire);
    ASSERT_EQ(decoded.size(), 1U);
    EXPECT_EQ(decoded[0].message.to, kPeer);
}

}  // namespace
