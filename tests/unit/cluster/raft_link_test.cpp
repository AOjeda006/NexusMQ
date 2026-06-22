// Pruebas de RaftEnvelopeReader/RaftEnvelopeWriter: enlace inter-nodo longitud-prefijo (ADR-0025).
// Deterministas con FakeProactor: el writer codifica `length:u32 | sobre`; el reader lo reensambla
// y decodifica. Plano inter-nodo separado del de cliente (no usa el FrameHeader/ApiKey).
#include "cluster/raft_link.hpp"

#include <gtest/gtest.h>
#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <optional>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "consensus/raft_rpc.hpp"
#include "consensus/raft_wire.hpp"
#include "io/socket.hpp"
#include "protocol/codec.hpp"
#include "support/fake_proactor.hpp"

namespace {

nexus::RaftEnvelope sample_envelope() {
    return nexus::RaftEnvelope{
        .topic = "orders",
        .partition = 3,
        .message = nexus::RaftMessage{.from = 1,
                                      .to = 2,
                                      .payload = nexus::RequestVoteArgs{.term = 7,
                                                                        .candidate_id = 1,
                                                                        .last_log_index = 42,
                                                                        .last_log_term = 5,
                                                                        .pre_vote = true}}};
}

// Arranca @p work y guarda su resultado en @p out (corrutina conductora para los tests).
template <class T>
nexus::task<void> collect(nexus::task<T> work, std::optional<T>& out) {
    out = co_await std::move(work);
}

// Entrega `wire` a las recv sucesivas del lector hasta que el conductor termine.
void feed_until_done(nexus::FakeProactor& fake, const nexus::task<void>& driver,
                     nexus::ByteSpan wire) {
    std::size_t off = 0;
    while (!driver.done() && fake.pending() > 0) {
        off += fake.deliver_recv_front(wire.subspan(off));
        fake.run_completions(1);
    }
}

int make_fd() {
    const int raw = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(raw, 0);
    return raw;
}

TEST(RaftEnvelopeWriter, Write_EmiteLengthPrefijoYSobreCodificado) {
    nexus::FakeProactor fake;
    nexus::Socket sock{make_fd()};
    nexus::RaftEnvelopeWriter writer{sock};

    const nexus::RaftEnvelope env = sample_envelope();
    std::optional<nexus::expected<void>> result;
    auto driver = collect(writer.write(fake, env), result);
    driver.handle().resume();

    // 1ª send: el prefijo de longitud (u32) = tamaño del sobre codificado.
    ASSERT_EQ(fake.pending(), 1U);
    EXPECT_EQ(fake.peek(0).kind, nexus::FakeProactor::OpKind::Send);
    const nexus::ByteSpan len_sent = fake.peek(0).write_buffer;
    ASSERT_EQ(len_sent.size(), sizeof(std::uint32_t));
    nexus::Decoder len_dec{len_sent};
    const std::uint32_t payload_len = len_dec.get_u32().value();
    fake.arm_front(static_cast<std::int32_t>(len_sent.size()));
    fake.run_completions(1);

    // 2ª send: el sobre, que decodifica de vuelta al original.
    ASSERT_EQ(fake.pending(), 1U);
    const nexus::ByteSpan body = fake.peek(0).write_buffer;
    EXPECT_EQ(body.size(), payload_len);
    nexus::Decoder dec{body};
    const nexus::expected<nexus::RaftEnvelope> decoded = nexus::RaftEnvelope::decode(dec);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, env);
    fake.arm_front(static_cast<std::int32_t>(body.size()));
    fake.run_completions(1);

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->has_value());
}

TEST(RaftEnvelopeReader, Read_SobreCompleto_DecodificaElOriginal) {
    // Construye el wire `length:u32 | sobre` con el writer (round-trip writer→reader).
    const nexus::RaftEnvelope env = sample_envelope();
    nexus::Buffer payload;
    nexus::Encoder enc{payload};
    env.encode(enc);
    nexus::Buffer wire;
    nexus::Encoder wire_enc{wire};
    wire_enc.put_u32(static_cast<std::uint32_t>(payload.as_span().size()));
    wire.append(payload.as_span());

    nexus::FakeProactor fake;
    nexus::Socket sock{make_fd()};
    nexus::RaftEnvelopeReader reader{sock};

    std::optional<nexus::expected<nexus::RaftEnvelope>> result;
    auto driver = collect(reader.read(fake, 64U * 1024U), result);
    driver.handle().resume();
    feed_until_done(fake, driver, wire.as_span());

    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ(**result, env);
}

TEST(RaftEnvelopeReader, Read_ExcedeMaxMessage_DevuelveInvalidArgument) {
    nexus::Buffer wire;
    nexus::Encoder enc{wire};
    enc.put_u32(4096);  // declara un sobre enorme

    nexus::FakeProactor fake;
    nexus::Socket sock{make_fd()};
    nexus::RaftEnvelopeReader reader{sock};

    std::optional<nexus::expected<nexus::RaftEnvelope>> result;
    auto driver = collect(reader.read(fake, 64), result);  // max_message = 64 bytes
    driver.handle().resume();
    feed_until_done(fake, driver, wire.as_span());

    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(RaftEnvelopeReader, Read_LengthCero_DevuelveInvalidArgument) {
    nexus::Buffer wire;
    nexus::Encoder enc{wire};
    enc.put_u32(0);  // un sobre no puede tener longitud cero

    nexus::FakeProactor fake;
    nexus::Socket sock{make_fd()};
    nexus::RaftEnvelopeReader reader{sock};

    std::optional<nexus::expected<nexus::RaftEnvelope>> result;
    auto driver = collect(reader.read(fake, 64U * 1024U), result);
    driver.handle().resume();
    feed_until_done(fake, driver, wire.as_span());

    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(RaftEnvelopeReader, Read_CierreAMediaTrama_DevuelveIoError) {
    // Solo entregamos los 4 bytes de length; el sobre nunca llega → EOF a media trama.
    nexus::Buffer wire;
    nexus::Encoder enc{wire};
    enc.put_u32(32);

    nexus::FakeProactor fake;
    nexus::Socket sock{make_fd()};
    nexus::RaftEnvelopeReader reader{sock};

    std::optional<nexus::expected<nexus::RaftEnvelope>> result;
    auto driver = collect(reader.read(fake, 64U * 1024U), result);
    driver.handle().resume();
    feed_until_done(fake, driver, wire.as_span());

    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().code(), nexus::ErrorCode::IoError);
}

}  // namespace
