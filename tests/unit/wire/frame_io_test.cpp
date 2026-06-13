// Pruebas de FrameReader/FrameWriter. Las unitarias usan FakeProactor (deterministas): entregan
// los bytes de la trama a las recv del lector y verifican las send del escritor. Si hay io_uring,
// un e2e por loopback valida el round-trip request→response real sobre el backend.
#include "wire/frame_io.hpp"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "io/socket.hpp"
#include "protocol/codec.hpp"
#include "protocol/frame.hpp"
#include "support/fake_proactor.hpp"

#ifdef NEXUS_HAVE_IOURING
#include "io/io_uring_backend.hpp"
#endif

namespace {

// Construye los bytes de wire de una trama (length recalculado): [length:u32][resto
// cabecera][payload].
std::vector<std::byte> encode_frame(nexus::FrameHeader header, nexus::ByteSpan payload) {
    header.length = nexus::FrameHeader::length_for(payload.size());
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    header.encode(enc);
    buf.append(payload);
    const nexus::ByteSpan span = buf.as_span();
    return {span.begin(), span.end()};
}

nexus::ByteSpan as_bytes(std::string_view text) {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

// Arranca @p work y guarda su resultado en @p out (corrutina conductora para los tests).
template <class T>
nexus::task<void> collect(nexus::task<T> work, std::optional<T>& out) {
    out = co_await std::move(work);
}

// Entrega `wire` a las recv sucesivas del lector hasta que el conductor termina (o se agotan los
// bytes, que el lector ve como EOF a media trama).
void feed_until_done(nexus::FakeProactor& fake, const nexus::task<void>& driver,
                     nexus::ByteSpan wire) {
    std::size_t off = 0;
    while (!driver.done() && fake.pending() > 0) {
        off += fake.deliver_recv_front(wire.subspan(off));
        fake.run_completions(1);
    }
}

TEST(FrameReader, ReadFrame_TramaCompleta_DecodificaCabeceraYPayload) {
    nexus::FrameHeader header{};
    header.api_key = nexus::ApiKey::Produce;
    header.api_version = 3;
    header.correlation_id = 0xDEADBEEF;
    header.flags = nexus::FrameHeader::kFlagCreditUpdate;
    const std::vector<std::byte> wire = encode_frame(header, as_bytes("payload-de-prueba"));

    nexus::FakeProactor fake;
    const int raw = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(raw, 0);
    nexus::Socket sock{raw};
    nexus::FrameReader reader{sock};

    std::optional<nexus::expected<nexus::Frame>> result;
    auto driver = collect(reader.read_frame(fake, 64U * 1024U), result);
    driver.handle().resume();
    feed_until_done(fake, driver, wire);

    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    const nexus::Frame& frame = **result;
    EXPECT_EQ(frame.header.api_key, nexus::ApiKey::Produce);
    EXPECT_EQ(frame.header.api_version, 3);
    EXPECT_EQ(frame.header.correlation_id, 0xDEADBEEFU);
    EXPECT_TRUE(frame.header.has_credit_update());
    const std::string payload(reinterpret_cast<const char*>(frame.payload.data()),
                              frame.payload.size());
    EXPECT_EQ(payload, "payload-de-prueba");
}

TEST(FrameReader, ReadFrame_ExcedeMaxFrame_DevuelveInvalidArgument) {
    // Cabecera que declara un length enorme; max_frame pequeño → rechazo antes de leer el cuerpo.
    nexus::FrameHeader header{};
    const std::vector<std::byte> wire = encode_frame(header, as_bytes(std::string(4096, 'x')));

    nexus::FakeProactor fake;
    const int raw = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(raw, 0);
    nexus::Socket sock{raw};
    nexus::FrameReader reader{sock};

    std::optional<nexus::expected<nexus::Frame>> result;
    auto driver = collect(reader.read_frame(fake, 64), result);  // max_frame = 64 bytes
    driver.handle().resume();
    feed_until_done(fake, driver, wire);

    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(FrameReader, ReadFrame_LengthMenorQueCabecera_DevuelveInvalidArgument) {
    // length = 5 (< 10 = resto de cabecera): trama imposible.
    nexus::Buffer buf;
    nexus::Encoder enc{buf};
    enc.put_u32(5);
    const nexus::ByteSpan wire = buf.as_span();

    nexus::FakeProactor fake;
    const int raw = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(raw, 0);
    nexus::Socket sock{raw};
    nexus::FrameReader reader{sock};

    std::optional<nexus::expected<nexus::Frame>> result;
    auto driver = collect(reader.read_frame(fake, 64U * 1024U), result);
    driver.handle().resume();
    feed_until_done(fake, driver, wire);

    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(FrameReader, ReadFrame_CierreAMediaTrama_DevuelveIoError) {
    nexus::FrameHeader header{};
    const std::vector<std::byte> full = encode_frame(header, as_bytes("cuerpo"));
    // Solo entregamos los 4 bytes de length; el cuerpo nunca llega → EOF a media trama.
    const nexus::ByteSpan truncated{full.data(), sizeof(std::uint32_t)};

    nexus::FakeProactor fake;
    const int raw = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(raw, 0);
    nexus::Socket sock{raw};
    nexus::FrameReader reader{sock};

    std::optional<nexus::expected<nexus::Frame>> result;
    auto driver = collect(reader.read_frame(fake, 64U * 1024U), result);
    driver.handle().resume();
    feed_until_done(fake, driver, truncated);

    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().code(), nexus::ErrorCode::IoError);
}

TEST(FrameWriter, WriteFrame_EnviaCabeceraYPayloadConLengthRecalculado) {
    nexus::FakeProactor fake;
    const int raw = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(raw, 0);
    nexus::Socket sock{raw};
    nexus::FrameWriter writer{sock};

    nexus::FrameHeader header{};
    header.api_key = nexus::ApiKey::Fetch;
    header.correlation_id = 7;
    const std::string payload = "hola wire";

    std::optional<nexus::expected<void>> result;
    auto driver = collect(writer.write_frame(fake, header, as_bytes(payload)), result);
    driver.handle().resume();

    // 1ª send: la cabecera (14 bytes), con length recalculado.
    ASSERT_EQ(fake.pending(), 1U);
    EXPECT_EQ(fake.peek(0).kind, nexus::FakeProactor::OpKind::Send);
    const nexus::ByteSpan header_sent = fake.peek(0).write_buffer;
    ASSERT_EQ(header_sent.size(), nexus::FrameHeader::kEncodedSize);
    nexus::Decoder dec{header_sent};
    const nexus::expected<nexus::FrameHeader> decoded = nexus::FrameHeader::decode(dec);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->api_key, nexus::ApiKey::Fetch);
    EXPECT_EQ(decoded->correlation_id, 7U);
    EXPECT_EQ(decoded->length, nexus::FrameHeader::length_for(payload.size()));
    fake.arm_front(static_cast<std::int32_t>(header_sent.size()));
    fake.run_completions(1);

    // 2ª send: el payload.
    ASSERT_EQ(fake.pending(), 1U);
    const nexus::ByteSpan payload_sent = fake.peek(0).write_buffer;
    const std::string sent(reinterpret_cast<const char*>(payload_sent.data()), payload_sent.size());
    EXPECT_EQ(sent, payload);
    fake.arm_front(static_cast<std::int32_t>(payload_sent.size()));
    fake.run_completions(1);

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->has_value());
}

TEST(FrameWriter, WriteFrame_EnvioParcial_ReintentaHastaCompletar) {
    nexus::FakeProactor fake;
    const int raw = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(raw, 0);
    nexus::Socket sock{raw};
    nexus::FrameWriter writer{sock};

    const nexus::FrameHeader header{};
    std::optional<nexus::expected<void>> result;
    auto driver = collect(writer.write_frame(fake, header, as_bytes("xy")), result);
    driver.handle().resume();

    // La cabecera se envía en dos trozos (8 + 6) para ejercitar el bucle de send_all.
    ASSERT_EQ(fake.pending(), 1U);
    EXPECT_EQ(fake.peek(0).write_buffer.size(), nexus::FrameHeader::kEncodedSize);
    fake.arm_front(8);
    fake.run_completions(1);
    ASSERT_EQ(fake.pending(), 1U);
    EXPECT_EQ(fake.peek(0).write_buffer.size(), nexus::FrameHeader::kEncodedSize - 8);
    fake.arm_front(static_cast<std::int32_t>(nexus::FrameHeader::kEncodedSize - 8));
    fake.run_completions(1);

    // Payload "xy" (2 bytes) en una sola send.
    ASSERT_EQ(fake.pending(), 1U);
    EXPECT_EQ(fake.peek(0).write_buffer.size(), 2U);
    fake.arm_front(2);
    fake.run_completions(1);

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->has_value());
}

#ifdef NEXUS_HAVE_IOURING

struct ServeResult {
    std::uint32_t correlation_id = 0;
    std::string payload;
};

// Servidor: acepta una conexión, lee una trama y responde con otra (payload "eco:" + recibido).
nexus::task<nexus::expected<ServeResult>> serve_one(nexus::Proactor& proactor,
                                                    nexus::Listener& listener) {
    nexus::expected<nexus::Socket> sock = co_await listener.async_accept(proactor);
    if (!sock) {
        co_return std::unexpected(sock.error());
    }
    nexus::FrameReader reader{*sock};
    nexus::FrameWriter writer{*sock};

    const nexus::expected<nexus::Frame> request = co_await reader.read_frame(proactor, 1U << 20U);
    if (!request) {
        co_return std::unexpected(request.error());
    }
    ServeResult result;
    result.correlation_id = request->header.correlation_id;
    result.payload.assign(reinterpret_cast<const char*>(request->payload.data()),
                          request->payload.size());

    nexus::FrameHeader response{};
    response.api_key = request->header.api_key;
    response.correlation_id = request->header.correlation_id;
    const std::string body = "eco:" + result.payload;
    const nexus::expected<void> written =
        co_await writer.write_frame(proactor, response, as_bytes(body));
    if (!written) {
        co_return std::unexpected(written.error());
    }
    co_return result;
}

TEST(FrameIoUring, RequestResponse_RoundTripPorLoopback) {
    try {
        (void)nexus::IoUringBackend{8};
    } catch (const std::system_error&) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    auto listener = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener.has_value());
    const std::uint16_t port = listener->local_port();
    ASSERT_GT(port, 0);

    nexus::IoUringBackend proactor{32};
    auto server = serve_one(proactor, *listener);
    server.handle().resume();  // arranca → accept pendiente

    const std::string request_payload = "hola NexusMQ";
    std::string echoed;
    std::uint32_t echoed_corr = 0;
    std::thread client([&] {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return;
        }
        timeval timeout{.tv_sec = 5, .tv_usec = 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(fd);
            return;
        }
        nexus::FrameHeader req{};
        req.api_key = nexus::ApiKey::Produce;
        req.correlation_id = 99;
        const std::vector<std::byte> wire = encode_frame(req, as_bytes(request_payload));
        ::send(fd, wire.data(), wire.size(), 0);

        // Lee la respuesta: length:u32, luego ese número de bytes, y decodifica.
        std::array<std::byte, 4> len_buf{};
        if (::recv(fd, len_buf.data(), len_buf.size(), MSG_WAITALL) == 4) {
            nexus::Decoder len_dec{nexus::ByteSpan{len_buf.data(), len_buf.size()}};
            const std::uint32_t rest = len_dec.get_u32().value_or(0);
            std::vector<std::byte> body(rest);
            if (rest > 0 &&
                ::recv(fd, body.data(), body.size(), MSG_WAITALL) == static_cast<ssize_t>(rest)) {
                // Reconstruye [length][rest] para decodificar con el codec.
                std::vector<std::byte> full(len_buf.begin(), len_buf.end());
                full.insert(full.end(), body.begin(), body.end());
                nexus::Decoder dec{nexus::ByteSpan{full.data(), full.size()}};
                const nexus::expected<nexus::FrameHeader> hdr = nexus::FrameHeader::decode(dec);
                if (hdr) {
                    echoed_corr = hdr->correlation_id;
                    const nexus::ByteSpan payload{full.data() + nexus::FrameHeader::kEncodedSize,
                                                  full.size() - nexus::FrameHeader::kEncodedSize};
                    echoed.assign(reinterpret_cast<const char*>(payload.data()), payload.size());
                }
            }
        }
        ::close(fd);
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!server.done() && std::chrono::steady_clock::now() < deadline) {
        proactor.run_completions(16);
    }
    client.join();

    ASSERT_TRUE(server.done());
    const nexus::expected<ServeResult> result = server.handle().promise().result();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->correlation_id, 99U);
    EXPECT_EQ(result->payload, request_payload);
    EXPECT_EQ(echoed_corr, 99U);
    EXPECT_EQ(echoed, "eco:" + request_payload);
}

#endif  // NEXUS_HAVE_IOURING

}  // namespace
