// E2E del plano de datos cifrado (ADR-0019, D4-2): un `Server` real con TLS activado en el plano de
// datos. Un cliente TLS completa el handshake y hace Produce/Fetch sobre el canal cifrado; un
// cliente en claro es rechazado (handshake fallido → cierre). Cierra el lazo accept → handshake →
// serve_connection sobre `TlsConnection` → RequestRouter, que las unitarias de TlsContext y el e2e
// de loopback (`tls_e2e_test`) no cubren.
#include <gtest/gtest.h>

#include "ingress/tls.hpp"  // arrastra NEXUS_HAVE_OPENSSL + TlsContext

#if defined(NEXUS_HAVE_OPENSSL) && defined(NEXUS_HAVE_IOURING)

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/task.hpp"
#include "io/io_uring_backend.hpp"
#include "io/socket.hpp"
#include "protocol/codec.hpp"
#include "protocol/error_code.hpp"
#include "protocol/frame.hpp"
#include "protocol/messages.hpp"
#include "server/server.hpp"
#include "support/test_certs.hpp"
#include "wire/frame_io.hpp"

namespace {

using namespace std::chrono_literals;

// Directorio temporal: alberga el `data_dir` del servidor y los PEM del certificado de prueba.
class TempDir {
public:
    explicit TempDir(const std::string& tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_tls_server_e2e_" + tag + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }
    [[nodiscard]] std::filesystem::path file(const char* name) const { return path_ / name; }

private:
    std::filesystem::path path_;
};

// Corrutina conductora: ejecuta @p work y guarda su resultado en @p out.
template <class T>
nexus::task<void> collect(nexus::task<T> work, std::optional<T>& out) {
    out = co_await std::move(work);
}

// Arranca cada conductor y bombea el reactor del cliente hasta que todos terminen (o venza el
// plazo).
bool drive(nexus::IoUringBackend& proactor, std::vector<nexus::task<void>*> drivers) {
    for (auto* driver : drivers) {
        driver->handle().resume();
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        bool all_done = true;
        for (auto* driver : drivers) {
            if (!driver->done()) {
                all_done = false;
            }
        }
        if (all_done) {
            return true;
        }
        proactor.run_completions(16);
    }
    return false;
}

bool iouring_available() {
    try {
        (void)nexus::IoUringBackend{8};
        return true;
    } catch (const std::system_error&) {
        return false;
    }
}

// Batch de `count` records (mismo formato que el resto de e2e: relleno de bytes 0x07).
std::vector<std::byte> encode_batch(std::int32_t count) {
    nexus::RecordBatchHeader header;
    header.record_count = count;
    const nexus::RecordBatch batch{
        header, std::vector<std::byte>(static_cast<std::size_t>(count), std::byte{0x7})};
    nexus::Buffer buf;
    batch.encode(buf);
    const nexus::ByteSpan span = buf.as_span();
    return {span.begin(), span.end()};
}

// Cliente TLS sobre un proactor propio (lo bombea el hilo de prueba): handshake + framing cifrado.
class TlsClient {
public:
    // Conecta a 127.0.0.1:@p port, envuelve en TLS con @p ctx y completa el handshake.
    bool connect(std::uint16_t port, const nexus::TlsContext& ctx) {
        nexus::expected<nexus::Socket> sock = nexus::Socket::connect("127.0.0.1", port);
        if (!sock) {
            return false;
        }
        nexus::expected<nexus::TlsConnection> conn = ctx.connect(std::move(*sock));
        if (!conn) {
            return false;
        }
        conn_.emplace(std::move(*conn));
        std::optional<nexus::expected<void>> handshaken;
        auto driver = collect(conn_->handshake(proactor_), handshaken);
        return drive(proactor_, {&driver}) && handshaken.has_value() && handshaken->has_value();
    }

    // Envía una trama (api_key/correlation/body) y devuelve el payload de la respuesta, o nullopt.
    std::optional<std::vector<std::byte>> round_trip(nexus::ApiKey key, std::uint32_t correlation,
                                                     nexus::ByteSpan body) {
        nexus::FrameWriter writer{*conn_};
        nexus::FrameHeader header;
        header.api_key = key;
        header.api_version = 0;
        header.correlation_id = correlation;
        std::optional<nexus::expected<void>> wrote;
        auto tw = collect(writer.write_frame(proactor_, header, body), wrote);
        if (!drive(proactor_, {&tw}) || !wrote.has_value() || !wrote->has_value()) {
            return std::nullopt;
        }
        nexus::FrameReader reader{*conn_};
        std::optional<nexus::expected<nexus::Frame>> got;
        auto tr = collect(reader.read_frame(proactor_, 1U << 20U), got);
        if (!drive(proactor_, {&tr}) || !got.has_value() || !got->has_value()) {
            return std::nullopt;
        }
        const nexus::Frame& frame = **got;
        return std::vector<std::byte>(frame.payload.begin(), frame.payload.end());
    }

private:
    nexus::IoUringBackend proactor_{64};
    std::optional<nexus::TlsConnection> conn_;
};

// Config de un Server TLS de un nodo (RF=1: confirma al producir) con el cert/clave de @p dir.
nexus::Server::Config make_tls_config(const TempDir& dir, const std::filesystem::path& cert,
                                      const std::filesystem::path& key) {
    nexus::Server::Config config;
    config.host = "127.0.0.1";
    config.port = 0;
    config.data_dir = dir.path() / "data";
    config.node_id = 1;
    config.num_reactors = 1;
    config.tls.cert_chain = cert;
    config.tls.private_key = key;
    return config;
}

TEST(TlsServerE2E, ProduceYFetch_SobreCanalCifrado) {
    if (!iouring_available()) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    TempDir dir{"produce_fetch"};
    const auto cert = dir.file("cert.pem");
    const auto key = dir.file("key.pem");
    ASSERT_TRUE(nexus::testing::write_self_signed(cert, key, "localhost"));

    std::optional<nexus::Server> server;
    try {
        server.emplace(make_tls_config(dir, cert, key));
    } catch (const std::system_error&) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    ASSERT_TRUE(server->create_topic("t", 1, /*replication_factor=*/1).has_value());
    ASSERT_TRUE(server->bind().has_value());
    const std::uint16_t port = server->port();
    ASSERT_GT(port, 0);
    std::thread server_thread{[srv = &*server] { srv->run(); }};

    const auto cli_ctx = nexus::TlsContext::client();  // sin CA: acepta el cert autofirmado
    ASSERT_TRUE(cli_ctx.has_value()) << cli_ctx.error().message();
    TlsClient client;
    ASSERT_TRUE(client.connect(port, *cli_ctx));

    // Produce 5 records sobre el canal cifrado.
    const std::vector<std::byte> batch = encode_batch(5);
    nexus::ProduceRequest preq;
    preq.topic = "t";
    preq.partition = 0;
    preq.batch = nexus::ByteSpan{batch.data(), batch.size()};
    nexus::Buffer pbody;
    nexus::Encoder penc{pbody};
    preq.encode(penc);
    const std::optional<std::vector<std::byte>> presp_bytes =
        client.round_trip(nexus::ApiKey::Produce, /*correlation=*/1, pbody.as_span());
    ASSERT_TRUE(presp_bytes.has_value());
    nexus::Decoder pdec{nexus::ByteSpan{presp_bytes->data(), presp_bytes->size()}};
    const nexus::expected<nexus::ProduceResponse> presp = nexus::ProduceResponse::decode(pdec);
    ASSERT_TRUE(presp.has_value());
    EXPECT_EQ(presp->error_code, nexus::WireError::None);
    EXPECT_EQ(presp->base_offset, 0);

    // Fetch desde 0: con RF=1 la escritura está confirmada → high_watermark = 5.
    nexus::FetchRequest freq;
    freq.topic = "t";
    freq.partition = 0;
    freq.fetch_offset = 0;
    freq.max_bytes = 64 * 1024;
    nexus::Buffer fbody;
    nexus::Encoder fenc{fbody};
    freq.encode(fenc);
    const std::optional<std::vector<std::byte>> fresp_bytes =
        client.round_trip(nexus::ApiKey::Fetch, /*correlation=*/2, fbody.as_span());
    ASSERT_TRUE(fresp_bytes.has_value());
    nexus::Decoder fdec{nexus::ByteSpan{fresp_bytes->data(), fresp_bytes->size()}};
    const nexus::expected<nexus::FetchResponse> fresp = nexus::FetchResponse::decode(fdec);
    ASSERT_TRUE(fresp.has_value());
    EXPECT_EQ(fresp->error_code, nexus::WireError::None);
    EXPECT_EQ(fresp->high_watermark, 5);

    server->stop();
    server_thread.join();
}

TEST(TlsServerE2E, ClienteEnClaro_EsRechazadoPorElServidorTls) {
    if (!iouring_available()) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    TempDir dir{"plaintext_rejected"};
    const auto cert = dir.file("cert.pem");
    const auto key = dir.file("key.pem");
    ASSERT_TRUE(nexus::testing::write_self_signed(cert, key, "localhost"));

    std::optional<nexus::Server> server;
    try {
        server.emplace(make_tls_config(dir, cert, key));
    } catch (const std::system_error&) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    ASSERT_TRUE(server->create_topic("t", 1, /*replication_factor=*/1).has_value());
    ASSERT_TRUE(server->bind().has_value());
    const std::uint16_t port = server->port();
    ASSERT_GT(port, 0);
    std::thread server_thread{[srv = &*server] { srv->run(); }};

    // Cliente en claro: manda bytes que el servidor TLS interpreta como un ClientHello inválido →
    // el handshake falla y la conexión se cierra, sin que se sirva ninguna petición.
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(fd, 0);
    timeval timeout{.tv_sec = 5, .tv_usec = 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);

    const std::string_view plaintext = "no soy un ClientHello TLS";
    ::send(fd, plaintext.data(), plaintext.size(), MSG_NOSIGNAL);

    // El servidor TLS **rechaza** el texto plano de una de dos formas válidas —ambas equivalentes a
    // «no degrada a claro»—: (a) cierra la conexión sin responder (`recv` → 0/EOF o <0/reset), o
    // (b) responde con un **alerta fatal TLS** antes de cerrar (un registro cuyo primer byte es el
    // content-type 0x15 = «alert»; OpenSSL suele emitir `unexpected_message`). Lo que nunca debe
    // ocurrir es que sirva datos de aplicación. Aceptamos ambas: el modo concreto depende de la
    // versión de OpenSSL y del entorno, y no forma parte del contrato.
    constexpr unsigned char kTlsAlertContentType =
        0x15;  // RFC 8446 §5.1: registro de tipo «alert».
    std::array<unsigned char, 16> buf{};
    const ssize_t got = ::recv(fd, buf.data(), buf.size(), 0);
    const bool cierre_silencioso = got <= 0;
    const bool alerta_fatal_tls = got > 0 && buf[0] == kTlsAlertContentType;
    EXPECT_TRUE(cierre_silencioso || alerta_fatal_tls)
        << "el servidor TLS no rechazó el texto plano: recv devolvió " << got
        << " byte(s), primer byte=0x" << std::hex << (got > 0 ? static_cast<int>(buf[0]) : 0);
    ::close(fd);

    server->stop();
    server_thread.join();
}

}  // namespace

#else

TEST(TlsServerE2E, OmitidoSinOpenSSLoIoUring) {
    GTEST_SKIP() << "compilado sin OpenSSL o sin io_uring";
}

#endif  // NEXUS_HAVE_OPENSSL && NEXUS_HAVE_IOURING
