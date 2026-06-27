/// @file   ingress/tls.cpp
/// @brief  Implementación de TlsContext/TlsConnection sobre OpenSSL (TLS 1.3, BIOs de memoria).
/// @ingroup ingress

#include "ingress/tls.hpp"

#ifdef NEXUS_HAVE_OPENSSL

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace nexus {

namespace {

/// Tamaño máximo de un registro TLS; dimensiona los búferes del puente BIO↔socket.
constexpr std::size_t kTlsChunk = std::size_t{16} * 1024;
/// Cota para acotar `std::size_t`→`int` en las llamadas a OpenSSL (que toman `int`).
constexpr int kMaxTlsIo = 1 << 20;

/// Trunca @p n al rango de `int` que aceptan `SSL_read`/`SSL_write`.
int clamp_io_len(std::size_t n) noexcept {
    return static_cast<int>(std::min<std::size_t>(n, static_cast<std::size_t>(kMaxTlsIo)));
}

/// Compone un mensaje de error a partir de @p context y la cola de errores de OpenSSL.
std::string tls_error(std::string_view context) {
    std::string message{context};
    if (const unsigned long code = ERR_get_error(); code != 0) {
        std::array<char, 256> buffer{};
        ERR_error_string_n(code, buffer.data(), buffer.size());
        message += ": ";
        message += buffer.data();
    }
    return message;
}

using CtxGuard = std::unique_ptr<SSL_CTX, void (*)(SSL_CTX*)>;

/// Crea un `SSL_CTX` con @p method, fijado a TLS 1.3 en ambos extremos del rango.
expected<CtxGuard> make_ctx(const SSL_METHOD* method, std::string_view role) {
    SSL_CTX* raw = SSL_CTX_new(method);
    if (raw == nullptr) {
        return make_error(ErrorCode::IoError, tls_error("SSL_CTX_new"));
    }
    CtxGuard ctx{raw, SSL_CTX_free};
    if (SSL_CTX_set_min_proto_version(ctx.get(), TLS1_3_VERSION) != 1 ||
        SSL_CTX_set_max_proto_version(ctx.get(), TLS1_3_VERSION) != 1) {
        return make_error(ErrorCode::IoError, tls_error(std::string{role} + ": fijar TLS 1.3"));
    }
    return ctx;
}

// OpenSSL recibe rutas como `const char*`. `std::filesystem::path::c_str()` ya es `const char*` en
// POSIX, pero `const wchar_t*` en Windows; ahí materializamos la ruta narrow del SO en una
// std::string cuyo `c_str()` sobrevive a la llamada. En POSIX devuelve una referencia a la propia
// ruta: `native_path(p).c_str()` queda byte-idéntico a `p.c_str()` (cero copia).
#if defined(_WIN32)
[[nodiscard]] std::string native_path(const std::filesystem::path& path) {
    return path.string();
}
#else
[[nodiscard]] const std::filesystem::path& native_path(const std::filesystem::path& path) {
    return path;
}
#endif

/// Carga el par certificado/clave PEM en @p ctx y comprueba que casan.
expected<void> load_keypair(SSL_CTX* ctx, const std::filesystem::path& cert,
                            const std::filesystem::path& key) {
    if (SSL_CTX_use_certificate_chain_file(ctx, native_path(cert).c_str()) != 1) {
        return make_error(ErrorCode::InvalidArgument, tls_error("cargar certificado"));
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, native_path(key).c_str(), SSL_FILETYPE_PEM) != 1) {
        return make_error(ErrorCode::InvalidArgument, tls_error("cargar clave privada"));
    }
    if (SSL_CTX_check_private_key(ctx) != 1) {
        return make_error(ErrorCode::InvalidArgument,
                          tls_error("la clave no casa con el certificado"));
    }
    return {};
}

}  // namespace

// ---- TlsContext ----------------------------------------------------------------------

TlsContext::TlsContext(TlsContext&& other) noexcept
    : ctx_(std::exchange(other.ctx_, nullptr)), require_client_cert_(other.require_client_cert_) {}

TlsContext& TlsContext::operator=(TlsContext&& other) noexcept {
    if (this != &other) {
        if (ctx_ != nullptr) {
            SSL_CTX_free(ctx_);
        }
        ctx_ = std::exchange(other.ctx_, nullptr);
        require_client_cert_ = other.require_client_cert_;
    }
    return *this;
}

TlsContext::~TlsContext() {
    if (ctx_ != nullptr) {
        SSL_CTX_free(ctx_);
    }
}

expected<TlsContext> TlsContext::server(const std::filesystem::path& cert_chain,
                                        const std::filesystem::path& private_key,
                                        const std::filesystem::path& client_ca) {
    expected<CtxGuard> ctx = make_ctx(TLS_server_method(), "servidor");
    if (!ctx) {
        return std::unexpected(ctx.error());
    }
    if (const expected<void> loaded = load_keypair(ctx->get(), cert_chain, private_key); !loaded) {
        return std::unexpected(loaded.error());
    }
    bool require_client_cert = false;
    if (!client_ca.empty()) {
        if (SSL_CTX_load_verify_locations(ctx->get(), native_path(client_ca).c_str(), nullptr) !=
            1) {
            return make_error(ErrorCode::InvalidArgument, tls_error("cargar CA de cliente"));
        }
        SSL_CTX_set_verify(ctx->get(), SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
        require_client_cert = true;
    }
    return TlsContext{ctx->release(), require_client_cert};
}

expected<TlsContext> TlsContext::client(const std::filesystem::path& ca_to_verify,
                                        const std::filesystem::path& cert,
                                        const std::filesystem::path& key) {
    expected<CtxGuard> ctx = make_ctx(TLS_client_method(), "cliente");
    if (!ctx) {
        return std::unexpected(ctx.error());
    }
    if (!ca_to_verify.empty()) {
        if (SSL_CTX_load_verify_locations(ctx->get(), native_path(ca_to_verify).c_str(), nullptr) !=
            1) {
            return make_error(ErrorCode::InvalidArgument, tls_error("cargar CA del servidor"));
        }
        SSL_CTX_set_verify(ctx->get(), SSL_VERIFY_PEER, nullptr);
    }
    if (!cert.empty() && !key.empty()) {
        if (const expected<void> loaded = load_keypair(ctx->get(), cert, key); !loaded) {
            return std::unexpected(loaded.error());
        }
    }
    return TlsContext{ctx->release(), false};
}

expected<TlsConnection> TlsContext::accept(Socket sock) const {
    SSL* ssl = SSL_new(ctx_);
    if (ssl == nullptr) {
        return make_error(ErrorCode::IoError, tls_error("SSL_new"));
    }
    BIO* rbio = BIO_new(BIO_s_mem());
    BIO* wbio = BIO_new(BIO_s_mem());
    if (rbio == nullptr || wbio == nullptr) {
        BIO_free(rbio);
        BIO_free(wbio);
        SSL_free(ssl);
        return make_error(ErrorCode::IoError, tls_error("BIO_new"));
    }
    SSL_set_bio(ssl, rbio, wbio);  // ssl adopta ambos BIOs (los libera SSL_free)
    SSL_set_accept_state(ssl);
    return TlsConnection{ssl, rbio, wbio, std::move(sock)};
}

expected<TlsConnection> TlsContext::connect(Socket sock) const {
    SSL* ssl = SSL_new(ctx_);
    if (ssl == nullptr) {
        return make_error(ErrorCode::IoError, tls_error("SSL_new"));
    }
    BIO* rbio = BIO_new(BIO_s_mem());
    BIO* wbio = BIO_new(BIO_s_mem());
    if (rbio == nullptr || wbio == nullptr) {
        BIO_free(rbio);
        BIO_free(wbio);
        SSL_free(ssl);
        return make_error(ErrorCode::IoError, tls_error("BIO_new"));
    }
    SSL_set_bio(ssl, rbio, wbio);
    SSL_set_connect_state(ssl);
    return TlsConnection{ssl, rbio, wbio, std::move(sock)};
}

// ---- TlsConnection -------------------------------------------------------------------

TlsConnection::TlsConnection(TlsConnection&& other) noexcept
    : ssl_(std::exchange(other.ssl_, nullptr)),
      rbio_(std::exchange(other.rbio_, nullptr)),
      wbio_(std::exchange(other.wbio_, nullptr)),
      sock_(std::move(other.sock_)) {}

TlsConnection& TlsConnection::operator=(TlsConnection&& other) noexcept {
    if (this != &other) {
        if (ssl_ != nullptr) {
            SSL_free(ssl_);  // libera también rbio_/wbio_
        }
        ssl_ = std::exchange(other.ssl_, nullptr);
        rbio_ = std::exchange(other.rbio_, nullptr);
        wbio_ = std::exchange(other.wbio_, nullptr);
        sock_ = std::move(other.sock_);
    }
    return *this;
}

TlsConnection::~TlsConnection() {
    if (ssl_ != nullptr) {
        SSL_free(ssl_);  // libera el SSL y sus BIOs adoptados
    }
}

task<expected<void>> TlsConnection::flush_outgoing(Proactor& proactor) {
    std::array<std::byte, kTlsChunk> chunk{};
    while (BIO_ctrl_pending(wbio_) > 0) {
        const int read = BIO_read(wbio_, chunk.data(), static_cast<int>(chunk.size()));
        if (read <= 0) {
            break;
        }
        ByteSpan pending{chunk.data(), static_cast<std::size_t>(read)};
        while (!pending.empty()) {
            const expected<std::size_t> sent = co_await sock_.async_send(proactor, pending);
            if (!sent) {
                co_return std::unexpected(sent.error());
            }
            if (*sent == 0) {
                co_return make_error(ErrorCode::IoError, "el par cerró durante el envío TLS");
            }
            pending = pending.subspan(*sent);
        }
    }
    co_return expected<void>{};
}

task<expected<std::size_t>> TlsConnection::feed_incoming(Proactor& proactor) {
    std::array<std::byte, kTlsChunk> chunk{};
    const expected<std::size_t> got =
        co_await sock_.async_recv(proactor, MutByteSpan{chunk.data(), chunk.size()});
    if (!got) {
        co_return std::unexpected(got.error());
    }
    if (*got == 0) {
        co_return std::size_t{0};  // el par cerró el transporte
    }
    const int written = BIO_write(rbio_, chunk.data(), static_cast<int>(*got));
    if (written <= 0 || std::cmp_not_equal(written, *got)) {
        co_return make_error(ErrorCode::IoError, "BIO_write al BIO de entrada TLS falló");
    }
    co_return *got;
}

task<expected<void>> TlsConnection::handshake(Proactor& proactor) {
    while (true) {
        ERR_clear_error();
        const int ret = SSL_do_handshake(ssl_);
        if (ret == 1) {
            co_return co_await flush_outgoing(proactor);  // vacía el último flight
        }
        const int err = SSL_get_error(ssl_, ret);
        if (err == SSL_ERROR_WANT_READ) {
            if (const expected<void> flushed = co_await flush_outgoing(proactor); !flushed) {
                co_return std::unexpected(flushed.error());
            }
            const expected<std::size_t> fed = co_await feed_incoming(proactor);
            if (!fed) {
                co_return std::unexpected(fed.error());
            }
            if (*fed == 0) {
                co_return make_error(ErrorCode::IoError, "el par cerró durante el handshake TLS");
            }
            continue;
        }
        if (err == SSL_ERROR_WANT_WRITE) {
            if (const expected<void> flushed = co_await flush_outgoing(proactor); !flushed) {
                co_return std::unexpected(flushed.error());
            }
            continue;
        }
        // Error terminal: OpenSSL pudo encolar una alerta fatal (p. ej. verificación fallida);
        // la enviamos al par (mejor esfuerzo) para un cierre limpio antes de propagar el error.
        const std::string detail = tls_error("handshake TLS");
        (void)co_await flush_outgoing(proactor);
        co_return make_error(ErrorCode::IoError, detail);
    }
}

task<expected<std::size_t>> TlsConnection::async_send(Proactor& proactor, ByteSpan data) {
    if (data.empty()) {
        co_return std::size_t{0};
    }
    while (true) {
        ERR_clear_error();
        const int ret = SSL_write(ssl_, data.data(), clamp_io_len(data.size()));
        if (ret > 0) {
            if (const expected<void> flushed = co_await flush_outgoing(proactor); !flushed) {
                co_return std::unexpected(flushed.error());
            }
            co_return static_cast<std::size_t>(ret);
        }
        const int err = SSL_get_error(ssl_, ret);
        if (err == SSL_ERROR_WANT_READ) {
            if (const expected<void> flushed = co_await flush_outgoing(proactor); !flushed) {
                co_return std::unexpected(flushed.error());
            }
            const expected<std::size_t> fed = co_await feed_incoming(proactor);
            if (!fed) {
                co_return std::unexpected(fed.error());
            }
            if (*fed == 0) {
                co_return make_error(ErrorCode::IoError, "el par cerró durante el envío TLS");
            }
            continue;
        }
        if (err == SSL_ERROR_WANT_WRITE) {
            if (const expected<void> flushed = co_await flush_outgoing(proactor); !flushed) {
                co_return std::unexpected(flushed.error());
            }
            continue;
        }
        co_return make_error(ErrorCode::IoError, tls_error("SSL_write"));
    }
}

task<expected<std::size_t>> TlsConnection::async_recv(Proactor& proactor, MutByteSpan buffer) {
    if (buffer.empty()) {
        co_return std::size_t{0};
    }
    while (true) {
        ERR_clear_error();
        const int ret = SSL_read(ssl_, buffer.data(), clamp_io_len(buffer.size()));
        if (ret > 0) {
            co_return static_cast<std::size_t>(ret);
        }
        const int err = SSL_get_error(ssl_, ret);
        if (err == SSL_ERROR_ZERO_RETURN) {
            co_return std::size_t{0};  // close_notify limpio
        }
        if (err == SSL_ERROR_WANT_READ) {
            if (const expected<void> flushed = co_await flush_outgoing(proactor); !flushed) {
                co_return std::unexpected(flushed.error());
            }
            const expected<std::size_t> fed = co_await feed_incoming(proactor);
            if (!fed) {
                co_return std::unexpected(fed.error());
            }
            if (*fed == 0) {
                co_return std::size_t{0};  // el par cerró: EOF
            }
            continue;
        }
        if (err == SSL_ERROR_WANT_WRITE) {
            if (const expected<void> flushed = co_await flush_outgoing(proactor); !flushed) {
                co_return std::unexpected(flushed.error());
            }
            continue;
        }
        co_return make_error(ErrorCode::IoError, tls_error("SSL_read"));
    }
}

std::optional<std::string> TlsConnection::peer_principal() const {
    X509* cert = SSL_get1_peer_certificate(ssl_);  // +1 referencia: liberar con X509_free
    if (cert == nullptr) {
        return std::nullopt;
    }
    std::array<char, 256> buffer{};
    const X509_NAME* name = X509_get_subject_name(cert);
    const int len = X509_NAME_get_text_by_NID(name, NID_commonName, buffer.data(),
                                              static_cast<int>(buffer.size()));
    X509_free(cert);
    if (len < 0) {
        return std::nullopt;
    }
    return std::string(buffer.data(), static_cast<std::size_t>(len));
}

}  // namespace nexus

#endif  // NEXUS_HAVE_OPENSSL
