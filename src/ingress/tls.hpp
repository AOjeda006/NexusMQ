/// @file   ingress/tls.hpp
/// @brief  TlsContext/TlsConnection: terminación TLS 1.3 (+ mTLS) sobre Socket vía OpenSSL.
/// @ingroup ingress

#pragma once

#ifdef NEXUS_HAVE_OPENSSL

#include <openssl/types.h>  // SSL, SSL_CTX, BIO (typedefs opacos; sin arrastrar toda la API)

#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "io/proactor.hpp"
#include "io/socket.hpp"

namespace nexus {

class TlsConnection;

/// @brief Contexto TLS compartido: RAII sobre `SSL_CTX` (OpenSSL), fijado a TLS 1.3.
/// @details Afinidad: THREAD-SAFE — un `SSL_CTX` es inmutable tras construirse y OpenSSL permite
///   crear `SSL` (por conexión) desde varios hilos; cada reactor lo comparte para abrir sus
///   `TlsConnection`. Carga la cadena de certificado y la clave privada del nodo; opcionalmente una
///   CA para **mTLS** (autenticación mutua intra-clúster, ADR-0006). Es **solo movible** (posee el
///   `SSL_CTX*`, que libera en el destructor).
/// @invariant `ctx_ != nullptr` mientras el objeto sea válido (un objeto movido-desde queda nulo).
class TlsContext {
public:
    TlsContext(TlsContext&& other) noexcept;
    TlsContext& operator=(TlsContext&& other) noexcept;
    TlsContext(const TlsContext&) = delete;
    TlsContext& operator=(const TlsContext&) = delete;
    ~TlsContext();

    /// @brief Crea un contexto de **servidor** TLS 1.3 con la cadena @p cert_chain y la clave
    ///   privada @p private_key (ambos PEM). Si @p client_ca no está vacío, exige y verifica el
    ///   certificado de cliente contra esa CA (**mTLS**). @return el contexto o `IoError`.
    [[nodiscard]] static expected<TlsContext> server(const std::filesystem::path& cert_chain,
                                                     const std::filesystem::path& private_key,
                                                     const std::filesystem::path& client_ca = {});

    /// @brief Crea un contexto de **cliente** TLS 1.3. Si @p ca_to_verify no está vacío,
    ///   verifica el certificado del servidor contra esa CA; si @p cert/@p key no están vacíos,
    ///   presenta ese certificado de cliente (**mTLS**). @return el contexto o `IoError`.
    [[nodiscard]] static expected<TlsContext> client(const std::filesystem::path& ca_to_verify = {},
                                                     const std::filesystem::path& cert = {},
                                                     const std::filesystem::path& key = {});

    /// @brief ¿Exige certificado de cliente (mTLS)? Solo cierto en servidores creados con CA.
    [[nodiscard]] bool require_client_cert() const noexcept { return require_client_cert_; }

    /// @brief Abre una `TlsConnection` en rol **servidor** (acepta el handshake) sobre @p sock.
    [[nodiscard]] expected<TlsConnection> accept(Socket sock) const;
    /// @brief Abre una `TlsConnection` en rol **cliente** (inicia el handshake) sobre @p sock.
    [[nodiscard]] expected<TlsConnection> connect(Socket sock) const;

private:
    TlsContext(SSL_CTX* ctx, bool require_client_cert) noexcept
        : ctx_(ctx), require_client_cert_(require_client_cert) {}

    SSL_CTX* ctx_ = nullptr;
    bool require_client_cert_ = false;
};

/// @brief Conexión TLS sobre un `Socket`: handshake y E/S cifrada asíncronas.
/// @details Afinidad: REACTOR-LOCAL — vive en el reactor que la creó; la criptografía corre en
///   línea y el transporte es asíncrono vía `Proactor`. Usa **BIOs de memoria** como puente:
///   OpenSSL cifra hacia un BIO de salida que vaciamos al socket, y desciframos desde un BIO de
///   entrada que alimentamos con lo recibido. Es **solo movible** (posee el `SSL*` —que libera
///   sus BIOs— y el `Socket`).
/// @invariant `ssl_ != nullptr` mientras sea válida; `rbio_`/`wbio_` son propiedad de `ssl_`.
class TlsConnection {
public:
    TlsConnection(TlsConnection&& other) noexcept;
    TlsConnection& operator=(TlsConnection&& other) noexcept;
    TlsConnection(const TlsConnection&) = delete;
    TlsConnection& operator=(const TlsConnection&) = delete;
    ~TlsConnection();

    /// @brief Ejecuta el handshake TLS (bombeando handshake records por el socket).
    [[nodiscard]] task<expected<void>> handshake(Proactor& proactor);

    /// @brief Descifra hasta `buffer.size()` bytes en @p buffer; produce los bytes obtenidos
    ///   (`0` = el par cerró la sesión, vía `close_notify` o EOF de transporte).
    [[nodiscard]] task<expected<std::size_t>> async_recv(Proactor& proactor, MutByteSpan buffer);
    /// @brief Cifra y envía @p data; produce cuántos bytes de @p data se aceptaron.
    [[nodiscard]] task<expected<std::size_t>> async_send(Proactor& proactor, ByteSpan data);

    /// @brief Principal del par extraído de su certificado (CN), para mTLS/authz; vacío si no hay.
    [[nodiscard]] std::optional<std::string> peer_principal() const;

    [[nodiscard]] const Socket& socket() const noexcept { return sock_; }

private:
    friend class TlsContext;
    TlsConnection(SSL* ssl, BIO* rbio, BIO* wbio, Socket sock) noexcept
        : ssl_(ssl), rbio_(rbio), wbio_(wbio), sock_(std::move(sock)) {}

    /// Vacía el BIO de salida (ciphertext pendiente) hacia el socket.
    [[nodiscard]] task<expected<void>> flush_outgoing(Proactor& proactor);
    /// Lee ciphertext del socket hacia el BIO de entrada; produce los bytes leídos
    ///   (`0` = el par cerró).
    [[nodiscard]] task<expected<std::size_t>> feed_incoming(Proactor& proactor);

    SSL* ssl_ = nullptr;
    BIO* rbio_ = nullptr;  // entrada: alimentamos lo recibido; OpenSSL descifra desde aquí
    BIO* wbio_ = nullptr;  // salida: OpenSSL cifra hacia aquí; lo vaciamos al socket
    Socket sock_;
};

}  // namespace nexus

#endif  // NEXUS_HAVE_OPENSSL
