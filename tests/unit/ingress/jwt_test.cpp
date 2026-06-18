// JwtVerifier (HS256, RFC 7515/7519): firma válida/ inválida, claims temporales con reloj
// inyectado, rechazo de algoritmos no-HS256 e interoperabilidad con un token real (jwt.io).
#include "ingress/jwt.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "common/base64.hpp"
#include "common/sha256.hpp"

namespace {

nexus::ByteSpan bytes_of(std::string_view text) {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

// Acuña un JWT HS256 firmando "header.payload" con el secreto (cabecera fija HS256).
std::string make_token(std::string_view secret, std::string_view payload_json) {
    const std::string header = R"({"alg":"HS256","typ":"JWT"})";
    const std::string signing = nexus::base64url_encode(bytes_of(header)) + "." +
                                nexus::base64url_encode(bytes_of(payload_json));
    const nexus::Sha256Digest mac = nexus::hmac_sha256(bytes_of(secret), bytes_of(signing));
    return signing + "." + nexus::base64url_encode(mac);
}

constexpr std::string_view kSecret = "una-clave-secreta-de-pruebas";

TEST(JwtVerifier, TokenValido_DevuelvePrincipal) {
    const std::string token =
        make_token(kSecret, R"({"sub":"alice","exp":2000,"roles":["admin","reader"]})");
    nexus::JwtVerifier verifier{std::string{kSecret}};

    const auto principal = verifier.verify(token, 1000);
    ASSERT_TRUE(principal.has_value());
    EXPECT_EQ(principal->subject, "alice");
    ASSERT_EQ(principal->roles.size(), 2U);
    EXPECT_EQ(principal->roles[0], "admin");
    EXPECT_EQ(principal->roles[1], "reader");
}

TEST(JwtVerifier, SecretoIncorrecto_Rechaza) {
    const std::string token = make_token(kSecret, R"({"sub":"alice","exp":2000})");
    nexus::JwtVerifier verifier{"otra-clave-distinta"};
    const auto result = verifier.verify(token, 1000);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(JwtVerifier, FirmaManipulada_Rechaza) {
    std::string token = make_token(kSecret, R"({"sub":"alice","exp":2000})");
    // Altera el primer carácter de la firma (bits significativos, no de relleno).
    const std::size_t sig_pos = token.rfind('.') + 1;
    token[sig_pos] = (token[sig_pos] == 'A') ? 'B' : 'A';
    nexus::JwtVerifier verifier{std::string{kSecret}};
    EXPECT_FALSE(verifier.verify(token, 1000).has_value());
}

TEST(JwtVerifier, Expirado_Rechaza) {
    const std::string token = make_token(kSecret, R"({"sub":"alice","exp":500})");
    nexus::JwtVerifier verifier{std::string{kSecret}};
    EXPECT_FALSE(verifier.verify(token, 1000).has_value());
}

TEST(JwtVerifier, AunNoValido_Nbf_Rechaza) {
    const std::string token = make_token(kSecret, R"({"sub":"alice","exp":5000,"nbf":2000})");
    nexus::JwtVerifier verifier{std::string{kSecret}};
    EXPECT_FALSE(verifier.verify(token, 1000).has_value());  // now < nbf.
    EXPECT_TRUE(verifier.verify(token, 3000).has_value());   // now >= nbf y < exp.
}

TEST(JwtVerifier, Leeway_ToleraDesfase) {
    const std::string token = make_token(kSecret, R"({"sub":"alice","exp":900})");
    nexus::JwtOptions options;
    options.leeway_seconds = 200;
    nexus::JwtVerifier verifier{std::string{kSecret}, options};
    EXPECT_TRUE(verifier.verify(token, 1000).has_value());   // 1000 <= 900 + 200.
    EXPECT_FALSE(verifier.verify(token, 1200).has_value());  // 1200 > 900 + 200.
}

TEST(JwtVerifier, RequireExp_RechazaSinExp) {
    const std::string token = make_token(kSecret, R"({"sub":"alice"})");
    nexus::JwtVerifier strict{std::string{kSecret}};  // require_exp = true por defecto.
    EXPECT_FALSE(strict.verify(token, 1000).has_value());

    nexus::JwtOptions lax;
    lax.require_exp = false;
    nexus::JwtVerifier lenient{std::string{kSecret}, lax};
    EXPECT_TRUE(lenient.verify(token, 1000).has_value());
}

TEST(JwtVerifier, IssuerYAudiencia) {
    const std::string token = make_token(
        kSecret, R"({"sub":"alice","exp":2000,"iss":"nexusmq","aud":["admin-api","metrics"]})");
    nexus::JwtOptions options;
    options.issuer = "nexusmq";
    options.audience = "admin-api";
    nexus::JwtVerifier verifier{std::string{kSecret}, options};
    EXPECT_TRUE(verifier.verify(token, 1000).has_value());

    nexus::JwtOptions wrong = options;
    wrong.issuer = "otro";
    const nexus::JwtVerifier wrong_issuer{std::string{kSecret}, wrong};
    EXPECT_FALSE(wrong_issuer.verify(token, 1000).has_value());

    nexus::JwtOptions wrong_aud = options;
    wrong_aud.audience = "no-presente";
    const nexus::JwtVerifier wrong_audience{std::string{kSecret}, wrong_aud};
    EXPECT_FALSE(wrong_audience.verify(token, 1000).has_value());
}

TEST(JwtVerifier, AlgNone_Rechaza) {
    // Cabecera con alg=none y firma vacía: el ataque clásico debe rechazarse.
    const std::string header = nexus::base64url_encode(bytes_of(R"({"alg":"none","typ":"JWT"})"));
    const std::string payload = nexus::base64url_encode(bytes_of(R"({"sub":"attacker"})"));
    const std::string token = header + "." + payload + ".";
    nexus::JwtVerifier verifier{std::string{kSecret}};
    EXPECT_FALSE(verifier.verify(token, 1000).has_value());
}

TEST(JwtVerifier, FormatoInvalido_Rechaza) {
    nexus::JwtVerifier verifier{std::string{kSecret}};
    EXPECT_FALSE(verifier.verify("no-es-un-jwt", 1000).has_value());
    EXPECT_FALSE(verifier.verify("solo.dos", 1000).has_value());
    EXPECT_FALSE(verifier.verify("a.b.c.d", 1000).has_value());
}

TEST(JwtVerifier, InteropJwtIo_TokenReal) {
    // Token canónico de jwt.io: secreto "your-256-bit-secret", sin exp (require_exp=false).
    constexpr std::string_view kJwtIoToken =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ."
        "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";
    nexus::JwtOptions options;
    options.require_exp = false;
    nexus::JwtVerifier verifier{"your-256-bit-secret", options};

    const auto principal = verifier.verify(kJwtIoToken, 1516239100);
    ASSERT_TRUE(principal.has_value());
    EXPECT_EQ(principal->subject, "1234567890");
}

}  // namespace
