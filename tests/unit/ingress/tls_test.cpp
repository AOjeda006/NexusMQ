// Pruebas unitarias de TlsContext: construcción de contextos servidor/cliente, exigencia de
// certificado de cliente (mTLS) y errores de carga. Sin reactor ni red (solo la fábrica). El
// handshake completo se ejerce por loopback en e2e/tls_e2e_test.cpp.
#include "ingress/tls.hpp"

#include <gtest/gtest.h>

#ifdef NEXUS_HAVE_OPENSSL

#include <filesystem>
#include <string>

#include "common/error.hpp"
#include "support/test_certs.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_tls_unit_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] std::filesystem::path file(const char* name) const { return path_ / name; }

private:
    std::filesystem::path path_;
};

TEST(TlsContext, Server_CertValido_SinCaNoExigeCliente) {
    TempDir dir{"srv"};
    const auto cert = dir.file("cert.pem");
    const auto key = dir.file("key.pem");
    ASSERT_TRUE(nexus::testing::write_self_signed(cert, key, "localhost"));

    const auto ctx = nexus::TlsContext::server(cert, key);
    ASSERT_TRUE(ctx.has_value()) << ctx.error().message();
    EXPECT_FALSE(ctx->require_client_cert());
}

TEST(TlsContext, Server_ConCa_ExigeCertificadoDeCliente) {
    TempDir dir{"mtls"};
    const auto cert = dir.file("cert.pem");
    const auto key = dir.file("key.pem");
    ASSERT_TRUE(nexus::testing::write_self_signed(cert, key, "localhost"));

    // El propio certificado autofirmado sirve de CA para mTLS.
    const auto ctx = nexus::TlsContext::server(cert, key, cert);
    ASSERT_TRUE(ctx.has_value()) << ctx.error().message();
    EXPECT_TRUE(ctx->require_client_cert());
}

TEST(TlsContext, Server_CertInexistente_DevuelveError) {
    const auto ctx = nexus::TlsContext::server("/no/existe/cert.pem", "/no/existe/key.pem");
    ASSERT_FALSE(ctx.has_value());
    EXPECT_EQ(ctx.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(TlsContext, Client_SinMaterial_SeCrea) {
    const auto ctx = nexus::TlsContext::client();
    ASSERT_TRUE(ctx.has_value()) << ctx.error().message();
    EXPECT_FALSE(ctx->require_client_cert());
}

}  // namespace

#else

TEST(TlsContext, OmitidoSinOpenSSL) {
    GTEST_SKIP() << "compilado sin OpenSSL";
}

#endif  // NEXUS_HAVE_OPENSSL
