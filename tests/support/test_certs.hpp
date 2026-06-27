// Generación de certificados de prueba (autofirmados) para los tests de TLS. Solo se compila con
// OpenSSL disponible; usa la API C de OpenSSL (los tests están exentos de clang-tidy).
#pragma once

#ifdef NEXUS_HAVE_OPENSSL

#include <openssl/asn1.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <filesystem>
#include <string>

namespace nexus::testing {

// Genera un par de claves EC P-256 con un certificado X.509 autofirmado de CN=@p common_name y los
// escribe en formato PEM: el certificado en @p cert_path y la clave privada en @p key_path.
// Devuelve true si todo el proceso fue bien. Un certificado autofirmado actúa además como su propia
// CA, lo que permite probar mTLS con un único par.
inline bool write_self_signed(const std::filesystem::path& cert_path,
                              const std::filesystem::path& key_path,
                              const std::string& common_name) {
    EVP_PKEY* pkey = EVP_EC_gen("P-256");
    if (pkey == nullptr) {
        return false;
    }
    X509* x509 = X509_new();
    bool ok = (x509 != nullptr);
    if (ok) {
        ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);
        X509_gmtime_adj(X509_getm_notBefore(x509), 0);
        X509_gmtime_adj(X509_getm_notAfter(x509), 60L * 60L * 24L * 365L);
        X509_set_pubkey(x509, pkey);
        X509_NAME* name = X509_get_subject_name(x509);
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                   reinterpret_cast<const unsigned char*>(common_name.c_str()), -1,
                                   -1, 0);
        X509_set_issuer_name(x509, name);  // autofirmado: emisor == sujeto
        ok = (X509_sign(x509, pkey, EVP_sha256()) > 0);
    }
    // Escribimos vía BIO_new_file (no `std::fopen` + `FILE*`): OpenSSL abre el fichero con SU
    // propio runtime, así que no cruzamos un `FILE*` entre el CRT de la app y el de OpenSSL. En
    // Windows eso evita el fallo «no OPENSSL_Applink» (que exige enlazar applink.c al pasar un
    // FILE* ajeno) y de paso resuelve la ruta narrow sin tocar `path::c_str()` (wchar_t en
    // Windows). (W3-B, ADR-0028)
    if (ok) {
        BIO* key_bio = BIO_new_file(key_path.string().c_str(), "wb");
        ok = (key_bio != nullptr);
        if (key_bio != nullptr) {
            ok = ok && (PEM_write_bio_PrivateKey(key_bio, pkey, nullptr, nullptr, 0, nullptr,
                                                 nullptr) == 1);
            BIO_free(key_bio);
        }
    }
    if (ok) {
        BIO* cert_bio = BIO_new_file(cert_path.string().c_str(), "wb");
        ok = (cert_bio != nullptr);
        if (cert_bio != nullptr) {
            ok = ok && (PEM_write_bio_X509(cert_bio, x509) == 1);
            BIO_free(cert_bio);
        }
    }
    X509_free(x509);
    EVP_PKEY_free(pkey);
    return ok;
}

}  // namespace nexus::testing

#endif  // NEXUS_HAVE_OPENSSL
