#include "storage/segment_crypto.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"

#ifdef NEXUS_HAVE_OPENSSL
#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/params.h>
#include <openssl/rand.h>

#include <memory>
#endif

namespace nexus {
namespace {

// Posiciones de los campos de la cabecera de un bloque cifrado (little-endian).
constexpr std::size_t kBlkVersionPos = 0;       // u8
constexpr std::size_t kBlkFlagsPos = 1;         // u8 (reservado)
constexpr std::size_t kBlkBaseOffsetPos = 2;    // i64
constexpr std::size_t kBlkRecordCountPos = 10;  // i32
constexpr std::size_t kBlkCtLenPos = 14;        // u32
constexpr std::size_t kBlkNoncePos = 18;        // 12 bytes
constexpr std::size_t kBlkTagPos = 30;          // 16 bytes
constexpr std::size_t kBlkAadLen = kBlkTagPos;  // AAD = toda la cabecera salvo el tag.
constexpr std::uint8_t kBlockVersion = 1;

// Posiciones de la cabecera de segmento cifrado.
constexpr std::size_t kSegVersionPos = 8;    // u8
constexpr std::size_t kSegKdfIdPos = 9;      // u8
constexpr std::size_t kSegCipherIdPos = 10;  // u8
constexpr std::size_t kSegFlagsPos = 11;     // u8 (reservado)
constexpr std::size_t kSegSaltPos = 12;      // kEncSaltBytes
constexpr std::uint8_t kSegmentVersion = 1;
constexpr std::uint8_t kKdfHkdfSha256 = 1;
constexpr std::uint8_t kCipherAes256Gcm = 1;

// Contexto de derivación de la DEK (separa el dominio de claves del proyecto).
constexpr std::string_view kDekInfo = "nexusmq/segment-dek/v1";

// Convierte un dígito hexadecimal a su valor; nullopt si no es hex.
[[nodiscard]] std::optional<std::uint8_t> hex_nibble(char character) noexcept {
    if (character >= '0' && character <= '9') {
        return static_cast<std::uint8_t>(character - '0');
    }
    if (character >= 'a' && character <= 'f') {
        return static_cast<std::uint8_t>(character - 'a' + 10);
    }
    if (character >= 'A' && character <= 'F') {
        return static_cast<std::uint8_t>(character - 'A' + 10);
    }
    return std::nullopt;
}

#ifdef NEXUS_HAVE_OPENSSL

// byte↔unsigned char: alias seguro (misma representación); OpenSSL usa `unsigned char*`.
[[nodiscard]] const unsigned char* uc(ByteSpan bytes) noexcept {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): byte→uchar, alias seguro.
    return reinterpret_cast<const unsigned char*>(bytes.data());
}
[[nodiscard]] unsigned char* uc(MutByteSpan bytes) noexcept {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): byte→uchar, alias seguro.
    return reinterpret_cast<unsigned char*>(bytes.data());
}

using CipherCtx = std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)>;

// Cifra @p plaintext con AES-256-GCM (@p dek, @p nonce, @p aad); escribe ciphertext y tag.
[[nodiscard]] expected<void> aes_gcm_encrypt(ByteSpan dek, ByteSpan nonce, ByteSpan aad,
                                             ByteSpan plaintext, MutByteSpan ciphertext_out,
                                             MutByteSpan tag_out) {
    const CipherCtx ctx{EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free};
    if (!ctx) {
        return make_error(ErrorCode::IoError, "EVP_CIPHER_CTX_new falló");
    }
    int len = 0;
    if (EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1 ||
        EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN, static_cast<int>(nonce.size()),
                            nullptr) != 1 ||
        EVP_EncryptInit_ex(ctx.get(), nullptr, nullptr, uc(dek), uc(nonce)) != 1) {
        return make_error(ErrorCode::IoError, "EVP_EncryptInit (GCM) falló");
    }
    if (!aad.empty() &&
        EVP_EncryptUpdate(ctx.get(), nullptr, &len, uc(aad), static_cast<int>(aad.size())) != 1) {
        return make_error(ErrorCode::IoError, "EVP_EncryptUpdate (AAD) falló");
    }
    if (!plaintext.empty() && EVP_EncryptUpdate(ctx.get(), uc(ciphertext_out), &len, uc(plaintext),
                                                static_cast<int>(plaintext.size())) != 1) {
        return make_error(ErrorCode::IoError, "EVP_EncryptUpdate falló");
    }
    int final_len = 0;
    if (EVP_EncryptFinal_ex(ctx.get(), uc(ciphertext_out) + len, &final_len) != 1) {
        return make_error(ErrorCode::IoError, "EVP_EncryptFinal falló");
    }
    if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG, static_cast<int>(tag_out.size()),
                            uc(tag_out)) != 1) {
        return make_error(ErrorCode::IoError, "EVP_CTRL_GCM_GET_TAG falló");
    }
    return {};
}

// Descifra @p ciphertext verificando el tag GCM y la AAD; `Corrupt` si la autenticación falla.
[[nodiscard]] expected<void> aes_gcm_decrypt(ByteSpan dek, ByteSpan nonce, ByteSpan aad,
                                             ByteSpan ciphertext, ByteSpan tag,
                                             MutByteSpan plaintext_out) {
    const CipherCtx ctx{EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free};
    if (!ctx) {
        return make_error(ErrorCode::IoError, "EVP_CIPHER_CTX_new falló");
    }
    int len = 0;
    if (EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1 ||
        EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN, static_cast<int>(nonce.size()),
                            nullptr) != 1 ||
        EVP_DecryptInit_ex(ctx.get(), nullptr, nullptr, uc(dek), uc(nonce)) != 1) {
        return make_error(ErrorCode::IoError, "EVP_DecryptInit (GCM) falló");
    }
    if (!aad.empty() &&
        EVP_DecryptUpdate(ctx.get(), nullptr, &len, uc(aad), static_cast<int>(aad.size())) != 1) {
        return make_error(ErrorCode::IoError, "EVP_DecryptUpdate (AAD) falló");
    }
    if (!ciphertext.empty() && EVP_DecryptUpdate(ctx.get(), uc(plaintext_out), &len, uc(ciphertext),
                                                 static_cast<int>(ciphertext.size())) != 1) {
        return make_error(ErrorCode::IoError, "EVP_DecryptUpdate falló");
    }
    // El tag esperado (SET_TAG requiere un puntero no-const; OpenSSL solo lo lee).
    std::array<std::byte, kEncTagBytes> expected_tag{};
    std::ranges::copy(tag, expected_tag.begin());
    if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG, static_cast<int>(expected_tag.size()),
                            uc(MutByteSpan{expected_tag})) != 1) {
        return make_error(ErrorCode::IoError, "EVP_CTRL_GCM_SET_TAG falló");
    }
    int final_len = 0;
    if (EVP_DecryptFinal_ex(ctx.get(), uc(plaintext_out) + len, &final_len) <= 0) {
        return make_error(ErrorCode::Corrupt,
                          "autenticación GCM falló (datos manipulados o clave/nonce incorrectos)");
    }
    return {};
}

#endif  // NEXUS_HAVE_OPENSSL

}  // namespace

bool encryption_available() noexcept {
#ifdef NEXUS_HAVE_OPENSSL
    return true;
#else
    return false;
#endif
}

expected<EncryptionKey> EncryptionKey::from_bytes(ByteSpan key) {
    if (key.size() != kEncKeyBytes) {
        return make_error(ErrorCode::InvalidArgument, "clave de cifrado: se esperan 32 bytes");
    }
#ifndef NEXUS_HAVE_OPENSSL
    return make_error(ErrorCode::Unsupported,
                      "cifrado no disponible: broker compilado sin OpenSSL");
#else
    std::array<std::byte, kEncKeyBytes> material{};
    std::ranges::copy(key, material.begin());
    return EncryptionKey{material};
#endif
}

expected<EncryptionKey> EncryptionKey::from_hex(std::string_view hex) {
    if (hex.size() != kEncKeyBytes * 2) {
        return make_error(ErrorCode::InvalidArgument,
                          "clave de cifrado: se esperan 64 dígitos hexadecimales (256 bits)");
    }
    std::array<std::byte, kEncKeyBytes> material{};
    for (std::size_t i = 0; i < kEncKeyBytes; ++i) {
        const auto high = hex_nibble(hex[(2 * i)]);
        const auto low = hex_nibble(hex[(2 * i) + 1]);
        if (!high || !low) {
            return make_error(ErrorCode::InvalidArgument,
                              "clave de cifrado: dígito no hexadecimal");
        }
        material[i] = static_cast<std::byte>((*high << 4) | *low);
    }
    return from_bytes(material);
}

expected<std::array<std::byte, kEncKeyBytes>> EncryptionKey::derive_segment_dek(
    [[maybe_unused]] ByteSpan salt) const {
#ifndef NEXUS_HAVE_OPENSSL
    return make_error(ErrorCode::Unsupported,
                      "cifrado no disponible: broker compilado sin OpenSSL");
#else
    using KdfPtr = std::unique_ptr<EVP_KDF, decltype(&EVP_KDF_free)>;
    using KdfCtxPtr = std::unique_ptr<EVP_KDF_CTX, decltype(&EVP_KDF_CTX_free)>;
    const KdfPtr kdf{EVP_KDF_fetch(nullptr, "HKDF", nullptr), &EVP_KDF_free};
    if (!kdf) {
        return make_error(ErrorCode::IoError, "HKDF no disponible en OpenSSL");
    }
    const KdfCtxPtr ctx{EVP_KDF_CTX_new(kdf.get()), &EVP_KDF_CTX_free};
    if (!ctx) {
        return make_error(ErrorCode::IoError, "EVP_KDF_CTX_new falló");
    }
    // Copias locales no-const para la API OSSL_PARAM (solo lee, pero exige void*).
    std::array<char, 7> digest = {'S', 'H', 'A', '2', '5', '6', '\0'};
    std::array<std::byte, kEncKeyBytes> key_material = key_;
    std::vector<std::byte> salt_material(salt.begin(), salt.end());
    std::vector<std::byte> info_material(kDekInfo.size());
    for (std::size_t i = 0; i < kDekInfo.size(); ++i) {
        info_material[i] = static_cast<std::byte>(kDekInfo[i]);
    }
    std::array<OSSL_PARAM, 5> params = {
        OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, digest.data(), 0),
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_KEY, uc(MutByteSpan{key_material}),
                                          key_material.size()),
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_SALT, uc(MutByteSpan{salt_material}),
                                          salt_material.size()),
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_INFO, uc(MutByteSpan{info_material}),
                                          info_material.size()),
        OSSL_PARAM_construct_end()};
    std::array<std::byte, kEncKeyBytes> dek{};
    if (EVP_KDF_derive(ctx.get(), uc(MutByteSpan{dek}), dek.size(), params.data()) <= 0) {
        return make_error(ErrorCode::IoError, "EVP_KDF_derive (HKDF) falló");
    }
    return dek;
#endif
}

expected<SegmentCipher> SegmentCipher::create([[maybe_unused]] const EncryptionKey& kek,
                                              [[maybe_unused]] ByteSpan salt) {
#ifndef NEXUS_HAVE_OPENSSL
    return make_error(ErrorCode::Unsupported,
                      "cifrado no disponible: broker compilado sin OpenSSL");
#else
    auto dek = kek.derive_segment_dek(salt);
    if (!dek) {
        return std::unexpected(dek.error());
    }
    return SegmentCipher{*dek};
#endif
}

expected<void> SegmentCipher::seal_block([[maybe_unused]] ByteSpan plaintext,
                                         [[maybe_unused]] Offset base_offset,
                                         [[maybe_unused]] std::int32_t record_count,
                                         [[maybe_unused]] Buffer& out) const {
#ifndef NEXUS_HAVE_OPENSSL
    return make_error(ErrorCode::Unsupported,
                      "cifrado no disponible: broker compilado sin OpenSSL");
#else
    if (plaintext.size() > std::numeric_limits<std::uint32_t>::max()) {
        return make_error(ErrorCode::InvalidArgument, "batch demasiado grande para cifrar");
    }
    const auto ct_len = static_cast<std::uint32_t>(plaintext.size());

    std::array<std::byte, kEncBlockHeaderSize> header{};
    const MutByteSpan head{header};
    header[kBlkVersionPos] = static_cast<std::byte>(kBlockVersion);
    header[kBlkFlagsPos] = std::byte{0};
    store_le<std::int64_t>(base_offset, head.subspan(kBlkBaseOffsetPos));
    store_le<std::int32_t>(record_count, head.subspan(kBlkRecordCountPos));
    store_le<std::uint32_t>(ct_len, head.subspan(kBlkCtLenPos));

    std::array<std::byte, kEncNonceBytes> nonce{};
    if (const auto random = random_bytes(nonce); !random) {
        return std::unexpected(random.error());
    }
    std::ranges::copy(nonce, header.begin() + kBlkNoncePos);

    std::vector<std::byte> ciphertext(ct_len);
    std::array<std::byte, kEncTagBytes> tag{};
    // La AAD cubre la cabecera salvo el tag: autentica los metadatos en claro y el nonce.
    if (const auto sealed =
            aes_gcm_encrypt(dek_, nonce, head.subspan(0, kBlkAadLen), plaintext, ciphertext, tag);
        !sealed) {
        return sealed;
    }
    std::ranges::copy(tag, header.begin() + kBlkTagPos);

    out.append(header);
    out.append(ciphertext);
    return {};
#endif
}

expected<EncryptedBlockView> SegmentCipher::peek_block(ByteSpan frame) {
    if (frame.size() < kEncBlockHeaderSize) {
        return make_error(ErrorCode::Corrupt, "bloque cifrado: cabecera incompleta");
    }
    if (std::to_integer<std::uint8_t>(frame[kBlkVersionPos]) != kBlockVersion) {
        return make_error(ErrorCode::Corrupt, "bloque cifrado: versión de formato desconocida");
    }
    return EncryptedBlockView{
        .base_offset = load_le<std::int64_t>(frame.subspan(kBlkBaseOffsetPos)),
        .record_count = load_le<std::int32_t>(frame.subspan(kBlkRecordCountPos)),
        .ciphertext_len = load_le<std::uint32_t>(frame.subspan(kBlkCtLenPos)),
    };
}

expected<void> SegmentCipher::open_block([[maybe_unused]] ByteSpan frame,
                                         [[maybe_unused]] Buffer& out) const {
#ifndef NEXUS_HAVE_OPENSSL
    return make_error(ErrorCode::Unsupported,
                      "cifrado no disponible: broker compilado sin OpenSSL");
#else
    const auto view = peek_block(frame);
    if (!view) {
        return std::unexpected(view.error());
    }
    if (frame.size() < view->on_disk_size()) {
        return make_error(ErrorCode::Corrupt, "bloque cifrado: ciphertext truncado");
    }
    const ByteSpan aad = frame.subspan(0, kBlkAadLen);
    const ByteSpan nonce = frame.subspan(kBlkNoncePos, kEncNonceBytes);
    const ByteSpan tag = frame.subspan(kBlkTagPos, kEncTagBytes);
    const ByteSpan ciphertext = frame.subspan(kEncBlockHeaderSize, view->ciphertext_len);

    std::vector<std::byte> plaintext(view->ciphertext_len);
    if (const auto opened = aes_gcm_decrypt(dek_, nonce, aad, ciphertext, tag, plaintext);
        !opened) {
        return opened;  // Corrupt si el tag/AAD no cuadran: no se anexa nada a `out`.
    }
    out.append(plaintext);
    return {};
#endif
}

void encode_segment_header(ByteSpan salt, MutByteSpan out) noexcept {
    std::ranges::copy(kEncSegmentMagic, out.begin());
    out[kSegVersionPos] = static_cast<std::byte>(kSegmentVersion);
    out[kSegKdfIdPos] = static_cast<std::byte>(kKdfHkdfSha256);
    out[kSegCipherIdPos] = static_cast<std::byte>(kCipherAes256Gcm);
    out[kSegFlagsPos] = std::byte{0};
    std::ranges::copy(salt, out.begin() + kSegSaltPos);
}

bool is_encrypted_segment_header(ByteSpan header) noexcept {
    if (header.size() < kEncSegmentMagic.size()) {
        return false;
    }
    return std::ranges::equal(kEncSegmentMagic, header.subspan(0, kEncSegmentMagic.size()));
}

expected<std::array<std::byte, kEncSaltBytes>> parse_segment_header(ByteSpan header) {
    if (header.size() < kEncSegmentHeaderSize) {
        return make_error(ErrorCode::Corrupt, "cabecera de segmento cifrado incompleta");
    }
    if (!is_encrypted_segment_header(header)) {
        return make_error(ErrorCode::Corrupt, "cabecera de segmento: magic no reconocido");
    }
    if (std::to_integer<std::uint8_t>(header[kSegVersionPos]) != kSegmentVersion) {
        return make_error(ErrorCode::Unsupported, "cabecera de segmento: versión desconocida");
    }
    std::array<std::byte, kEncSaltBytes> salt{};
    std::ranges::copy(header.subspan(kSegSaltPos, kEncSaltBytes), salt.begin());
    return salt;
}

expected<void> random_bytes([[maybe_unused]] MutByteSpan out) {
#ifndef NEXUS_HAVE_OPENSSL
    return make_error(ErrorCode::Unsupported,
                      "cifrado no disponible: broker compilado sin OpenSSL");
#else
    if (out.empty()) {
        return {};
    }
    if (RAND_bytes(uc(out), static_cast<int>(out.size())) != 1) {
        return make_error(ErrorCode::IoError, "RAND_bytes (CSPRNG) falló");
    }
    return {};
#endif
}

}  // namespace nexus
