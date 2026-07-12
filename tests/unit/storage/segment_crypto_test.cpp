#include "storage/segment_crypto.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"

namespace {

using nexus::ByteSpan;
using nexus::EncryptionKey;
using nexus::ErrorCode;
using nexus::MutByteSpan;
using nexus::SegmentCipher;

// Una KEK de prueba (64 hex = 256 bits). NUNCA una clave real; solo para los tests.
constexpr std::string_view kTestKeyHex =
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

// Salt de segmento fija (16 bytes) para reproducibilidad de las derivaciones.
std::array<std::byte, nexus::kEncSaltBytes> test_salt(std::byte fill = std::byte{0x5A}) {
    std::array<std::byte, nexus::kEncSaltBytes> salt{};
    salt.fill(fill);
    return salt;
}

// Un plaintext de prueba (simula un RecordBatch codificado).
std::vector<std::byte> sample_plaintext(std::size_t len = 64) {
    std::vector<std::byte> data(len);
    for (std::size_t i = 0; i < len; ++i) {
        data[i] = static_cast<std::byte>(i * 7 + 1);
    }
    return data;
}

EncryptionKey make_key() {
    auto key = EncryptionKey::from_hex(kTestKeyHex);
    EXPECT_TRUE(key.has_value());
    return std::move(*key);
}

SegmentCipher make_cipher() {
    const auto salt = test_salt();
    auto cipher = SegmentCipher::create(make_key(), salt);
    EXPECT_TRUE(cipher.has_value());
    return std::move(*cipher);
}

// --- Validación de la KEK (no requiere OpenSSL para los casos de formato) ---

TEST(SegmentCrypto, FromHex_LongitudInvalida_DevuelveInvalidArgument) {
    const auto key = EncryptionKey::from_hex("00010203");  // demasiado corta
    ASSERT_FALSE(key.has_value());
    EXPECT_EQ(key.error().code(), ErrorCode::InvalidArgument);
}

TEST(SegmentCrypto, FromHex_DigitoNoHexadecimal_DevuelveInvalidArgument) {
    std::string bad(64, 'g');  // longitud correcta, dígitos no hex
    const auto key = EncryptionKey::from_hex(bad);
    ASSERT_FALSE(key.has_value());
    EXPECT_EQ(key.error().code(), ErrorCode::InvalidArgument);
}

TEST(SegmentCrypto, FromBytes_LongitudInvalida_DevuelveInvalidArgument) {
    const std::vector<std::byte> short_key(16, std::byte{0});
    const auto key = EncryptionKey::from_bytes(short_key);
    ASSERT_FALSE(key.has_value());
    EXPECT_EQ(key.error().code(), ErrorCode::InvalidArgument);
}

// --- Derivación de DEK (HKDF) ---

TEST(SegmentCrypto, DeriveSegmentDek_MismaEntrada_EsDeterminista) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto key = make_key();
    const auto salt = test_salt();
    const auto dek1 = key.derive_segment_dek(salt);
    const auto dek2 = key.derive_segment_dek(salt);
    ASSERT_TRUE(dek1.has_value());
    ASSERT_TRUE(dek2.has_value());
    EXPECT_EQ(*dek1, *dek2);
}

TEST(SegmentCrypto, DeriveSegmentDek_SaltDistinta_ProduceDekDistinta) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto key = make_key();
    const auto dek1 = key.derive_segment_dek(test_salt(std::byte{0x11}));
    const auto dek2 = key.derive_segment_dek(test_salt(std::byte{0x22}));
    ASSERT_TRUE(dek1.has_value());
    ASSERT_TRUE(dek2.has_value());
    EXPECT_NE(*dek1, *dek2);
}

// --- Round-trip de bloque ---

TEST(SegmentCrypto, SealOpenBlock_RoundTrip_RecuperaPlaintext) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    const auto plaintext = sample_plaintext();

    nexus::Buffer frame;
    ASSERT_TRUE(
        cipher.seal_block(plaintext, /*base_offset=*/42, /*record_count=*/7, frame).has_value());
    // El bloque en disco es mayor que el plaintext (cabecera + tag).
    EXPECT_EQ(frame.size(), nexus::kEncBlockHeaderSize + plaintext.size());

    nexus::Buffer recovered;
    ASSERT_TRUE(cipher.open_block(frame.as_span(), recovered).has_value());
    const std::vector<std::byte> got(recovered.as_span().begin(), recovered.as_span().end());
    EXPECT_EQ(got, plaintext);
}

TEST(SegmentCrypto, SealBlock_PlaintextVacio_RoundTrip) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    nexus::Buffer frame;
    ASSERT_TRUE(cipher.seal_block({}, /*base_offset=*/0, /*record_count=*/0, frame).has_value());
    nexus::Buffer recovered;
    ASSERT_TRUE(cipher.open_block(frame.as_span(), recovered).has_value());
    EXPECT_TRUE(recovered.as_span().empty());
}

// --- Invariante nº1: no reutilización de nonce ---

TEST(SegmentCrypto, SealBlock_MismoPlaintextDosVeces_ProduceNoncesYCiphertextDistintos) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    const auto plaintext = sample_plaintext();

    nexus::Buffer a;
    nexus::Buffer b;
    ASSERT_TRUE(cipher.seal_block(plaintext, 0, 1, a).has_value());
    ASSERT_TRUE(cipher.seal_block(plaintext, 0, 1, b).has_value());
    // Dos cifrados del MISMO plaintext deben diferir por completo (nonce fresco cada vez).
    const std::vector<std::byte> fa(a.as_span().begin(), a.as_span().end());
    const std::vector<std::byte> fb(b.as_span().begin(), b.as_span().end());
    EXPECT_NE(fa, fb);
}

TEST(SegmentCrypto, SealBlock_MuchosBloques_TodosLosNoncesDistintos) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    std::vector<std::array<std::byte, nexus::kEncNonceBytes>> nonces;
    constexpr int kBlocks = 512;
    for (int i = 0; i < kBlocks; ++i) {
        nexus::Buffer frame;
        ASSERT_TRUE(cipher.seal_block(sample_plaintext(8), i, 1, frame).has_value());
        std::array<std::byte, nexus::kEncNonceBytes> nonce{};
        std::copy_n(frame.as_span().begin() + 18, nexus::kEncNonceBytes, nonce.begin());
        nonces.push_back(nonce);
    }
    std::sort(nonces.begin(), nonces.end());
    EXPECT_EQ(std::unique(nonces.begin(), nonces.end()), nonces.end()) << "nonce repetido";
}

// --- Detección de manipulación (AEAD) ---

TEST(SegmentCrypto, OpenBlock_CiphertextAlterado_DevuelveCorrupt) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    nexus::Buffer frame;
    ASSERT_TRUE(cipher.seal_block(sample_plaintext(), 42, 7, frame).has_value());
    std::vector<std::byte> tampered(frame.as_span().begin(), frame.as_span().end());
    tampered[nexus::kEncBlockHeaderSize + 3] ^= std::byte{0xFF};  // un byte del ciphertext

    nexus::Buffer out;
    const auto opened = cipher.open_block(tampered, out);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error().code(), ErrorCode::Corrupt);
}

TEST(SegmentCrypto, OpenBlock_TagAlterado_DevuelveCorrupt) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    nexus::Buffer frame;
    ASSERT_TRUE(cipher.seal_block(sample_plaintext(), 42, 7, frame).has_value());
    std::vector<std::byte> tampered(frame.as_span().begin(), frame.as_span().end());
    tampered[30] ^= std::byte{0x01};  // primer byte del tag

    nexus::Buffer out;
    const auto opened = cipher.open_block(tampered, out);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error().code(), ErrorCode::Corrupt);
}

TEST(SegmentCrypto, OpenBlock_MetadatoEnClaroAlterado_DevuelveCorrupt) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    nexus::Buffer frame;
    ASSERT_TRUE(cipher.seal_block(sample_plaintext(), 42, 7, frame).has_value());
    std::vector<std::byte> tampered(frame.as_span().begin(), frame.as_span().end());
    tampered[2] ^= std::byte{0xFF};  // altera base_offset (dentro de la AAD)

    nexus::Buffer out;
    const auto opened = cipher.open_block(tampered, out);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error().code(), ErrorCode::Corrupt);
}

TEST(SegmentCrypto, OpenBlock_ConDekDistinta_DevuelveCorrupt) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    nexus::Buffer frame;
    ASSERT_TRUE(cipher.seal_block(sample_plaintext(), 42, 7, frame).has_value());

    // Otro segmento (salt distinta) → DEK distinta → no debe poder descifrar.
    auto other = SegmentCipher::create(make_key(), test_salt(std::byte{0x99}));
    ASSERT_TRUE(other.has_value());
    nexus::Buffer out;
    const auto opened = other->open_block(frame.as_span(), out);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error().code(), ErrorCode::Corrupt);
}

// --- Traversal sin descifrar ---

TEST(SegmentCrypto, PeekBlock_DevuelveMetadatosDeTraversal) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    const auto plaintext = sample_plaintext(100);
    nexus::Buffer frame;
    ASSERT_TRUE(
        cipher.seal_block(plaintext, /*base_offset=*/1000, /*record_count=*/5, frame).has_value());
    const auto view = SegmentCipher::peek_block(frame.as_span());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->base_offset, 1000);
    EXPECT_EQ(view->record_count, 5);
    EXPECT_EQ(view->ciphertext_len, plaintext.size());
    EXPECT_EQ(view->on_disk_size(), nexus::kEncBlockHeaderSize + plaintext.size());
    EXPECT_EQ(view->last_offset(), 1004);
}

TEST(SegmentCrypto, PeekBlock_CabeceraTruncada_DevuelveCorrupt) {
    const std::vector<std::byte> tiny(4, std::byte{0});
    const auto view = SegmentCipher::peek_block(tiny);
    ASSERT_FALSE(view.has_value());
    EXPECT_EQ(view.error().code(), ErrorCode::Corrupt);
}

// --- Cabecera de segmento ---

TEST(SegmentCrypto, SegmentHeader_RoundTrip_RecuperaSalt) {
    const auto salt = test_salt(std::byte{0x3C});
    std::array<std::byte, nexus::kEncSegmentHeaderSize> header{};
    nexus::encode_segment_header(salt, header);
    EXPECT_TRUE(nexus::is_encrypted_segment_header(header));

    const auto parsed = nexus::parse_segment_header(header);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(*parsed, salt);
}

TEST(SegmentCrypto, ParseSegmentHeader_MagicInvalido_DevuelveCorrupt) {
    std::array<std::byte, nexus::kEncSegmentHeaderSize> header{};  // todo ceros: sin magic
    EXPECT_FALSE(nexus::is_encrypted_segment_header(header));
    const auto parsed = nexus::parse_segment_header(header);
    ASSERT_FALSE(parsed.has_value());
    EXPECT_EQ(parsed.error().code(), ErrorCode::Corrupt);
}

TEST(SegmentCrypto, IsEncryptedSegmentHeader_PlaintextBatch_EsFalso) {
    // Un batch en claro empieza por base_offset (i64) pequeño: no coincide con el magic.
    std::array<std::byte, nexus::kEncSegmentHeaderSize> plain{};
    plain[0] = std::byte{0x00};
    EXPECT_FALSE(nexus::is_encrypted_segment_header(plain));
}

// --- CSPRNG ---

TEST(SegmentCrypto, RandomBytes_RellenaYVariaEntreLlamadas) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    std::array<std::byte, 32> a{};
    std::array<std::byte, 32> b{};
    ASSERT_TRUE(nexus::random_bytes(a).has_value());
    ASSERT_TRUE(nexus::random_bytes(b).has_value());
    EXPECT_NE(a, b);  // colisión de 256 bits: prácticamente imposible
}

// --- Property-based (RNG sembrado, determinista) ---

// Para muchos plaintexts aleatorios de tamaño y metadatos variables, seal->open recupera el
// plaintext exacto y peek expone los metadatos de traversal correctos.
TEST(SegmentCryptoProperty, SealOpen_PlaintextsAleatorios_RoundTrip) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    std::mt19937_64 rng(0xC0FFEEULL);
    for (int iter = 0; iter < 300; ++iter) {
        const std::size_t len = rng() % 2048;
        std::vector<std::byte> plaintext(len);
        for (auto& byte : plaintext) {
            byte = static_cast<std::byte>(rng() & 0xFFU);
        }
        const auto base_offset = static_cast<nexus::Offset>(rng() % 1'000'000'000);
        const auto record_count = static_cast<std::int32_t>(rng() % 10'000);

        nexus::Buffer frame;
        ASSERT_TRUE(cipher.seal_block(plaintext, base_offset, record_count, frame).has_value());

        const auto view = SegmentCipher::peek_block(frame.as_span());
        ASSERT_TRUE(view.has_value());
        EXPECT_EQ(view->base_offset, base_offset);
        EXPECT_EQ(view->record_count, record_count);
        EXPECT_EQ(view->ciphertext_len, len);

        nexus::Buffer recovered;
        ASSERT_TRUE(cipher.open_block(frame.as_span(), recovered).has_value());
        const std::vector<std::byte> got(recovered.as_span().begin(), recovered.as_span().end());
        EXPECT_EQ(got, plaintext);
    }
}

// Invariante de integridad: alterar CUALQUIER byte del bloque (cabecera, nonce, tag o ciphertext)
// hace fallar open (nunca devuelve un plaintext distinto en silencio).
TEST(SegmentCryptoProperty, TamperSweep_CualquierByteAlterado_Falla) {
    if (!nexus::encryption_available()) {
        GTEST_SKIP() << "sin OpenSSL";
    }
    const auto cipher = make_cipher();
    const auto plaintext = sample_plaintext(24);
    nexus::Buffer frame;
    ASSERT_TRUE(cipher.seal_block(plaintext, 123, 4, frame).has_value());
    const std::vector<std::byte> original(frame.as_span().begin(), frame.as_span().end());

    for (std::size_t pos = 0; pos < original.size(); ++pos) {
        std::vector<std::byte> tampered = original;
        tampered[pos] ^= std::byte{0xFF};
        nexus::Buffer out;
        const auto opened = cipher.open_block(tampered, out);
        EXPECT_FALSE(opened.has_value()) << "byte " << pos << " alterado y open no falló";
    }
}

}  // namespace
