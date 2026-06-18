// SHA-256 (FIPS 180-4) y HMAC-SHA256 (RFC 2104): known-answer tests con vectores de FIPS 180-4 y
// RFC 4231, más comprobación de que el modo incremental coincide con el de una sola llamada.
#include "common/sha256.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

// Convierte una cadena ASCII en una vista de bytes (sin el terminador nulo).
nexus::ByteSpan bytes_of(std::string_view text) {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

TEST(Sha256, VectorVacio_FIPS) {
    EXPECT_EQ(nexus::to_hex(nexus::sha256(bytes_of(""))),
              "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST(Sha256, VectorAbc_FIPS) {
    EXPECT_EQ(nexus::to_hex(nexus::sha256(bytes_of("abc"))),
              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(Sha256, VectorBloqueLargo_FIPS) {
    // 448 bits: fuerza dos bloques de compresión (mensaje + padding desbordan 64 bytes).
    const std::string_view msg = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    EXPECT_EQ(nexus::to_hex(nexus::sha256(bytes_of(msg))),
              "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
}

TEST(Sha256, MensajeMultibloque_MillonDeA) {
    // FIPS 180-4: un millón de 'a' → varios bloques completos vía update repetido.
    nexus::Sha256 hasher;
    const std::array<std::byte, 1000> chunk = [] {
        std::array<std::byte, 1000> buf{};
        buf.fill(std::byte{'a'});
        return buf;
    }();
    for (int i = 0; i < 1000; ++i) {
        hasher.update(chunk);
    }
    EXPECT_EQ(nexus::to_hex(hasher.finish()),
              "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

TEST(Sha256, Incremental_CoincideConUnaLlamada) {
    const std::string_view msg = "The quick brown fox jumps over the lazy dog";
    const nexus::Sha256Digest one_shot = nexus::sha256(bytes_of(msg));

    nexus::Sha256 hasher;
    for (const char character : msg) {  // byte a byte: ejercita el buffer parcial.
        const auto value = static_cast<std::byte>(character);
        hasher.update(std::span<const std::byte, 1>{&value, 1});
    }
    EXPECT_EQ(hasher.finish(), one_shot);
}

TEST(HmacSha256, Rfc4231_Caso1) {
    const std::array<std::byte, 20> key = [] {
        std::array<std::byte, 20> buf{};
        buf.fill(std::byte{0x0b});
        return buf;
    }();
    EXPECT_EQ(nexus::to_hex(nexus::hmac_sha256(key, bytes_of("Hi There"))),
              "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
}

TEST(HmacSha256, Rfc4231_Caso2_ClaveCorta) {
    EXPECT_EQ(nexus::to_hex(
                  nexus::hmac_sha256(bytes_of("Jefe"), bytes_of("what do ya want for nothing?"))),
              "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
}

TEST(HmacSha256, Rfc4231_Caso6_ClaveMasLargaQueBloque) {
    // Clave de 131 bytes (>64): RFC 2104 manda hashearla primero.
    const std::vector<std::byte> key(131, std::byte{0xaa});
    const std::string_view data = "Test Using Larger Than Block-Size Key - Hash Key First";
    EXPECT_EQ(nexus::to_hex(nexus::hmac_sha256(key, bytes_of(data))),
              "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54");
}

}  // namespace
