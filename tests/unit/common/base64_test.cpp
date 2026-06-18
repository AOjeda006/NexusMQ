// base64url (RFC 4648 §5, sin relleno): vectores de ida y vuelta, alfabeto URL-safe y rechazos.
#include "common/base64.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace {

nexus::ByteSpan bytes_of(std::string_view text) {
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

std::string decode_to_string(std::string_view encoded) {
    const auto decoded = nexus::base64url_decode(encoded);
    EXPECT_TRUE(decoded.has_value());
    return {reinterpret_cast<const char*>(decoded->data()), decoded->size()};
}

TEST(Base64Url, VectoresRfc4648_Encode) {
    // RFC 4648 §10 (alfabeto estándar; idéntico salvo +/ → -_, que no aparecen aquí).
    EXPECT_EQ(nexus::base64url_encode(bytes_of("")), "");
    EXPECT_EQ(nexus::base64url_encode(bytes_of("f")), "Zg");
    EXPECT_EQ(nexus::base64url_encode(bytes_of("fo")), "Zm8");
    EXPECT_EQ(nexus::base64url_encode(bytes_of("foo")), "Zm9v");
    EXPECT_EQ(nexus::base64url_encode(bytes_of("foob")), "Zm9vYg");
    EXPECT_EQ(nexus::base64url_encode(bytes_of("fooba")), "Zm9vYmE");
    EXPECT_EQ(nexus::base64url_encode(bytes_of("foobar")), "Zm9vYmFy");
}

TEST(Base64Url, AlfabetoUrlSafe_MenosMasYBarra) {
    // Bytes que en base64 estándar darían '+' y '/'; en URL-safe deben ser '-' y '_'.
    const std::array<std::byte, 2> data = {std::byte{0xfb}, std::byte{0xff}};
    EXPECT_EQ(nexus::base64url_encode(data), "-_8");

    const std::array<std::byte, 3> all_ones = {std::byte{0xff}, std::byte{0xff}, std::byte{0xff}};
    EXPECT_EQ(nexus::base64url_encode(all_ones), "____");
}

TEST(Base64Url, RoundTrip) {
    EXPECT_EQ(decode_to_string("Zg"), "f");
    EXPECT_EQ(decode_to_string("Zm9v"), "foo");
    EXPECT_EQ(decode_to_string("Zm9vYmE"), "fooba");
    EXPECT_EQ(decode_to_string("Zm9vYmFy"), "foobar");
}

TEST(Base64Url, ToleraRellenoOpcional) {
    // Aunque base64url no emita '=', el decodificador lo acepta (interoperabilidad).
    EXPECT_EQ(decode_to_string("Zm8="), "fo");
    EXPECT_EQ(decode_to_string("Zg=="), "f");
}

TEST(Base64Url, RechazaCaracterInvalido) {
    const auto result = nexus::base64url_decode("Zm9v*");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::InvalidArgument);
    // El '+' es del alfabeto estándar, no del URL-safe: debe rechazarse.
    EXPECT_FALSE(nexus::base64url_decode("ab+c").has_value());
}

TEST(Base64Url, RechazaLongitudImposible) {
    EXPECT_FALSE(nexus::base64url_decode("Z").has_value());      // len % 4 == 1.
    EXPECT_FALSE(nexus::base64url_decode("Zm9vZ").has_value());  // 5 → 1 mod 4.
}

}  // namespace
