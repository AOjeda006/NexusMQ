/// @file   base64.cpp
/// @brief  Implementación del codec base64url sin relleno (RFC 4648 §5).
/// @ingroup common

#include "common/base64.hpp"

#include <array>
#include <cstdint>

namespace nexus {

namespace {

/// Alfabeto URL-safe: A-Z a-z 0-9 '-' '_' (índices 0..63).
constexpr std::string_view kAlphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Tabla inversa carácter→valor (0..63); -1 marca «no pertenece al alfabeto».
constexpr std::array<std::int8_t, 256> make_decode_table() {
    std::array<std::int8_t, 256> table{};
    table.fill(-1);
    for (std::size_t i = 0; i < kAlphabet.size(); ++i) {
        table[static_cast<unsigned char>(kAlphabet[i])] = static_cast<std::int8_t>(i);
    }
    return table;
}

constexpr std::array<std::int8_t, 256> kDecodeTable = make_decode_table();

}  // namespace

std::string base64url_encode(ByteSpan data) {
    std::string out;
    out.reserve(((data.size() + 2) / 3) * 4);

    std::uint32_t buffer = 0;
    int bits = 0;
    for (const std::byte byte : data) {
        buffer = (buffer << 8) | std::to_integer<std::uint32_t>(byte);
        bits += 8;
        while (bits >= 6) {
            bits -= 6;
            out.push_back(kAlphabet[(buffer >> bits) & 0x3FU]);
        }
    }
    if (bits > 0) {  // bits residuales, justificados a la izquierda con ceros.
        out.push_back(kAlphabet[(buffer << (6 - bits)) & 0x3FU]);
    }
    return out;
}

expected<std::vector<std::byte>> base64url_decode(std::string_view text) {
    // Tolera el relleno '=' final, aunque base64url no lo emita.
    while (!text.empty() && text.back() == '=') {
        text.remove_suffix(1);
    }
    if (text.size() % 4 == 1) {
        return make_error(ErrorCode::InvalidArgument, "base64url: longitud inválida");
    }

    std::vector<std::byte> out;
    out.reserve((text.size() / 4) * 3);

    std::uint32_t buffer = 0;
    int bits = 0;
    for (const char character : text) {
        const std::int8_t value = kDecodeTable[static_cast<unsigned char>(character)];
        if (value < 0) {
            return make_error(ErrorCode::InvalidArgument, "base64url: carácter inválido");
        }
        buffer = (buffer << 6) | static_cast<std::uint32_t>(value);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<std::byte>((buffer >> bits) & 0xFFU));
        }
    }
    return out;
}

}  // namespace nexus
