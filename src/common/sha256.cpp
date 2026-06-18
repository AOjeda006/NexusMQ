/// @file   sha256.cpp
/// @brief  Implementación de SHA-256 (FIPS 180-4) y HMAC-SHA256 (RFC 2104).
/// @ingroup common

#include "common/sha256.hpp"

#include <algorithm>
#include <string_view>

namespace nexus {

namespace {

/// Constantes de ronda K[0..63] (FIPS 180-4, §4.2.2): parte fraccionaria de las raíces cúbicas de
/// los 64 primeros primos.
constexpr std::array<std::uint32_t, 64> kRoundConstants = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

/// Valores iniciales H0..H7 (FIPS 180-4, §5.3.3): raíces cuadradas de los 8 primeros primos.
constexpr std::array<std::uint32_t, 8> kInitialState = {
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};

/// Rotación circular a la derecha de 32 bits.
constexpr std::uint32_t rotr(std::uint32_t x, unsigned n) noexcept {
    return (x >> n) | (x << (32U - n));
}

constexpr std::uint32_t ch(std::uint32_t x, std::uint32_t y, std::uint32_t z) noexcept {
    return (x & y) ^ (~x & z);
}

constexpr std::uint32_t maj(std::uint32_t x, std::uint32_t y, std::uint32_t z) noexcept {
    return (x & y) ^ (x & z) ^ (y & z);
}

constexpr std::uint32_t big_sigma0(std::uint32_t x) noexcept {
    return rotr(x, 2) ^ rotr(x, 13) ^ rotr(x, 22);
}

constexpr std::uint32_t big_sigma1(std::uint32_t x) noexcept {
    return rotr(x, 6) ^ rotr(x, 11) ^ rotr(x, 25);
}

constexpr std::uint32_t small_sigma0(std::uint32_t x) noexcept {
    return rotr(x, 7) ^ rotr(x, 18) ^ (x >> 3);
}

constexpr std::uint32_t small_sigma1(std::uint32_t x) noexcept {
    return rotr(x, 17) ^ rotr(x, 19) ^ (x >> 10);
}

/// Lee 4 bytes big-endian desde @p p en un word de 32 bits.
std::uint32_t read_be32(const std::byte* p) noexcept {
    return (std::to_integer<std::uint32_t>(p[0]) << 24) |
           (std::to_integer<std::uint32_t>(p[1]) << 16) |
           (std::to_integer<std::uint32_t>(p[2]) << 8) | std::to_integer<std::uint32_t>(p[3]);
}

}  // namespace

Sha256::Sha256() noexcept {
    reset();
}

void Sha256::reset() noexcept {
    state_ = kInitialState;
    buffer_.fill(std::byte{0});
    buffer_len_ = 0;
    total_len_ = 0;
}

void Sha256::process_block(const std::byte* block) noexcept {
    std::array<std::uint32_t, 64> w{};
    for (std::size_t t = 0; t < 16; ++t) {
        w[t] = read_be32(block + (t * 4));
    }
    for (std::size_t t = 16; t < 64; ++t) {
        w[t] = small_sigma1(w[t - 2]) + w[t - 7] + small_sigma0(w[t - 15]) + w[t - 16];
    }

    std::uint32_t a = state_[0];
    std::uint32_t b = state_[1];
    std::uint32_t c = state_[2];
    std::uint32_t d = state_[3];
    std::uint32_t e = state_[4];
    std::uint32_t f = state_[5];
    std::uint32_t g = state_[6];
    std::uint32_t h = state_[7];

    for (std::size_t t = 0; t < 64; ++t) {
        const std::uint32_t t1 = h + big_sigma1(e) + ch(e, f, g) + kRoundConstants[t] + w[t];
        const std::uint32_t t2 = big_sigma0(a) + maj(a, b, c);
        h = g;
        g = f;
        f = e;
        e = d + t1;
        d = c;
        c = b;
        b = a;
        a = t1 + t2;
    }

    state_[0] += a;
    state_[1] += b;
    state_[2] += c;
    state_[3] += d;
    state_[4] += e;
    state_[5] += f;
    state_[6] += g;
    state_[7] += h;
}

void Sha256::update(ByteSpan data) noexcept {
    total_len_ += data.size();
    std::size_t offset = 0;

    // Completa primero el bloque parcial pendiente, si lo hay.
    if (buffer_len_ > 0) {
        while (buffer_len_ < kBlockSize && offset < data.size()) {
            buffer_[buffer_len_++] = data[offset++];
        }
        if (buffer_len_ == kBlockSize) {
            process_block(buffer_.data());
            buffer_len_ = 0;
        }
    }

    // Procesa bloques completos directamente desde la entrada (sin copiar).
    while (data.size() - offset >= kBlockSize) {
        process_block(data.data() + offset);
        offset += kBlockSize;
    }

    // Guarda el resto como bloque parcial.
    while (offset < data.size()) {
        buffer_[buffer_len_++] = data[offset++];
    }
}

Sha256Digest Sha256::finish() noexcept {
    const std::uint64_t bit_len = total_len_ * 8U;

    // Padding: byte 0x80, luego ceros hasta dejar 8 bytes para la longitud.
    buffer_[buffer_len_++] = std::byte{0x80};
    if (buffer_len_ > kBlockSize - 8) {
        while (buffer_len_ < kBlockSize) {
            buffer_[buffer_len_++] = std::byte{0};
        }
        process_block(buffer_.data());
        buffer_len_ = 0;
    }
    while (buffer_len_ < kBlockSize - 8) {
        buffer_[buffer_len_++] = std::byte{0};
    }

    // Longitud del mensaje en bits, big-endian (64 bits).
    for (std::size_t i = 0; i < 8; ++i) {
        buffer_[buffer_len_++] = static_cast<std::byte>((bit_len >> (56U - (i * 8U))) & 0xFFU);
    }
    process_block(buffer_.data());

    // Serializa el estado a digest big-endian.
    Sha256Digest digest{};
    for (std::size_t i = 0; i < 8; ++i) {
        digest[(i * 4) + 0] = static_cast<std::byte>((state_[i] >> 24) & 0xFFU);
        digest[(i * 4) + 1] = static_cast<std::byte>((state_[i] >> 16) & 0xFFU);
        digest[(i * 4) + 2] = static_cast<std::byte>((state_[i] >> 8) & 0xFFU);
        digest[(i * 4) + 3] = static_cast<std::byte>(state_[i] & 0xFFU);
    }
    return digest;
}

Sha256Digest sha256(ByteSpan data) noexcept {
    Sha256 hasher;
    hasher.update(data);
    return hasher.finish();
}

Sha256Digest hmac_sha256(ByteSpan key, ByteSpan message) noexcept {
    // K0: la clave normalizada a un bloque (hash si excede, ceros a la derecha si falta).
    std::array<std::byte, Sha256::kBlockSize> block_key{};
    if (key.size() > Sha256::kBlockSize) {
        const Sha256Digest hashed = sha256(key);
        std::ranges::copy(hashed, block_key.begin());
    } else {
        std::ranges::copy(key, block_key.begin());
    }

    std::array<std::byte, Sha256::kBlockSize> inner_pad{};
    std::array<std::byte, Sha256::kBlockSize> outer_pad{};
    for (std::size_t i = 0; i < Sha256::kBlockSize; ++i) {
        inner_pad[i] = block_key[i] ^ std::byte{0x36};
        outer_pad[i] = block_key[i] ^ std::byte{0x5c};
    }

    Sha256 inner;
    inner.update(inner_pad);
    inner.update(message);
    const Sha256Digest inner_digest = inner.finish();

    Sha256 outer;
    outer.update(outer_pad);
    outer.update(inner_digest);
    return outer.finish();
}

std::string to_hex(ByteSpan data) {
    constexpr std::string_view kHexDigits = "0123456789abcdef";
    std::string out;
    out.reserve(data.size() * 2);
    for (const std::byte byte : data) {
        const auto value = std::to_integer<unsigned>(byte);
        out.push_back(kHexDigits[value >> 4]);
        out.push_back(kHexDigits[value & 0x0FU]);
    }
    return out;
}

}  // namespace nexus
