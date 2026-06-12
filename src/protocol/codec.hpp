/// @file   protocol/codec.hpp
/// @brief  Encoder/Decoder: (de)serialización del protocolo binario con chequeo de límites.
/// @ingroup protocol

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "common/bytes.hpp"
#include "common/error.hpp"

namespace nexus {

/// @brief Escritor secuencial sobre un `Buffer` (formato de wire, little-endian). Afinidad:
///   REACTOR-LOCAL. Cada `put_*` añade al final; no falla (el `Buffer` crece).
class Encoder {
public:
    explicit Encoder(Buffer& out) noexcept : out_(out) {}

    void put_u8(std::uint8_t value);
    void put_u16(std::uint16_t value);
    void put_u32(std::uint32_t value);
    void put_i16(std::int16_t value);
    void put_i32(std::int32_t value);
    void put_i64(std::int64_t value);
    void put_varint(std::uint64_t value);
    /// Bytes con prefijo de longitud (varint) + contenido.
    void put_bytes(ByteSpan data);
    /// Cadena con prefijo de longitud (varint) + UTF-8.
    void put_string(std::string_view text);

private:
    // Vista no propietaria ligada a la vida del Buffer (Encoder no es asignable, a propósito).
    Buffer& out_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

/// @brief Lector secuencial sobre una vista de bytes. Afinidad: REACTOR-LOCAL.
/// @details **Decodificador defensivo** (entrada no confiable): cada `get_*` valida que haya
///   bytes suficientes y nunca lee fuera de la vista; devuelve `InvalidArgument` si no.
class Decoder {
public:
    explicit Decoder(ByteSpan in) noexcept : in_(in) {}

    [[nodiscard]] expected<std::uint8_t> get_u8();
    [[nodiscard]] expected<std::uint16_t> get_u16();
    [[nodiscard]] expected<std::uint32_t> get_u32();
    [[nodiscard]] expected<std::int16_t> get_i16();
    [[nodiscard]] expected<std::int32_t> get_i32();
    [[nodiscard]] expected<std::int64_t> get_i64();
    [[nodiscard]] expected<std::uint64_t> get_varint();
    /// Bytes con prefijo de longitud; la vista apunta dentro de la entrada (zero-copy).
    [[nodiscard]] expected<ByteSpan> get_bytes();
    /// Cadena con prefijo de longitud; la vista apunta dentro de la entrada (zero-copy).
    [[nodiscard]] expected<std::string_view> get_string();

    /// Bytes aún sin consumir.
    [[nodiscard]] std::size_t remaining() const noexcept { return in_.size() - pos_; }
    [[nodiscard]] bool empty() const noexcept { return pos_ >= in_.size(); }

private:
    /// Avanza @p n bytes con chequeo de límites; `InvalidArgument` si no hay suficientes.
    [[nodiscard]] expected<ByteSpan> take(std::size_t n);

    ByteSpan in_;
    std::size_t pos_ = 0;
};

}  // namespace nexus
