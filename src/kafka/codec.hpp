/// @file   kafka/codec.hpp
/// @brief  Codec del protocolo de Apache Kafka (big-endian) para el subconjunto compatible (F7).
/// @ingroup kafka

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "common/bytes.hpp"
#include "common/error.hpp"

/// @brief Subconjunto **compatible con Apache Kafka** (F7): codec de wire y mensajes de las APIs
///   `ApiVersions`/`Metadata`/`Produce`/`Fetch`, para hablar con clientes como `kcat`/librdkafka.
/// @details El protocolo de Kafka es **big-endian** (a diferencia del nativo de NexusMQ, que es
///   little-endian, ADR-0013), de ahí un codec propio. Distingue tipos *clásicos* (longitud con
///   `INT16`/`INT32`) de los *compactos* de las **versiones flexibles** (longitud en
///   `UNSIGNED_VARINT` y *tagged fields*).
namespace nexus::kafka {

/// @brief Escritor secuencial del protocolo Kafka (big-endian). Afinidad: REACTOR-LOCAL.
/// @details Cada `put_*` añade al final del `Buffer`; no falla (el búfer crece).
class Encoder {
public:
    explicit Encoder(Buffer& out) noexcept : out_(out) {}

    void put_i8(std::int8_t value);
    void put_i16(std::int16_t value);
    void put_i32(std::int32_t value);
    void put_i64(std::int64_t value);
    void put_u32(std::uint32_t value);
    void put_bool(bool value);

    /// `UNSIGNED_VARINT` (LEB128 sin signo): longitudes compactas y *tagged fields*.
    void put_unsigned_varint(std::uint32_t value);
    /// `VARINT` (zigzag + LEB128, 32 bits).
    void put_varint(std::int32_t value);
    /// `VARLONG` (zigzag + LEB128, 64 bits).
    void put_varlong(std::int64_t value);

    /// `STRING`: `INT16` longitud + UTF-8 (no admite nulo).
    void put_string(std::string_view text);
    /// `NULLABLE_STRING`: `INT16` longitud (-1 = nulo) + UTF-8.
    void put_nullable_string(const std::optional<std::string_view>& text);
    /// `COMPACT_STRING`: `UNSIGNED_VARINT`(len+1) + UTF-8.
    void put_compact_string(std::string_view text);
    /// `COMPACT_NULLABLE_STRING`: `UNSIGNED_VARINT`(len+1; 0 = nulo) + UTF-8.
    void put_compact_nullable_string(const std::optional<std::string_view>& text);

    /// `BYTES`: `INT32` longitud + contenido.
    void put_bytes(ByteSpan data);
    /// `NULLABLE_BYTES`: `INT32` longitud (-1 = nulo) + contenido.
    void put_nullable_bytes(const std::optional<ByteSpan>& data);
    /// `COMPACT_BYTES`: `UNSIGNED_VARINT`(len+1) + contenido.
    void put_compact_bytes(ByteSpan data);

    /// Longitud de un `ARRAY` clásico (`INT32`; -1 = nulo). Los elementos los escribe el llamante.
    void put_array_len(std::int32_t count);
    /// Longitud de un `COMPACT_ARRAY` (`UNSIGNED_VARINT`(count+1); 0 = nulo).
    void put_compact_array_len(std::int32_t count);

    /// Sección de *tagged fields* **vacía** (un `UNSIGNED_VARINT` 0) de las versiones flexibles.
    void put_empty_tagged_fields();

    /// Bytes ya serializados (p. ej. un RecordBatch construido aparte).
    void put_raw(ByteSpan data);

private:
    Buffer& out_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

/// @brief Lector secuencial del protocolo Kafka (big-endian). Afinidad: REACTOR-LOCAL.
/// @details **Decodificador defensivo** (entrada no confiable): cada `get_*` valida los límites y
///   nunca lee fuera de la vista; devuelve `InvalidArgument`/`Corrupt` si la entrada es inválida.
class Decoder {
public:
    explicit Decoder(ByteSpan in) noexcept : in_(in) {}

    [[nodiscard]] expected<std::int8_t> get_i8();
    [[nodiscard]] expected<std::int16_t> get_i16();
    [[nodiscard]] expected<std::int32_t> get_i32();
    [[nodiscard]] expected<std::int64_t> get_i64();
    [[nodiscard]] expected<std::uint32_t> get_u32();
    [[nodiscard]] expected<bool> get_bool();

    [[nodiscard]] expected<std::uint32_t> get_unsigned_varint();
    [[nodiscard]] expected<std::int32_t> get_varint();
    [[nodiscard]] expected<std::int64_t> get_varlong();

    [[nodiscard]] expected<std::string> get_string();
    [[nodiscard]] expected<std::optional<std::string>> get_nullable_string();
    [[nodiscard]] expected<std::string> get_compact_string();
    [[nodiscard]] expected<std::optional<std::string>> get_compact_nullable_string();

    /// Bytes con prefijo `INT32`; la vista apunta dentro de la entrada (zero-copy).
    [[nodiscard]] expected<ByteSpan> get_bytes();
    [[nodiscard]] expected<std::optional<ByteSpan>> get_nullable_bytes();
    [[nodiscard]] expected<ByteSpan> get_compact_bytes();

    /// Longitud de un `ARRAY` clásico (`INT32`); -1 se devuelve como tal (nulo).
    [[nodiscard]] expected<std::int32_t> get_array_len();
    /// Longitud de un `COMPACT_ARRAY` (`UNSIGNED_VARINT`-1); -1 = nulo.
    [[nodiscard]] expected<std::int32_t> get_compact_array_len();

    /// Salta la sección de *tagged fields* (cuenta + cada tag/longitud/datos) sin interpretarla.
    [[nodiscard]] expected<void> skip_tagged_fields();

    [[nodiscard]] std::size_t remaining() const noexcept { return in_.size() - pos_; }
    [[nodiscard]] bool empty() const noexcept { return pos_ >= in_.size(); }

private:
    [[nodiscard]] expected<ByteSpan> take(std::size_t n);

    ByteSpan in_;
    std::size_t pos_ = 0;
};

}  // namespace nexus::kafka
