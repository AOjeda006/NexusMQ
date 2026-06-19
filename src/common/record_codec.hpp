/// @file   common/record_codec.hpp
/// @brief  Codec por record: Record (key/value/headers) y (de)serialización varint/zigzag.
/// @ingroup common

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Cabecera de aplicación de un record (par clave→valor). Afinidad: INMUTABLE.
/// @details La clave es una cadena no nula; el valor son bytes anulables (`nullopt`).
struct RecordHeader {
    std::string key;                              ///< Clave (UTF-8, no nula).
    std::optional<std::vector<std::byte>> value;  ///< Valor; `nullopt` = nulo.
    bool operator==(const RecordHeader&) const = default;
};

/// @brief Un record individual dentro de un RecordBatch. Afinidad: INMUTABLE.
/// @details `key`/`value` son **anulables** (`nullopt`). Un record con `value == nullopt` es un
///   **tombstone**: marca de borrado por clave para la compactación (F3). El `offset` es absoluto
///   al **decodificar** (`base_offset + offset_delta`); al **codificar** lo ignora el builder, que
///   asigna el delta por orden de inserción.
struct Record {
    std::optional<std::vector<std::byte>> key;    ///< Clave; `nullopt` = sin clave.
    std::optional<std::vector<std::byte>> value;  ///< Valor; `nullopt` = tombstone.
    std::vector<RecordHeader> headers;            ///< Headers de aplicación (puede ir vacío).
    std::int64_t timestamp_delta = 0;             ///< Delta respecto al timestamp base (reservado).
    Offset offset = 0;                            ///< Offset absoluto (relleno al decodificar).
    bool operator==(const Record&) const = default;
};

/// Topes anti-DoS al decodificar (entrada no confiable): nº de records/headers.
inline constexpr std::int64_t kMaxRecordsPerBatch = 1'000'000;
inline constexpr std::int64_t kMaxHeadersPerRecord = 10'000;
/// Tope del blob de records **descomprimido** (anti *decompression bomb*): 64 MiB.
inline constexpr std::size_t kMaxRecordBytes = std::size_t{64} * 1024 * 1024;

/// @brief Codifica @p rec (con su prefijo de longitud) al final de @p out.
/// @param offset_delta Delta del offset respecto al base del batch (0 para el primero).
/// @details Layout por record (estilo Kafka v2): `length:varint(zigzag)` del cuerpo, luego
///   `attributes:i8 | timestampDelta:varint | offsetDelta:varint | keyLen:varint(-1=nulo) | key |
///   valueLen:varint(-1=nulo) | value | headerCount:varint | headers`.
void encode_record(const Record& rec, std::int64_t offset_delta, Buffer& out);

/// @brief Decodifica un record desde @p cursor (empieza en el prefijo de longitud) y **avanza** el
///   cursor tras él. @p base_offset reconstruye `Record::offset` absoluto.
/// @return El record, o `Corrupt` si está truncado o malformado (decodificador defensivo).
[[nodiscard]] expected<Record> decode_record(ByteSpan& cursor, Offset base_offset);

/// @brief Decodifica todos los records del blob de @p batch (con offsets absolutos).
/// @details Si los bits de códec de `attrs` indican compresión (F5), el blob se **descomprime**
///   primero, acotando la salida a @p max_decompressed (anti *decompression bomb*).
[[nodiscard]] expected<std::vector<Record>> decode_records(
    const RecordBatch& batch, std::size_t max_decompressed = kMaxRecordBytes);

/// @brief Acumula records y produce un RecordBatch (asigna offset_delta por orden de inserción).
///   Afinidad: REACTOR-LOCAL.
class RecordBatchBuilder {
public:
    /// Añade @p rec al final (su delta será su posición en el batch).
    RecordBatchBuilder& add(Record rec);

    [[nodiscard]] bool empty() const noexcept { return records_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return records_.size(); }

    /// @brief Construye el batch. Sobrescribe `header.record_count` con el nº de records; el
    ///   `base_offset` lo asigna el log al anexar (queda como venga en @p header, normalmente 0).
    ///   Los campos de idempotencia de @p header se respetan.
    /// @param codec Si no es `None`, comprime el blob de records y fija los bits de códec de
    /// `attrs`
    ///   (F5). Si el códec no está compilado, el batch se construye **sin** comprimir (`None`).
    [[nodiscard]] RecordBatch build(RecordBatchHeader header = {}, Codec codec = Codec::None) const;

private:
    std::vector<Record> records_;
};

}  // namespace nexus
