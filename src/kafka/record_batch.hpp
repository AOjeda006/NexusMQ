/// @file   kafka/record_batch.hpp
/// @brief  Inspección de la cabecera de un RecordBatch v2 de Kafka y reasignación de su offset
/// base.
/// @ingroup kafka

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "common/bytes.hpp"
#include "common/error.hpp"

namespace nexus::kafka {

/// `magic` de un RecordBatch v2 (el único formato que produce/consume librdkafka moderno).
inline constexpr std::int8_t kRecordBatchMagicV2 = 2;

/// @brief Bytes de la **cabecera fija** de un RecordBatch v2 (antes de los records):
///   `baseOffset:i64 | batchLength:i32 | partitionLeaderEpoch:i32 | magic:i8 | crc:u32 |
///   attributes:i16 | lastOffsetDelta:i32 | baseTimestamp:i64 | maxTimestamp:i64 | producerId:i64 |
///   producerEpoch:i16 | baseSequence:i32 | recordCount:i32` = 61 bytes.
inline constexpr std::size_t kRecordBatchHeaderSize = 61;

/// @brief Campos de la cabecera de un RecordBatch v2 que el broker necesita. Afinidad: INMUTABLE.
/// @details El broker trata el batch como un blob **opaco** (lo guarda y lo devuelve tal cual),
/// pero
///   necesita el `record_count` para avanzar los offsets del log y el `encoded_size` para recorrer
///   varios batches concatenados en un mismo blob de records.
struct RecordBatchInfo {
    std::int64_t base_offset = 0;   ///< Offset base que trae el productor (lo reasigna el log).
    std::int32_t record_count = 0;  ///< Número de records del batch.
    std::int32_t last_offset_delta = 0;  ///< Delta del último offset (= `record_count - 1`).
    std::size_t encoded_size = 0;  ///< Bytes totales del batch (`12 + batchLength`), para recorrer.
};

/// @brief Lee la cabecera del **primer** RecordBatch v2 al inicio de @p batch sin validar el CRC.
/// @details Decodificador defensivo (entrada no confiable): valida `magic == 2`, que `batchLength`
///   abarque al menos la cabecera fija y que el batch completo (`12 + batchLength`) quepa en
///   @p batch. **No** valida el CRC (eso lo hizo el productor; el broker no reinterpreta los
///   records).
/// @return La cabecera, o `Corrupt` si el batch está truncado, el `magic` no es v2 o `batchLength`
///   es inconsistente.
[[nodiscard]] expected<RecordBatchInfo> peek_record_batch(ByteSpan batch);

/// @brief Reescribe el `baseOffset` (primeros 8 bytes, big-endian) de un RecordBatch v2 in situ.
/// @details El log del broker asigna el offset base autoritativo al anexar; lo refleja en el batch
///   para que el consumidor vea offsets correctos. El `baseOffset` **no** está cubierto por el CRC
///   del batch (el CRC empieza en `attributes`), así que reescribirlo no invalida la integridad.
/// @param[in,out] batch Bytes del batch; su tamaño debe ser >= 8 (cabecera presente).
/// @param base_offset Offset base autoritativo asignado por el log.
/// @pre `batch.size() >= 8`.
void set_base_offset(std::span<std::byte> batch, std::int64_t base_offset) noexcept;

}  // namespace nexus::kafka
