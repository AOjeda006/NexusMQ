/// @file record.hpp
/// @brief RecordBatch: unidad de escritura/replicación del log.
/// @ingroup storage

#pragma once

#include <cstdint>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Campos de cabecera de un RecordBatch.
/// @details 'length' y 'crc' no se guardan aquí: se derivan al codificar.
struct RecordBatchHeader {
    Offset base_offset = 0;            ///< Offset del primer record del batch.
    std::uint16_t attrs = 0;           ///< Códec de compresión + flags.
    std::int64_t producer_id = -1;     ///< Productor idempotente (-1 = ninguno).
    std::int16_t producer_epoch = -1;  ///< Época del productor.
    std::int32_t base_sequence = -1;   ///< Secuencia base (idempotencia).
    std::int32_t record_count = 0;     ///< Número de records del payload.
};

/// @brief Batch de records serializable, con CRC32C de integridad.
/// @details Layout en disco (little-endian): 'base_offset' | length | crc | attrs | producer_id |
/// producer_epoch | base_sequence | record_count | records'. El 'length' cuenta los bytes tras él;
/// el **CRC32C cubre desde 'attrs' hasta el final**. El payload de records se trata aquí como bytes
/// opacos.
/// @invariant decode(encode(x)) == x.
class RecordBatch {
public:
    RecordBatch(RecordBatchHeader header, std::vector<std::byte> records);

    /// Serializa el batch (con 'length' y 'crc') al final de @p out.
    void encode(Buffer& out) const;

    /// @brief Decodifica un batch desde @p data, validando 'length' y CRC32C.
    /// @return El batch, o 'Corrupt' si está truncado o el CRC no cuadra.
    [[nodiscard]] static expected<RecordBatch> decode(ByteSpan data);

    [[nodiscard]] const RecordBatchHeader& header() const noexcept { return header_; }
    [[nodiscard]] ByteSpan records() const noexcept { return records_; }

    /// Mayor offset del batch: 'base_offset + record_count - 1'.
    [[nodiscard]] Offset last_offset() const noexcept;

private:
    RecordBatchHeader header_;
    std::vector<std::byte> records_;
};

}  // namespace nexus
