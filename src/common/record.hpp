/// @file record.hpp
/// @brief RecordBatch: unidad de escritura/replicación del log.
/// @ingroup storage

#pragma once

#include <cstddef>
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

/// @brief Vista ligera de la cabecera de un batch en disco. Afinidad: INMUTABLE.
/// @details La produce 'RecordBatch::peek' sin copiar los records ni validar el CRC: sirve
/// para **recorrer** el log (seek/recuperación) conociendo el tamaño y los offsets de cada
/// batch a coste mínimo.
struct RecordBatchView {
    Offset base_offset = 0;         ///< Offset del primer record del batch.
    std::int32_t record_count = 0;  ///< Número de records del batch.
    std::size_t encoded_size = 0;   ///< Bytes totales del batch en disco (cabecera + records).

    /// Mayor offset del batch: 'base_offset + record_count - 1'.
    [[nodiscard]] Offset last_offset() const noexcept { return base_offset + record_count - 1; }
};

/// @brief Batch de records serializable, con CRC32C de integridad.
/// @details Layout en disco (little-endian): 'base_offset' | length | crc | attrs | producer_id |
/// producer_epoch | base_sequence | record_count | records'. El 'length' cuenta los bytes tras él;
/// el **CRC32C cubre desde 'attrs' hasta el final**. El payload de records se trata aquí como bytes
/// opacos.
/// @invariant decode(encode(x)) == x.
class RecordBatch {
public:
    /// Tamaño de la cabecera fija en disco (offsets de campo en .cpp).
    static constexpr std::size_t kHeaderSize = 36;

    RecordBatch(RecordBatchHeader header, std::vector<std::byte> records);

    /// Serializa el batch (con 'length' y 'crc') al final de @p out.
    void encode(Buffer& out) const;

    /// @brief Decodifica un batch desde @p data, validando 'length' y CRC32C.
    /// @return El batch, o 'Corrupt' si está truncado o el CRC no cuadra.
    [[nodiscard]] static expected<RecordBatch> decode(ByteSpan data);

    /// @brief Lee la cabecera de un batch sin copiar records ni validar CRC.
    /// @details Decodificador defensivo: acota 'length' antes de derivar 'encoded_size'.
    ///   No comprueba que @p data contenga el batch completo (eso lo hace 'decode'); el que
    ///   recorre el log valida el tamaño contra los bytes disponibles.
    /// @return La vista, o 'Corrupt' si la cabecera está truncada o 'length' es inconsistente.
    [[nodiscard]] static expected<RecordBatchView> peek(ByteSpan data);

    [[nodiscard]] const RecordBatchHeader& header() const noexcept { return header_; }
    [[nodiscard]] ByteSpan records() const noexcept { return records_; }

    /// Bytes totales del batch al serializarse (cabecera + records).
    [[nodiscard]] std::size_t encoded_size() const noexcept {
        return kHeaderSize + records_.size();
    }

    /// Mayor offset del batch: 'base_offset + record_count - 1'.
    [[nodiscard]] Offset last_offset() const noexcept;

private:
    RecordBatchHeader header_;
    std::vector<std::byte> records_;
};

}  // namespace nexus
