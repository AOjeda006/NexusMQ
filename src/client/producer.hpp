/// @file   client/producer.hpp
/// @brief  Producer: fachada de publicación sobre el `Client` (Fase 1b).
/// @ingroup client

#pragma once

#include <span>
#include <string>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/record_codec.hpp"
#include "common/types.hpp"

namespace nexus {

class Client;

/// @brief Publica mensajes en un topic/partición construyendo un `RecordBatch`. Afinidad:
///   REACTOR-LOCAL (usa el `Client`, que no es thread-safe).
/// @details Empaqueta los records (key/value/headers anulables) con el codec por record (F2) dentro
///   del blob del batch y lo envía con `Produce`. 1b: no idempotente (sin reintentos ni créditos;
///   eso llega con la distribución de Fase 2). Mantiene una referencia al `Client`, que debe
///   sobrevivir al `Producer`.
class Producer {
public:
    explicit Producer(Client& client) noexcept : client_(client) {}

    /// @brief Publica un único @p value (sin clave) en @p topic/@p partition.
    /// @return el offset asignado.
    [[nodiscard]] expected<Offset> send(const std::string& topic, PartitionId partition,
                                        ByteSpan value);

    /// @brief Publica un record con @p key y @p value (para compactación por clave, F3).
    [[nodiscard]] expected<Offset> send_keyed(const std::string& topic, PartitionId partition,
                                              ByteSpan key, ByteSpan value);

    /// @brief Publica un **tombstone** (record con @p key y value nulo): borrado por clave.
    [[nodiscard]] expected<Offset> send_tombstone(const std::string& topic, PartitionId partition,
                                                  ByteSpan key);

    /// @brief Publica @p values (solo valor) como un solo batch (offsets consecutivos).
    /// @return el offset base (el del primer record) o el error traducido del `WireError`.
    [[nodiscard]] expected<Offset> send_batch(const std::string& topic, PartitionId partition,
                                              std::span<const ByteSpan> values);

    /// @brief Publica @p records (con control total de key/value/headers) como un solo batch.
    [[nodiscard]] expected<Offset> send_records(const std::string& topic, PartitionId partition,
                                                std::span<const Record> records);

private:
    Client& client_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

}  // namespace nexus
