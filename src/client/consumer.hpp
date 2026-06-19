/// @file   client/consumer.hpp
/// @brief  Consumer: fachada de consumo por partición sobre el `Client` (Fase 1b).
/// @ingroup client

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/error.hpp"
#include "common/record_codec.hpp"
#include "common/types.hpp"

namespace nexus {

class Client;

/// @brief Un record consumido (copias propietarias). Afinidad: INMUTABLE.
/// @details `key`/`value` son **anulables**: `value == nullopt` es un **tombstone** (borrado por
///   clave). El `offset` es el absoluto en la partición.
struct ConsumedRecord {
    Offset offset = 0;
    std::optional<std::vector<std::byte>> key;
    std::optional<std::vector<std::byte>> value;
    std::vector<RecordHeader> headers;
};

/// @brief Consume de una (topic, partición) llevando la posición de lectura. Afinidad:
///   REACTOR-LOCAL (usa el `Client`, que no es thread-safe).
/// @details Fase 1b: consumo **simple por partición y offset** (sin grupos: `subscribe`/`commit`/
///   `JoinGroup`/`SyncGroup`/`Heartbeat` y el rebalanceo cooperativo llegan en Fase 2). `poll`
///   pide un `Fetch` desde la posición actual, decodifica los `RecordBatch` devueltos en records
///   individuales y avanza la posición. Mantiene una referencia al `Client`, que debe sobrevivirlo.
class Consumer {
public:
    Consumer(Client& client, std::string topic, PartitionId partition) noexcept
        : client_(client), topic_(std::move(topic)), partition_(partition) {}

    /// @brief Lee desde la posición actual (hasta @p max_bytes); avanza la posición tras
    /// decodificar.
    /// @return los records leídos (vacío si está al día) o el error traducido del `WireError`.
    [[nodiscard]] expected<std::vector<ConsumedRecord>> poll(
        std::int32_t max_bytes = kDefaultMaxBytes);

    /// Próximo offset a leer.
    [[nodiscard]] Offset position() const noexcept { return position_; }
    /// Reposiciona la lectura en @p offset.
    void seek(Offset offset) noexcept { position_ = offset; }

private:
    /// Tope de bytes por `poll` por defecto (1 MiB).
    static constexpr std::int32_t kDefaultMaxBytes = 1 << 20;

    Client& client_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::string topic_;
    PartitionId partition_;
    Offset position_ = 0;
};

}  // namespace nexus
