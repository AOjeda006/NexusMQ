/// @file   client/producer.hpp
/// @brief  Producer: fachada de publicación sobre el `Client` (Fase 1b).
/// @ingroup client

#pragma once

#include <span>
#include <string>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"

namespace nexus {

class Client;

/// @brief Publica mensajes en un topic/partición construyendo un `RecordBatch`. Afinidad:
///   REACTOR-LOCAL (usa el `Client`, que no es thread-safe).
/// @details Empaqueta los valores como records longitud-prefijo dentro del blob opaco del batch
///   (el broker no los interpreta) y lo envía con `Produce`. 1b: no idempotente (sin reintentos ni
///   créditos; eso llega con la distribución de Fase 2). Mantiene una referencia al `Client`, que
///   debe sobrevivir al `Producer`.
class Producer {
public:
    explicit Producer(Client& client) noexcept : client_(client) {}

    /// @brief Publica un único @p value en @p topic/@p partition. @return el offset asignado.
    [[nodiscard]] expected<Offset> send(const std::string& topic, PartitionId partition,
                                        ByteSpan value);

    /// @brief Publica @p values como un solo batch (cada uno recibe un offset consecutivo).
    /// @return el offset base (el del primer record) o el error traducido del `WireError`.
    [[nodiscard]] expected<Offset> send_batch(const std::string& topic, PartitionId partition,
                                              std::span<const ByteSpan> values);

private:
    Client& client_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

}  // namespace nexus
