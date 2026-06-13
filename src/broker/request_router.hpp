/// @file   broker/request_router.hpp
/// @brief  RequestRouter: despacha una petición decodificada del protocolo a la lógica del broker.
/// @ingroup broker

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "broker/offset_manager.hpp"
#include "broker/topic_manager.hpp"
#include "common/bytes.hpp"
#include "common/error.hpp"
#include "protocol/frame.hpp"
#include "protocol/versioning.hpp"

namespace nexus {

class Decoder;

/// @brief Traduce una petición (cuerpo ya enmarcado) en su respuesta, sobre la lógica del broker.
///   Afinidad: REACTOR-LOCAL.
/// @details Es el puente protocolo↔dominio: decodifica el cuerpo según la `ApiKey`, llama al
///   `TopicManager`/`Partition` y **codifica la respuesta** en un búfer. No toca framing ni sockets
///   (eso es la `Connection` del servidor): así es testeable sin red. Los errores del núcleo se
///   traducen a `WireError` con `from_error` en el borde (ADR-0009).
class RequestRouter {
public:
    RequestRouter(TopicManager& topics, NodeId node_id, std::string host,
                  std::uint16_t port) noexcept
        : topics_(topics), node_id_(node_id), host_(std::move(host)), port_(port) {}

    /// @brief Despacha @p key (cuerpo en @p body) y escribe la respuesta codificada en @p out.
    /// @return Éxito (con @p out relleno) o un error si la `ApiKey` no se soporta o el cuerpo es
    ///   indecodificable de forma irrecuperable (la `Connection` cierra entonces la conexión).
    [[nodiscard]] expected<void> dispatch(ApiKey key, std::uint16_t api_version, Decoder& body,
                                          Buffer& out);

    /// Rangos de versión que soporta este servidor (para `ApiVersions` y la negociación).
    [[nodiscard]] static std::vector<ApiVersionRange> supported_versions();

private:
    TopicManager& topics_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    NodeId node_id_;
    std::string host_;
    std::uint16_t port_;
    /// Offsets confirmados por grupo (REACTOR-LOCAL: uno por router/reactor; ver `OffsetManager`).
    OffsetManager offsets_;
};

}  // namespace nexus
