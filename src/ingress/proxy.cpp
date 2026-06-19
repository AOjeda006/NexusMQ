/// @file   ingress/proxy.cpp
/// @brief  Implementación del modo proxy del ingress (enrutado + relevo de tramas).
/// @ingroup ingress

#include "ingress/proxy.hpp"

#include "protocol/frame.hpp"
#include "wire/frame_io.hpp"

namespace nexus {

Proxy::Proxy(LoadBalancer& balancer) noexcept : balancer_(balancer) {}

std::optional<NodeId> Proxy::route(std::string_view key) {
    return balancer_.pick(key);
}

task<expected<void>> Proxy::forward(Proactor& proactor, Socket& client, Socket& upstream,
                                    std::size_t max_frame) {
    FrameReader client_reader{client};
    FrameWriter client_writer{client};
    FrameReader upstream_reader{upstream};
    FrameWriter upstream_writer{upstream};

    while (true) {
        // 1) Petición del cliente. Un fallo de lectura (EOF o error) cierra el relevo limpiamente.
        const expected<Frame> request = co_await client_reader.read_frame(proactor, max_frame);
        if (!request) {
            co_return expected<void>{};
        }

        // 2) Reenvía la petición tal cual al líder (cabecera + payload, sin reinterpretar).
        if (const expected<void> sent =
                co_await upstream_writer.write_frame(proactor, request->header, request->payload);
            !sent) {
            co_return std::unexpected(sent.error());
        }

        // 3) Respuesta del líder; un fallo aquí sí es error (el líder cayó a media operación).
        const expected<Frame> response = co_await upstream_reader.read_frame(proactor, max_frame);
        if (!response) {
            co_return std::unexpected(response.error());
        }

        // 4) Devuelve la respuesta al cliente.
        if (const expected<void> sent =
                co_await client_writer.write_frame(proactor, response->header, response->payload);
            !sent) {
            co_return std::unexpected(sent.error());
        }
    }
}

}  // namespace nexus
