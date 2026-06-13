/// @file   server/connection.cpp
/// @brief  Implementación del bucle de servicio de una conexión.
/// @ingroup server

#include "server/connection.hpp"

#include <cstddef>
#include <utility>

#include "broker/request_router.hpp"
#include "common/bytes.hpp"
#include "common/error.hpp"
#include "io/socket.hpp"
#include "protocol/codec.hpp"
#include "protocol/frame.hpp"
#include "wire/frame_io.hpp"

namespace nexus {

namespace {
/// Tope de tamaño de trama entrante (anti-DoS); las peticiones del protocolo son pequeñas salvo
/// los batches de Produce, acotados por el cliente y el broker.
constexpr std::size_t kMaxFrame = 16UL * 1024 * 1024;
}  // namespace

task<void> serve_connection(Proactor& proactor, Socket sock, RequestRouter& router) {
    FrameReader reader{sock};
    FrameWriter writer{sock};
    Buffer response_body;
    while (true) {
        const expected<Frame> frame = co_await reader.read_frame(proactor, kMaxFrame);
        if (!frame) {
            break;  // EOF o error de lectura: el par cerró → terminamos la conexión.
        }

        response_body.clear();
        Decoder body{frame->payload};
        const expected<void> dispatched =
            router.dispatch(frame->header.api_key, frame->header.api_version, body, response_body);
        if (!dispatched) {
            break;  // ApiKey no soportada / petición irrecuperable → cerramos.
        }

        // La cabecera de respuesta espeja api_key/version y el correlation_id de la petición.
        FrameHeader response_header;
        response_header.api_key = frame->header.api_key;
        response_header.api_version = frame->header.api_version;
        response_header.correlation_id = frame->header.correlation_id;
        const expected<void> sent =
            co_await writer.write_frame(proactor, response_header, response_body.as_span());
        if (!sent) {
            break;
        }
    }
    co_return;
}

}  // namespace nexus
