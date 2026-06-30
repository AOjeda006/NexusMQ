/// @file   server/connection.hpp
/// @brief  serve_connection: bucle leer-trama → despachar → escribir-trama de una conexión.
/// @ingroup server

#pragma once

#include <cstddef>
#include <utility>

#include "broker/request_router.hpp"
#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "protocol/codec.hpp"
#include "protocol/frame.hpp"
#include "wire/frame_io.hpp"

namespace nexus {

class Proactor;

/// @brief Sirve una conexión cliente hasta que el par cierra o hay un error. Afinidad:
///   REACTOR-LOCAL.
/// @details Bucle de petición/respuesta: lee una trama (`FrameReader`), despacha su cuerpo por
///   `RequestRouter` y escribe la respuesta (`FrameWriter`) con la cabecera que **espeja**
///   `correlation_id`/`api_key`. Toma posesión del @p stream (lo cierra al terminar, RAII). El tipo
///   de flujo (`Socket` en claro, `TlsConnection` cifrado) es un parámetro de plantilla, de modo
///   que el mismo bucle sirve ambos planos sin coste de llamada virtual (ADR-0019).
/// @note El diseño original modelaba una clase `Connection`; aquí es una corrutina libre que posee
///   el flujo en su *frame* (evita miembros auto-referenciados).
template <ByteStream Stream>
task<void> serve_connection(Proactor& proactor, Stream stream, RequestRouter& router) {
    /// Tope de tamaño de trama entrante (anti-DoS); las peticiones del protocolo son pequeñas salvo
    /// los batches de Produce, acotados por el cliente y el broker.
    constexpr std::size_t kMaxClientFrame = 16UL * 1024 * 1024;

    FrameReader reader{stream};
    FrameWriter writer{stream};
    Buffer response_body;
    while (true) {
        const expected<Frame> frame = co_await reader.read_frame(proactor, kMaxClientFrame);
        if (!frame) {
            break;  // EOF o error de lectura: el par cerró → terminamos la conexión.
        }

        response_body.clear();
        Decoder body{frame->payload};
        const expected<void> dispatched = co_await router.dispatch(
            frame->header.api_key, frame->header.api_version, body, response_body);
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
