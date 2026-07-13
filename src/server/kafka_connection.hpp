/// @file   server/kafka_connection.hpp
/// @brief  serve_kafka_connection: bucle leer-trama Kafka (`Size:INT32`) → gateway → escribir —
/// F7f.
/// @ingroup server

#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "kafka/codec.hpp"
#include "kafka/gateway.hpp"
#include "wire/frame_io.hpp"  // ByteStream

namespace nexus {

class Proactor;

namespace detail {

/// @brief Lee de @p stream hasta que @p buf contenga exactamente @p total bytes (reensambla
///   parciales). @return éxito, o un error si el par cierra a media trama (EOF).
template <ByteStream Stream>
task<expected<void>> kafka_read_exactly(Proactor& proactor, Stream& stream, Buffer& buf,
                                        std::size_t total) {
    while (buf.size() < total) {
        const std::size_t already = buf.size();
        const MutByteSpan tail = buf.extend(total - already);
        const expected<std::size_t> got = co_await stream.async_recv(proactor, tail);
        if (!got) {
            buf.truncate(already);
            co_return std::unexpected(got.error());
        }
        if (*got == 0) {
            buf.truncate(already);
            co_return make_error(ErrorCode::IoError, "conexión Kafka cerrada por el par (EOF)");
        }
        buf.truncate(already + *got);
    }
    co_return expected<void>{};
}

/// Envía @p data por completo, reintentando los envíos parciales.
template <ByteStream Stream>
task<expected<void>> kafka_send_all(Proactor& proactor, Stream& stream, ByteSpan data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const expected<std::size_t> n = co_await stream.async_send(proactor, data.subspan(sent));
        if (!n) {
            co_return std::unexpected(n.error());
        }
        if (*n == 0) {
            co_return make_error(ErrorCode::IoError, "send Kafka devolvió 0 (conexión cerrada)");
        }
        sent += *n;
    }
    co_return expected<void>{};
}

}  // namespace detail

/// @brief Sirve una conexión de un cliente **Kafka** hasta que cierra o hay error. REACTOR-LOCAL.
/// @details Framing de Kafka: cada petición/respuesta va precedida de su tamaño en `INT32`
///   big-endian. Lee el tamaño, lee el cuerpo, lo pasa a `KafkaGateway::handle_request` (que enruta
///   por `api_key` y delega en el broker) y antepone el tamaño a la respuesta. Toma posesión del
///   @p stream (lo cierra al terminar, RAII). El gateway/broker son compartidos (referencia); el
///   bucle es per-conexión. Una `api_key` no soportada o una petición irrecuperable cierran la
///   conexión (como hace el camino nativo).
/// @param[in,out] gateway Dispatcher Kafka compartido (sobre el adaptador del broker).
template <ByteStream Stream>
task<void> serve_kafka_connection(Proactor& proactor, Stream stream, kafka::KafkaGateway& gateway) {
    /// Tope de tamaño de una petición Kafka entrante (anti-DoS); holgado para batches de Produce.
    constexpr std::size_t kMaxKafkaFrame = 16UL * 1024 * 1024;

    Buffer req_buf;
    Buffer size_prefix;
    while (true) {
        // 1) Tamaño de la petición (INT32 big-endian).
        req_buf.clear();
        if (const expected<void> got = co_await detail::kafka_read_exactly(
                proactor, stream, req_buf, sizeof(std::int32_t));
            !got) {
            break;  // EOF o error: el par cerró.
        }
        kafka::Decoder size_dec{req_buf.as_span()};
        const expected<std::int32_t> size = size_dec.get_i32();
        if (!size || *size <= 0 || std::cmp_greater(*size, kMaxKafkaFrame)) {
            break;  // tamaño inválido o excesivo: cerramos.
        }

        // 2) Cuerpo de la petición.
        req_buf.clear();
        if (const expected<void> got = co_await detail::kafka_read_exactly(
                proactor, stream, req_buf, static_cast<std::size_t>(*size));
            !got) {
            break;
        }

        // 3) Despacho (puede saltar al reactor dueño de la partición y volver).
        const expected<Buffer> response = co_await gateway.handle_request(req_buf.as_span());
        if (!response) {
            break;  // api_key no soportada / petición irrecuperable: cerramos.
        }

        // 4) Respuesta con su tamaño antepuesto (INT32 big-endian).
        size_prefix.clear();
        kafka::Encoder size_enc{size_prefix};
        size_enc.put_i32(static_cast<std::int32_t>(response->size()));
        if (const expected<void> sent =
                co_await detail::kafka_send_all(proactor, stream, size_prefix.as_span());
            !sent) {
            break;
        }
        if (const expected<void> sent =
                co_await detail::kafka_send_all(proactor, stream, response->as_span());
            !sent) {
            break;
        }
    }
    co_return;
}

}  // namespace nexus
