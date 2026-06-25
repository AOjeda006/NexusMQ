/// @file   wire/frame_io.hpp
/// @brief  FrameReader/FrameWriter: lee y escribe tramas longitud-prefijo sobre un flujo async.
/// @ingroup wire

#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "protocol/codec.hpp"
#include "protocol/frame.hpp"

namespace nexus {

class Proactor;

/// @brief Flujo de bytes asíncrono sobre el `Proactor`: contrato mínimo que consumen
///   `FrameReader`/`FrameWriter`. Afinidad: REACTOR-LOCAL.
/// @details Lo cumplen `Socket` (texto plano) y `TlsConnection` (cifrado), de modo que el mismo
///   framing sirve sobre ambos sin coste de llamada virtual (DI **estática** en el hot-path:
///   parámetro de plantilla, no interfaz dinámica; ADR-0019). `async_recv` recibe en un
///   `MutByteSpan` y produce los bytes leídos (`0` = el par cerró); `async_send` envía un
///   `ByteSpan` y produce los bytes aceptados. Ambos devuelven `task<expected<std::size_t>>`.
template <typename S>
concept ByteStream = requires(S& stream, Proactor& proactor, MutByteSpan in, ByteSpan out) {
    { stream.async_recv(proactor, in) } -> std::same_as<task<expected<std::size_t>>>;
    { stream.async_send(proactor, out) } -> std::same_as<task<expected<std::size_t>>>;
};

/// @brief Trama recibida: cabecera + payload. Afinidad: REACTOR-LOCAL.
/// @details `payload` apunta **dentro del búfer del `FrameReader`** (zero-copy); es válido solo
///   hasta la siguiente llamada a `read_frame` sobre el mismo lector. Cópialo si debe sobrevivir.
struct Frame {
    FrameHeader header;
    ByteSpan payload;
};

/// @brief Lee tramas longitud-prefijo (§7.2) de un `ByteStream` vía el `Proactor`. REACTOR-LOCAL.
/// @details Lee primero el campo `length:u32`, lo valida (cota inferior = resto de cabecera; cota
///   superior = @p max_frame, anti-DoS) y luego el resto de la trama, reensamblando lecturas
///   parciales. Reutiliza un `Buffer` propio entre tramas para amortizar las reservas. El tipo de
///   flujo se deduce por CTAD (`FrameReader reader{stream}`).
/// @invariant El flujo referenciado debe sobrevivir al lector y a sus lecturas en vuelo.
template <ByteStream Stream>
class FrameReader {
public:
    /// Adopta @p stream por referencia (no toma posesión): debe sobrevivir al lector.
    explicit FrameReader(Stream& stream) noexcept : sock_(stream) {}

    /// @brief Lee una trama completa. @param max_frame tamaño máximo total en el wire (incluido el
    ///   propio `length`). @return la `Frame` o un `Error` (`InvalidArgument` si la cabecera es
    ///   inválida o excede @p max_frame; `IoError` si el par cierra la conexión a media trama).
    task<expected<Frame>> read_frame(Proactor& proactor, std::size_t max_frame) {
        buf_.clear();

        // 1) Campo length (u32): cuántos bytes siguen (resto de cabecera + payload).
        if (const expected<void> filled = co_await read_exactly(proactor, sizeof(std::uint32_t));
            !filled) {
            co_return std::unexpected(filled.error());
        }
        Decoder length_dec{buf_.as_span()};
        const expected<std::uint32_t> length = length_dec.get_u32();
        if (!length) {
            co_return std::unexpected(length.error());
        }

        // 2) Validar length: cota inferior (debe caber el resto de cabecera) y superior
        // (max_frame).
        if (*length < kHeaderRest) {
            co_return make_error(ErrorCode::InvalidArgument,
                                 "trama truncada: length < resto de cabecera");
        }
        const std::size_t total = sizeof(std::uint32_t) + *length;  // tamaño total en el wire
        if (total > max_frame) {
            co_return make_error(ErrorCode::InvalidArgument, "trama excede max_frame");
        }

        // 3) Leer el resto de la trama (buf_ pasa a tener `total` bytes).
        if (const expected<void> filled = co_await read_exactly(proactor, total); !filled) {
            co_return std::unexpected(filled.error());
        }

        // 4) Decodificar la cabecera completa y exponer el payload (zero-copy en buf_).
        Decoder dec{buf_.as_span()};
        const expected<FrameHeader> header = FrameHeader::decode(dec);
        if (!header) {
            co_return std::unexpected(header.error());
        }
        const ByteSpan payload = buf_.as_span().subspan(FrameHeader::kEncodedSize);
        co_return Frame{.header = *header, .payload = payload};
    }

private:
    /// Bytes de la cabecera que siguen al campo `length` (resto = length mínimo válido).
    static constexpr std::size_t kHeaderRest = FrameHeader::kEncodedSize - sizeof(std::uint32_t);

    /// Lee de la red hasta que `buf_` contenga exactamente @p total bytes (reensambla parciales).
    task<expected<void>> read_exactly(Proactor& proactor, std::size_t total) {
        while (buf_.size() < total) {
            const std::size_t already = buf_.size();
            const MutByteSpan tail = buf_.extend(total - already);
            const expected<std::size_t> got = co_await sock_.async_recv(proactor, tail);
            if (!got) {
                buf_.truncate(already);  // descarta la cola sin llenar
                co_return std::unexpected(got.error());
            }
            if (*got == 0) {
                buf_.truncate(already);
                co_return make_error(ErrorCode::IoError,
                                     "conexión cerrada por el par (EOF) a media trama");
            }
            buf_.truncate(already + *got);
        }
        co_return expected<void>{};
    }

    Stream& sock_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Buffer buf_;    ///< Acumula la trama en curso; se reutiliza entre lecturas.
};

/// @brief Escribe tramas longitud-prefijo en un `ByteStream` vía el `Proactor`. REACTOR-LOCAL.
/// @details Codifica la cabecera (calculando `length` a partir del payload) y la envía seguida del
///   payload, reintentando envíos parciales hasta completar. Sin copia del payload. El tipo de
///   flujo se deduce por CTAD (`FrameWriter writer{stream}`).
/// @invariant El flujo referenciado debe sobrevivir al escritor y a sus envíos en vuelo.
template <ByteStream Stream>
class FrameWriter {
public:
    /// Adopta @p stream por referencia (no toma posesión): debe sobrevivir al escritor.
    explicit FrameWriter(Stream& stream) noexcept : sock_(stream) {}

    /// @brief Escribe una trama: cabecera + @p payload. `header.length` se **recalcula** a partir
    ///   del tamaño del payload (el llamante solo fija api_key/version/correlation_id/flags).
    /// @return éxito o `IoError` si el par cierra la conexión durante el envío.
    task<expected<void>> write_frame(Proactor& proactor, const FrameHeader& header,
                                     ByteSpan payload) {
        FrameHeader hdr = header;
        hdr.length =
            FrameHeader::length_for(payload.size());  // consistencia: el escritor fija length

        buf_.clear();
        Encoder enc{buf_};
        hdr.encode(enc);
        if (const expected<void> sent = co_await send_all(proactor, buf_.as_span()); !sent) {
            co_return std::unexpected(sent.error());
        }
        if (!payload.empty()) {
            if (const expected<void> sent = co_await send_all(proactor, payload); !sent) {
                co_return std::unexpected(sent.error());
            }
        }
        co_return expected<void>{};
    }

private:
    /// Envía @p data por completo, reintentando los envíos parciales.
    task<expected<void>> send_all(Proactor& proactor, ByteSpan data) {
        std::size_t sent = 0;
        while (sent < data.size()) {
            const expected<std::size_t> n = co_await sock_.async_send(proactor, data.subspan(sent));
            if (!n) {
                co_return std::unexpected(n.error());
            }
            if (*n == 0) {
                co_return make_error(ErrorCode::IoError, "send devolvió 0 (conexión cerrada)");
            }
            sent += *n;
        }
        co_return expected<void>{};
    }

    Stream& sock_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Buffer buf_;    ///< Búfer de codificación de la cabecera; se reutiliza entre escrituras.
};

}  // namespace nexus
