/// @file   wire/frame_io.hpp
/// @brief  FrameReader/FrameWriter: lee y escribe tramas longitud-prefijo sobre un Socket async.
/// @ingroup wire

#pragma once

#include <cstddef>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "protocol/frame.hpp"

namespace nexus {

class Socket;
class Proactor;

/// @brief Trama recibida: cabecera + payload. Afinidad: REACTOR-LOCAL.
/// @details `payload` apunta **dentro del búfer del `FrameReader`** (zero-copy); es válido solo
///   hasta la siguiente llamada a `read_frame` sobre el mismo lector. Cópialo si debe sobrevivir.
struct Frame {
    FrameHeader header;
    ByteSpan payload;
};

/// @brief Lee tramas longitud-prefijo (§7.2) de un `Socket` vía el `Proactor`. REACTOR-LOCAL.
/// @details Lee primero el campo `length:u32`, lo valida (cota inferior = resto de cabecera; cota
///   superior = @p max_frame, anti-DoS) y luego el resto de la trama, reensamblando lecturas
///   parciales. Reutiliza un `Buffer` propio entre tramas para amortizar las reservas.
/// @invariant El `Socket` referenciado debe sobrevivir al lector y a sus lecturas en vuelo.
class FrameReader {
public:
    /// Adopta @p sock por referencia (no toma posesión): debe sobrevivir al lector.
    explicit FrameReader(Socket& sock) noexcept : sock_(sock) {}

    /// @brief Lee una trama completa. @param max_frame tamaño máximo total en el wire (incluido el
    ///   propio `length`). @return la `Frame` o un `Error` (`InvalidArgument` si la cabecera es
    ///   inválida o excede @p max_frame; `IoError` si el par cierra la conexión a media trama).
    task<expected<Frame>> read_frame(Proactor& proactor, std::size_t max_frame);

private:
    /// Lee de la red hasta que `buf_` contenga exactamente @p total bytes (reensambla parciales).
    task<expected<void>> read_exactly(Proactor& proactor, std::size_t total);

    Socket& sock_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Buffer buf_;    ///< Acumula la trama en curso; se reutiliza entre lecturas.
};

/// @brief Escribe tramas longitud-prefijo en un `Socket` vía el `Proactor`. REACTOR-LOCAL.
/// @details Codifica la cabecera (calculando `length` a partir del payload) y la envía seguida del
///   payload, reintentando envíos parciales hasta completar. Sin copia del payload.
/// @invariant El `Socket` referenciado debe sobrevivir al escritor y a sus envíos en vuelo.
class FrameWriter {
public:
    /// Adopta @p sock por referencia (no toma posesión): debe sobrevivir al escritor.
    explicit FrameWriter(Socket& sock) noexcept : sock_(sock) {}

    /// @brief Escribe una trama: cabecera + @p payload. `header.length` se **recalcula** a partir
    ///   del tamaño del payload (el llamante solo fija api_key/version/correlation_id/flags).
    /// @return éxito o `IoError` si el par cierra la conexión durante el envío.
    task<expected<void>> write_frame(Proactor& proactor, const FrameHeader& header,
                                     ByteSpan payload);

private:
    /// Envía @p data por completo, reintentando los envíos parciales.
    task<expected<void>> send_all(Proactor& proactor, ByteSpan data);

    Socket& sock_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Buffer buf_;    ///< Búfer de codificación de la cabecera; se reutiliza entre escrituras.
};

}  // namespace nexus
