/// @file   wire/frame_io.cpp
/// @brief  Implementación de FrameReader/FrameWriter (framing async sobre el Proactor).
/// @ingroup wire

#include "wire/frame_io.hpp"

#include <cstdint>

#include "io/socket.hpp"
#include "protocol/codec.hpp"

namespace nexus {

namespace {
/// Bytes de la cabecera que siguen al campo `length` (resto de cabecera = length mínimo válido).
constexpr std::size_t kHeaderRest = FrameHeader::kEncodedSize - sizeof(std::uint32_t);
}  // namespace

task<expected<void>> FrameReader::read_exactly(Proactor& proactor, std::size_t total) {
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

task<expected<Frame>> FrameReader::read_frame(Proactor& proactor, std::size_t max_frame) {
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

    // 2) Validar length: cota inferior (debe caber el resto de cabecera) y superior (max_frame).
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

task<expected<void>> FrameWriter::send_all(Proactor& proactor, ByteSpan data) {
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

task<expected<void>> FrameWriter::write_frame(Proactor& proactor, const FrameHeader& header,
                                              ByteSpan payload) {
    FrameHeader hdr = header;
    hdr.length = FrameHeader::length_for(payload.size());  // consistencia: el escritor fija length

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

}  // namespace nexus
