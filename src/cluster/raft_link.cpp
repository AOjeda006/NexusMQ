/// @file   cluster/raft_link.cpp
/// @brief  Implementación de RaftEnvelopeReader/Writer (enlace inter-nodo async sobre el Proactor).
/// @ingroup cluster

#include "cluster/raft_link.hpp"

#include <array>
#include <cstdint>

#include "common/types.hpp"  // store_le
#include "io/socket.hpp"
#include "protocol/codec.hpp"

namespace nexus {

namespace {
/// Tamaño del prefijo de longitud (u32) que precede a cada sobre en el wire.
constexpr std::size_t kLengthPrefix = sizeof(std::uint32_t);
}  // namespace

task<expected<void>> RaftEnvelopeReader::read_exactly(Proactor& proactor, std::size_t total) {
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

task<expected<RaftEnvelope>> RaftEnvelopeReader::read(Proactor& proactor, std::size_t max_message) {
    buf_.clear();

    // 1) Prefijo de longitud (u32): número de bytes del sobre que sigue.
    if (const expected<void> filled = co_await read_exactly(proactor, kLengthPrefix); !filled) {
        co_return std::unexpected(filled.error());
    }
    Decoder length_dec{buf_.as_span()};
    const expected<std::uint32_t> length = length_dec.get_u32();
    if (!length) {
        co_return std::unexpected(length.error());
    }

    // 2) Validar length: un sobre nunca es vacío (cota inferior) y no excede max_message
    // (anti-DoS).
    if (*length == 0) {
        co_return make_error(ErrorCode::InvalidArgument, "sobre de Raft con longitud cero");
    }
    const std::size_t total = kLengthPrefix + *length;  // tamaño total en el wire
    if (total > max_message) {
        co_return make_error(ErrorCode::InvalidArgument, "sobre de Raft excede max_message");
    }

    // 3) Leer el sobre completo (buf_ pasa a tener `total` bytes).
    if (const expected<void> filled = co_await read_exactly(proactor, total); !filled) {
        co_return std::unexpected(filled.error());
    }

    // 4) Decodificar el sobre (defensivo: entrada no confiable), saltando el prefijo de longitud.
    Decoder dec{buf_.as_span().subspan(kLengthPrefix)};
    co_return RaftEnvelope::decode(dec);
}

task<expected<void>> RaftEnvelopeWriter::send_all(Proactor& proactor, ByteSpan data) {
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

task<expected<void>> RaftEnvelopeWriter::write(Proactor& proactor, const RaftEnvelope& envelope) {
    buf_.clear();
    Encoder enc{buf_};
    envelope.encode(enc);  // buf_ = bytes del sobre

    // Prefijo de longitud (vive en el frame de la corrutina, sobrevive a los co_await de send_all).
    std::array<std::byte, kLengthPrefix> length_prefix{};
    store_le<std::uint32_t>(static_cast<std::uint32_t>(buf_.size()),
                            MutByteSpan{length_prefix.data(), length_prefix.size()});

    if (const expected<void> sent =
            co_await send_all(proactor, ByteSpan{length_prefix.data(), length_prefix.size()});
        !sent) {
        co_return std::unexpected(sent.error());
    }
    if (const expected<void> sent = co_await send_all(proactor, buf_.as_span()); !sent) {
        co_return std::unexpected(sent.error());
    }
    co_return expected<void>{};
}

}  // namespace nexus
