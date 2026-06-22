/// @file   cluster/raft_link.hpp
/// @brief  RaftEnvelopeReader/Writer: enlace inter-nodo longitud-prefijo de RaftEnvelope
/// (ADR-0025).
/// @ingroup cluster

#pragma once

#include <cstddef>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "consensus/raft_wire.hpp"

namespace nexus {

class Socket;
class Proactor;

/// @brief Lee `RaftEnvelope` longitud-prefijo de un `Socket` async (plano inter-nodo). Afinidad:
///   REACTOR-LOCAL.
/// @details Wire: `length:u32` (little-endian, número de bytes del sobre) seguido de los bytes del
///   sobre (`RaftEnvelope::decode`). Es un plano **separado** del de cliente (ADR-0004/0013/0025):
///   no usa `FrameHeader`/`ApiKey` para mantener ambos protocolos ortogonales. Reensambla lecturas
///   parciales y acota el tamaño (anti-DoS), reutilizando un `Buffer` propio entre sobres.
/// @invariant El `Socket` referenciado debe sobrevivir al lector y a sus lecturas en vuelo.
class RaftEnvelopeReader {
public:
    /// Adopta @p sock por referencia (no toma posesión): debe sobrevivir al lector.
    explicit RaftEnvelopeReader(Socket& sock) noexcept : sock_(sock) {}

    /// @brief Lee un sobre completo.
    /// @param[in,out] proactor Puerto de E/S del reactor sobre el que se recibe.
    /// @param max_message Tamaño máximo total en el wire (incluido el propio `length`), anti-DoS.
    /// @return El `RaftEnvelope`, o un `Error` (`InvalidArgument` si `length` es 0 o excede
    ///   @p max_message o el sobre está mal formado; `IoError` si el par cierra a media trama).
    [[nodiscard]] task<expected<RaftEnvelope>> read(Proactor& proactor, std::size_t max_message);

private:
    /// Lee de la red hasta que `buf_` contenga exactamente @p total bytes (reensambla parciales).
    [[nodiscard]] task<expected<void>> read_exactly(Proactor& proactor, std::size_t total);

    Socket& sock_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Buffer buf_;    ///< Acumula el sobre en curso; se reutiliza entre lecturas.
};

/// @brief Escribe `RaftEnvelope` longitud-prefijo en un `Socket` async (plano inter-nodo).
///   Afinidad: REACTOR-LOCAL.
/// @details Codifica el sobre, antepone su longitud (`u32`) y lo envía, reintentando los envíos
///   parciales hasta completar. Reutiliza un `Buffer` propio entre escrituras.
/// @invariant El `Socket` referenciado debe sobrevivir al escritor y a sus envíos en vuelo.
class RaftEnvelopeWriter {
public:
    /// Adopta @p sock por referencia (no toma posesión): debe sobrevivir al escritor.
    explicit RaftEnvelopeWriter(Socket& sock) noexcept : sock_(sock) {}

    /// @brief Escribe @p envelope precedido de su longitud.
    /// @param[in,out] proactor Puerto de E/S del reactor sobre el que se envía.
    /// @return Éxito, o `IoError` si el par cierra la conexión durante el envío.
    [[nodiscard]] task<expected<void>> write(Proactor& proactor, const RaftEnvelope& envelope);

private:
    /// Envía @p data por completo, reintentando los envíos parciales.
    [[nodiscard]] task<expected<void>> send_all(Proactor& proactor, ByteSpan data);

    Socket& sock_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Buffer buf_;    ///< Búfer de codificación del sobre; se reutiliza entre escrituras.
};

}  // namespace nexus
