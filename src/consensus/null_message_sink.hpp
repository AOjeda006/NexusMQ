/// @file   consensus/null_message_sink.hpp
/// @brief  NullMessageSink: sumidero de Raft que descarta (sin transporte). Placeholder pre-D3.5.
/// @ingroup consensus

#pragma once

#include "consensus/raft_carrier.hpp"
#include "consensus/raft_wire.hpp"

namespace nexus {

/// @brief `RaftMessageSink` que **descarta** los sobres salientes. Afinidad: INMUTABLE (sin
/// estado).
/// @details Placeholder mientras no hay transporte inter-nodo real (llega en D3.5). Es válido para
///   el caso **mono-nodo** (grupo Raft de un único votante, sin peers): el portador nunca encola
///   mensajes salientes, así que `send` no se invoca. Si llegara a invocarse (peers configurados
///   sin transporte), el mensaje se pierde a propósito —el peer está inalcanzable—; la corrección
///   la garantiza el quórum, no este sumidero. Se sustituye por el transporte TCP en D3.5.
class NullMessageSink final : public RaftMessageSink {
public:
    void send(const RaftEnvelope& /*envelope*/) override {}
};

}  // namespace nexus
