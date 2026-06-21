/// @file   consensus/raft_wire.hpp
/// @brief  RaftEnvelope: sobre de transporte inter-nodo de un mensaje de Raft (ADR-0025).
/// @ingroup consensus

#pragma once

#include <string>

#include "common/error.hpp"
#include "common/types.hpp"
#include "consensus/raft_rpc.hpp"

namespace nexus {

class Encoder;
class Decoder;

/// @brief Mensaje de Raft enrutado a una réplica de partición concreta, para el transporte
///   inter-nodo (ADR-0025). Afinidad: INMUTABLE.
/// @details Envuelve un `RaftMessage` con la identidad global de la réplica destino
///   `(topic, partition)`, de modo que el nodo receptor entregue el RPC al `RaftNode` correcto.
///   Viaja por el **plano inter-nodo** (separado del plano de cliente, ADR-0004/0013) con prefijo
///   de longitud para *streaming* sobre TCP; el discriminante del `variant` del RPC viaja como
///   `type:u8` y el `payload` reutiliza el `encode`/`decode` por tipo de `raft_rpc` (ADR-0014).
/// @invariant `decode(encode(x)) == x` para todo sobre bien formado (round-trip).
struct RaftEnvelope {
    std::string topic;          ///< Topic de la réplica destino.
    PartitionId partition = 0;  ///< Índice de partición dentro del topic.
    RaftMessage message;        ///< RPC de Raft (con `from`/`to` y la variante de payload).

    /// @brief Serializa el sobre en @p enc: `topic | partition | from | to | type:u8 | payload`.
    /// @param[in,out] enc Codificador sobre el que se escribe el sobre.
    void encode(Encoder& enc) const;

    /// @brief Decodifica un sobre desde @p dec (decodificador defensivo, entrada no confiable).
    /// @param[in,out] dec Decodificador con los bytes del sobre.
    /// @return El sobre, o `InvalidArgument` si la entrada está truncada o `type` no corresponde a
    ///   ningún RPC de Raft.
    [[nodiscard]] static expected<RaftEnvelope> decode(Decoder& dec);

    bool operator==(const RaftEnvelope&) const = default;
};

}  // namespace nexus
