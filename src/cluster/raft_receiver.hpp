/// @file   cluster/raft_receiver.hpp
/// @brief  serve_raft_connection: bucle de recepción de sobres de Raft de una conexión (ADR-0025).
/// @ingroup cluster

#pragma once

#include <cstddef>

#include "common/move_only_function.hpp"
#include "common/task.hpp"
#include "consensus/raft_wire.hpp"
#include "io/socket.hpp"

namespace nexus {

class Proactor;

/// @brief Manejador de un sobre de Raft recibido (lo invoca el bucle de recepción por cada sobre).
///   El plano inter-nodo lo cablea para enrutar el sobre al `RaftCarrier` dueño de su partición.
using RaftEnvelopeHandler = MoveOnlyFunction<void(const RaftEnvelope&)>;

/// @brief Sirve una conexión inter-nodo entrante: lee sobres de Raft en bucle y los entrega a
///   @p on_envelope hasta que el par cierra o hay un error. Afinidad: REACTOR-LOCAL.
/// @details Toma posesión de @p socket (lo cierra al terminar, RAII). Desencuadra con el
///   `RaftEnvelopeReader` (longitud-prefijo, anti-DoS por @p max_message). Es **best-effort**: ante
///   EOF o entrada inválida cierra la conexión (el peer reconecta); no propaga el error (lo absorbe
///   el cierre). No serializa ni enruta: eso lo decide @p on_envelope (separación de planos).
/// @param[in,out] proactor Puerto de E/S del reactor que sirve la conexión.
/// @param socket Conexión entrante (se mueve dentro; vive lo que dure la corrutina).
/// @param on_envelope Manejador invocado por cada sobre bien formado.
/// @param max_message Tamaño máximo de un sobre en el wire (incluido el `length`), anti-DoS.
[[nodiscard]] task<void> serve_raft_connection(Proactor& proactor, Socket socket,
                                               RaftEnvelopeHandler on_envelope,
                                               std::size_t max_message);

}  // namespace nexus
