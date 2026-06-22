/// @file   cluster/raft_receiver.cpp
/// @brief  Implementación de serve_raft_connection (bucle de recepción inter-nodo de Raft).
/// @ingroup cluster

#include "cluster/raft_receiver.hpp"

#include <utility>

#include "cluster/raft_link.hpp"
#include "common/error.hpp"

namespace nexus {

task<void> serve_raft_connection(Proactor& proactor, Socket socket, RaftEnvelopeHandler on_envelope,
                                 std::size_t max_message) {
    RaftEnvelopeReader reader{
        socket};  // referencia a `socket`, que vive en el frame de la corrutina
    while (true) {
        const expected<RaftEnvelope> envelope = co_await reader.read(proactor, max_message);
        if (!envelope) {
            co_return;  // EOF o sobre inválido: cierra la conexión (best-effort; el peer
                        // reconecta).
        }
        on_envelope(*envelope);
    }
}

}  // namespace nexus
