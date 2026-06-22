/// @file   cluster/raft_transport.cpp
/// @brief  Implementación de RaftTransport (emisión saliente de Raft sobre TCP a peers).
/// @ingroup cluster

#include "cluster/raft_transport.hpp"

#include <cstddef>
#include <utility>

#include "cluster/raft_link.hpp"
#include "common/error.hpp"
#include "io/proactor.hpp"

namespace nexus {

RaftTransport::RaftTransport(NodeId self, const PeerDirectory& directory, Proactor& proactor,
                             Spawner spawner)
    : RaftTransport(self, directory, proactor, std::move(spawner), Config{}) {}

RaftTransport::RaftTransport(NodeId self, const PeerDirectory& directory, Proactor& proactor,
                             Spawner spawner, Config config)
    : self_(self),
      directory_(directory),
      proactor_(proactor),
      spawner_(std::move(spawner)),
      config_(config) {}

void RaftTransport::send(const RaftEnvelope& envelope) {
    const NodeId to = envelope.message.to;
    if (to == self_ || !directory_.contains(to)) {
        return;  // no hay a quién enviar (uno mismo o peer desconocido): best-effort, se descarta.
    }
    PeerLink& link = links_[to];
    if (link.outbox.size() >= config_.max_queue) {
        link.outbox.pop_front();  // backpressure: descarta el más antiguo (Raft reenvía).
    }
    link.outbox.push_back(envelope);
    if (!link.sending) {
        link.sending = true;
        spawner_(run_sender(to));
    }
}

task<void> RaftTransport::run_sender(NodeId peer) {
    PeerLink& link = links_.at(peer);                    // estable: el peer no se borra.
    const PeerAddress* address = directory_.find(peer);  // existe: lo comprobó send().
    while (!link.outbox.empty()) {
        if (!link.socket.is_open()) {
            expected<Socket> sock =
                co_await Socket::async_connect(proactor_, address->host, address->port);
            if (!sock) {
                link.outbox.clear();  // sin conexión: descarta (Raft reenvía en el próximo tick).
                break;
            }
            sock->set_nodelay(true);  // Raft son mensajes pequeños y sensibles a latencia.
            link.socket = std::move(*sock);
        }
        // Saca un sobre y transpórtalo. Se mueve al frame de la corrutina: vive durante el
        // co_await.
        const RaftEnvelope envelope = std::move(link.outbox.front());
        link.outbox.pop_front();
        RaftEnvelopeWriter writer{link.socket};
        if (const expected<void> written = co_await writer.write(proactor_, envelope); !written) {
            link.socket.close();  // fallo de envío: cierra y reconecta en la próxima vuelta.
        }
    }
    link.sending = false;  // cola vacía: la conexión queda abierta para el próximo `send`.
}

}  // namespace nexus
