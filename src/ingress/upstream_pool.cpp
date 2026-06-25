/// @file   ingress/upstream_pool.cpp
/// @brief  Implementación del UpstreamPool (dialado asíncrono + reúso acotado de conexiones).
/// @ingroup ingress

#include "ingress/upstream_pool.hpp"

#include <utility>

#include "io/proactor.hpp"

namespace nexus {

task<expected<Socket>> UpstreamPool::acquire(Proactor& proactor, NodeId node) {
    // 1) Reúsa una conexión ociosa de la free-list del nodo, si hay (LIFO: la más reciente, más
    //    probablemente sana).
    if (const auto it = idle_.find(node); it != idle_.end() && !it->second.empty()) {
        Socket reused = std::move(it->second.back());
        it->second.pop_back();
        co_return reused;
    }

    // 2) Resuelve la dirección del plano de datos del nodo (validación en el borde, fail-fast).
    const PeerAddress* addr = peers_.find(node);
    if (addr == nullptr) {
        co_return make_error(ErrorCode::NotFound,
                             "nodo aguas arriba sin direccion en el directorio");
    }

    // 3) Sin ociosas: diala una conexión nueva de forma asíncrona (no bloquea el reactor).
    co_return co_await Socket::async_connect(proactor, addr->host, addr->port);
}

void UpstreamPool::release(NodeId node, Socket socket) {
    std::vector<Socket>& free_list = idle_[node];
    if (free_list.size() >= max_idle_per_node_) {
        return;  // free-list llena: el socket se cierra al destruirse (RAII), no se guarda.
    }
    free_list.push_back(std::move(socket));
}

std::size_t UpstreamPool::idle(NodeId node) const noexcept {
    const auto it = idle_.find(node);
    return it == idle_.end() ? 0 : it->second.size();
}

}  // namespace nexus
