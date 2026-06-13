/// @file   server/server.cpp
/// @brief  Implementación del arranque del broker mono-nodo.
/// @ingroup server

#include "server/server.hpp"

#include <memory>
#include <utility>

#include "io/io_uring_backend.hpp"
#include "server/connection.hpp"

namespace nexus {

namespace {

/// Profundidad de la cola del anillo io_uring del reactor del servidor.
constexpr unsigned int kRingDepth = 256;

/// Bucle de aceptación: acepta conexiones y lanza una corrutina de servicio por cada una.
task<void> accept_loop(Reactor& reactor, Listener& listener, RequestRouter& router) {
    while (true) {
        expected<Socket> client = co_await listener.async_accept(reactor.proactor());
        if (!client) {
            co_return;  // listener cerrado o error: dejamos de aceptar.
        }
        reactor.spawn(serve_connection(reactor.proactor(), std::move(*client), router));
    }
}

}  // namespace

Server::Server(Config config)
    : config_(std::move(config)),
      topics_(config_.data_dir),
      reactor_(/*core_id=*/0, /*num_cores=*/1, std::make_unique<IoUringBackend>(kRingDepth)) {}

expected<void> Server::create_topic(std::string name, std::int32_t partition_count) {
    expected<TopicMetadata> meta = topics_.create_topic(std::move(name), partition_count);
    if (!meta) {
        return std::unexpected(meta.error());
    }
    return {};
}

expected<void> Server::bind() {
    expected<Listener> listener = Listener::bind(config_.host, config_.port);
    if (!listener) {
        return std::unexpected(listener.error());
    }
    listener_ = std::move(*listener);
    router_.emplace(topics_, config_.node_id, config_.advertised_host, listener_->local_port());
    return {};
}

std::uint16_t Server::port() const noexcept {
    return listener_ ? listener_->local_port() : 0;
}

void Server::run() {
    if (!listener_ || !router_) {
        return;  // Precondición: `bind()` antes de `run()`; sin listener no hay nada que servir.
    }
    reactor_.spawn(accept_loop(reactor_, *listener_, *router_));
    reactor_.run();
}

void Server::stop() noexcept {
    reactor_.stop();
}

}  // namespace nexus
