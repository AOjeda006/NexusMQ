/// @file   server/server.cpp
/// @brief  Implementación del arranque del broker mono-nodo.
/// @ingroup server

#include "server/server.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "broker/consumer_group.hpp"
#include "broker/group_coordinator.hpp"
#include "io/io_uring_backend.hpp"
#include "server/admin_http.hpp"
#include "server/connection.hpp"

namespace nexus {

namespace {

/// Profundidad de la cola del anillo io_uring del reactor del servidor.
constexpr unsigned int kRingDepth = 256;

/// Bucle de aceptación del plano de datos: una corrutina de servicio por conexión.
task<void> accept_loop(Reactor& reactor, Listener& listener, RequestRouter& router) {
    while (true) {
        expected<Socket> client = co_await listener.async_accept(reactor.proactor());
        if (!client) {
            co_return;  // listener cerrado o error: dejamos de aceptar.
        }
        reactor.spawn(serve_connection(reactor.proactor(), std::move(*client), router));
    }
}

/// Bucle de aceptación del plano de operación: una corrutina HTTP por conexión.
task<void> admin_accept_loop(Reactor& reactor, Listener& listener, const AdminRouter& router) {
    while (true) {
        expected<Socket> client = co_await listener.async_accept(reactor.proactor());
        if (!client) {
            co_return;
        }
        reactor.spawn(serve_admin_connection(reactor.proactor(), std::move(*client), router));
    }
}

}  // namespace

Server::Server(Config config)
    : config_(std::move(config)),
      topics_(config_.data_dir),
      reactor_(/*core_id=*/0, /*num_cores=*/1, std::make_unique<IoUringBackend>(kRingDepth)) {
    const JwtVerifier* verifier = nullptr;
    if (!config_.jwt_secret.empty()) {
        verifier = &jwt_.emplace(config_.jwt_secret);
    }
    // `emplace` devuelve la referencia al objeto construido: la usamos directamente para no
    // desreferenciar el `optional` recién poblado (bugprone-unchecked-optional-access en tidy-18).
    AdminApi& api = admin_api_.emplace(topics_, config_.node_id,
                                       [this](Page page) { return list_groups(page); });
    RestGateway& rest = rest_.emplace(api, verifier);
    admin_router_.emplace(rest, health_, metrics_);
    if (config_.min_free_disk_bytes > 0) {
        health_.register_readiness("disk",
                                   disk_space_probe(config_.data_dir, config_.min_free_disk_bytes));
    }
}

std::vector<GroupSummary> Server::list_groups(Page page) const {
    if (!router_) {
        return {};
    }
    const std::vector<GroupDigest> digests = router_->group_coordinator().list_groups();
    std::vector<GroupSummary> summaries;
    summaries.reserve(digests.size());
    for (const GroupDigest& digest : digests) {
        summaries.push_back(
            GroupSummary{.group_id = digest.group_id,
                         .state = std::string{group_state_name(digest.state)},
                         .generation = digest.generation,
                         .member_count = static_cast<std::int64_t>(digest.member_count)});
    }
    const std::size_t offset = page.offset();
    if (offset >= summaries.size()) {
        return {};
    }
    const std::size_t end = std::min(summaries.size(), offset + page.size);
    return {std::make_move_iterator(summaries.begin() + static_cast<std::ptrdiff_t>(offset)),
            std::make_move_iterator(summaries.begin() + static_cast<std::ptrdiff_t>(end))};
}

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

    if (config_.admin_port) {
        expected<Listener> admin = Listener::bind(config_.host, *config_.admin_port);
        if (!admin) {
            return std::unexpected(admin.error());
        }
        admin_listener_ = std::move(*admin);
    }
    return {};
}

std::uint16_t Server::port() const noexcept {
    return listener_ ? listener_->local_port() : 0;
}

std::uint16_t Server::admin_port() const noexcept {
    return admin_listener_ ? admin_listener_->local_port() : 0;
}

void Server::run() {
    if (!listener_ || !router_) {
        return;  // Precondición: `bind()` antes de `run()`; sin listener no hay nada que servir.
    }
    reactor_.spawn(accept_loop(reactor_, *listener_, *router_));
    if (admin_listener_ && admin_router_) {
        reactor_.spawn(admin_accept_loop(reactor_, *admin_listener_, *admin_router_));
    }
    health_.set_started(true);  // ya servimos: `/readyz` puede dar 200.
    reactor_.run();
}

void Server::stop() noexcept {
    health_.set_live(false);  // drenando: `/healthz` da 503 para sacarnos del balanceador.
    reactor_.stop();
}

}  // namespace nexus
