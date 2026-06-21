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

/// Profundidad de la cola del anillo io_uring de cada reactor del servidor.
constexpr unsigned int kRingDepth = 256;

/// Factoría por defecto del `Proactor` de cada reactor: io_uring (plano de control).
std::unique_ptr<Proactor> make_io_uring_proactor(int /*core_id*/) {
    return std::make_unique<IoUringBackend>(kRingDepth);
}

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

Server::Server(Config config, ReactorPool::ProactorFactory proactor_factory)
    : config_(std::move(config)),
      topics_(config_.data_dir),
      proactor_factory_(proactor_factory ? std::move(proactor_factory) : make_io_uring_proactor) {
    if (config_.num_reactors <= 0) {
        config_.num_reactors = 1;
    }
    // Valida el plano de control en construcción: si io_uring no está disponible, la factoría por
    // defecto lanza aquí (no en el hilo del reactor), preservando el contrato de fallo previo.
    static_cast<void>(proactor_factory_(0));

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
    // Arranca el pool: workers 1..N-1 en sus hilos, núcleo 0 inline (lo corremos aquí). El plano de
    // datos vive en el núcleo 0 mientras no haya sharding (D3.4c): sin estado compartido entre
    // hilos.
    pool_.start_main_inline(config_.num_reactors, proactor_factory_);
    Reactor& main = pool_.reactor(0);
    main_reactor_.store(&main, std::memory_order_release);
    if (stop_requested_.load(std::memory_order_acquire)) {
        pool_.shutdown();  // `stop()` llegó durante el arranque: no servimos.
        return;
    }

    main.spawn(accept_loop(main, *listener_, *router_));
    if (admin_listener_ && admin_router_) {
        main.spawn(admin_accept_loop(main, *admin_listener_, *admin_router_));
    }
    health_.set_started(true);  // ya servimos: `/readyz` puede dar 200.
    main.run();                 // bloquea el hilo llamante hasta `stop()`.
    pool_.shutdown();           // para y une los workers (el núcleo 0 ya salió de su bucle).
    main_reactor_.store(nullptr, std::memory_order_release);
}

void Server::stop() noexcept {
    health_.set_live(false);  // drenando: `/healthz` da 503 para sacarnos del balanceador.
    stop_requested_.store(true, std::memory_order_release);
    // Despierta el núcleo 0 si `run` ya lo publicó (atómico + eventfd: async-signal-safe).
    if (Reactor* main = main_reactor_.load(std::memory_order_acquire); main != nullptr) {
        main->stop();
    }
}

}  // namespace nexus
