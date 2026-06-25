/// @file   server/server.cpp
/// @brief  Implementación del arranque del broker mono-nodo.
/// @ingroup server

#include "server/server.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "broker/consumer_group.hpp"
#include "broker/group_coordinator.hpp"
#include "cluster/raft_receiver.hpp"
#include "consensus/raft_carrier.hpp"
#include "io/io_uring_backend.hpp"
#include "reactor/cross_core_call.hpp"
#include "server/admin_http.hpp"
#include "server/connection.hpp"

namespace nexus {

namespace {

/// Profundidad de la cola del anillo io_uring de cada reactor del servidor.
constexpr unsigned int kRingDepth = 256;

/// Tamaño máximo de un sobre de Raft en el wire inter-nodo (anti-DoS): 16 MiB (holgado para
/// `AppendEntries`/`InstallSnapshot` con lotes grandes).
constexpr std::size_t kMaxRaftMessage = std::size_t{16} * 1024 * 1024;

/// Factoría por defecto del `Proactor` de cada reactor: io_uring (plano de control).
std::unique_ptr<Proactor> make_io_uring_proactor(int /*core_id*/) {
    return std::make_unique<IoUringBackend>(kRingDepth);
}

/// @brief Resuelve `num_reactors`: `<= 0` significa **auto** (todos los núcleos disponibles).
/// @details Se aplica **antes** de construir los catálogos (que se dimensionan con este valor), por
///   eso normaliza el `Config` en la lista de inicialización. `hardware_concurrency()` puede
///   devolver 0 si no lo detecta → mínimo 1.
Server::Config resolve_config(Server::Config config) {
    if (config.num_reactors <= 0) {
        const unsigned detected = std::thread::hardware_concurrency();
        config.num_reactors = detected > 0 ? static_cast<int>(detected) : 1;
    }
    return config;
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
    : config_(resolve_config(std::move(config))),
      peers_(config_.peers),
      catalog_(config_.data_dir, config_.num_reactors, config_.node_id, config_.raft_config,
               peers_.node_ids(), config_.compaction),
      group_catalog_(config_.num_reactors),
      proactor_factory_(proactor_factory ? std::move(proactor_factory) : make_io_uring_proactor) {
    // Valida el plano de control en construcción: si io_uring no está disponible, la factoría por
    // defecto lanza aquí (no en el hilo del reactor), preservando el contrato de fallo previo.
    static_cast<void>(proactor_factory_(0));

    const JwtVerifier* verifier = nullptr;
    if (!config_.jwt_secret.empty()) {
        verifier = &jwt_.emplace(config_.jwt_secret);
    }
    // `emplace` devuelve la referencia al objeto construido: la usamos directamente para no
    // desreferenciar el `optional` recién poblado (bugprone-unchecked-optional-access en tidy-18).
    // El admin (REST) opera sobre el manager del núcleo 0 para las lecturas locales (describe/list
    // de topics, con metadatos completos en cada núcleo); crear/borrar topic se propaga a todos los
    // núcleos por paso de mensajes una vez cableado (`bind_cluster`, ADR-0026).
    AdminApi& api = admin_api_.emplace(catalog_.manager(0), config_.node_id,
                                       [this](Page page) { return list_groups(page); });
    RestGateway& rest = rest_.emplace(api, verifier);
    admin_router_.emplace(rest, health_, metrics_);
    if (config_.min_free_disk_bytes > 0) {
        health_.register_readiness("disk",
                                   disk_space_probe(config_.data_dir, config_.min_free_disk_bytes));
    }
}

task<std::vector<GroupSummary>> Server::list_groups(Page page) {
    // Agrega el coordinador de cada núcleo: cada grupo se coordina en `hash(group_id) % N`, así que
    // el listado completo cruza núcleos (ADR-0026). El admin se sirve en el núcleo 0; con N=1 el
    // `call_on` es local e inline. Antes de `run` (monohilo) se lee directamente el núcleo 0.
    std::vector<GroupDigest> digests;
    if (partition_router_) {
        Reactor& self = *main_reactor_.load(std::memory_order_acquire);
        for (int core = 0; core < group_catalog_.core_count(); ++core) {
            std::vector<GroupDigest> shard = co_await call_on(
                self, partition_router_->reactor(core),
                [this, core] { return group_catalog_.groups(core).list_groups(); });
            digests.insert(digests.end(), std::make_move_iterator(shard.begin()),
                           std::make_move_iterator(shard.end()));
        }
    } else {
        digests = group_catalog_.groups(0).list_groups();
    }
    std::ranges::sort(digests, {}, &GroupDigest::group_id);  // orden global determinista.

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
        co_return std::vector<GroupSummary>{};
    }
    const std::size_t end = std::min(summaries.size(), offset + page.size);
    co_return std::vector<GroupSummary>{
        std::make_move_iterator(summaries.begin() + static_cast<std::ptrdiff_t>(offset)),
        std::make_move_iterator(summaries.begin() + static_cast<std::ptrdiff_t>(end))};
}

expected<void> Server::create_topic(const std::string& name, std::int32_t partition_count,
                                    std::int16_t replication_factor) {
    // Plano de control pre-run (monohilo): el catálogo replica el topic a todos los núcleos. Con
    // replication_factor > 1 cada núcleo crea sus particiones como ReplicatedPartition + portador.
    expected<TopicMetadata> meta =
        catalog_.create_topic(name, partition_count, {}, replication_factor);
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
    router_.emplace(catalog_.manager(0), config_.node_id, config_.advertised_host,
                    listener_->local_port());

    if (config_.admin_port) {
        expected<Listener> admin = Listener::bind(config_.host, *config_.admin_port);
        if (!admin) {
            return std::unexpected(admin.error());
        }
        admin_listener_ = std::move(*admin);
    }
    if (config_.cluster_port) {
        expected<Listener> cluster = Listener::bind(config_.host, *config_.cluster_port);
        if (!cluster) {
            return std::unexpected(cluster.error());
        }
        cluster_listener_ = std::move(*cluster);
    }
    return {};
}

std::uint16_t Server::port() const noexcept {
    return listener_ ? listener_->local_port() : 0;
}

std::uint16_t Server::admin_port() const noexcept {
    return admin_listener_ ? admin_listener_->local_port() : 0;
}

std::uint16_t Server::cluster_port() const noexcept {
    return cluster_listener_ ? cluster_listener_->local_port() : 0;
}

void Server::start_raft_ticks(Reactor& main) {
    const auto interval =
        std::max(config_.raft_config.heartbeat_interval, std::chrono::milliseconds{1});
    for (int core = 0; core < config_.num_reactors; ++core) {
        std::vector<RaftCarrier*> carriers = catalog_.manager(core).carriers();
        if (carriers.empty()) {
            continue;  // este núcleo no sirve particiones replicadas.
        }
        // El portador se conduce en el hilo de SU reactor (la FSM/log/producción de su partición
        // son reactor-locales): nada de tocar el RaftNode desde otro hilo.
        auto tick = [carriers = std::move(carriers)](MonoTime now) {
            for (RaftCarrier* carrier : carriers) {
                carrier->on_tick(now);
            }
        };
        Reactor& owner = pool_.reactor(core);
        if (core == 0) {
            owner.every(interval, std::move(tick));  // núcleo 0: este hilo, registro directo.
        } else {
            // Núcleos 1..N-1 corren en su propio hilo; registra el temporizador EN ese hilo por el
            // buzón (el conjunto de temporizadores del reactor no es thread-safe).
            main.submit_to(core, [&owner, interval, tick = std::move(tick)]() mutable {
                owner.every(interval, std::move(tick));
            });
        }
    }
}

void Server::start_raft_transport(Reactor& main) {
    if (!config_.cluster_port) {
        return;  // sin plano inter-nodo: los portadores quedan como votante único (sumidero nulo).
    }
    raft_transports_.resize(static_cast<std::size_t>(config_.num_reactors));
    for (int core = 0; core < config_.num_reactors; ++core) {
        Reactor& owner = pool_.reactor(core);
        // Crea el transporte del núcleo y lo instala como sumidero de sus portadores, EN su hilo
        // (el transporte y el sumidero diferido son reactor-locales). El transporte emite por
        // corrutinas lanzadas en el propio reactor (spawner).
        auto setup = [this, core, &owner]() {
            auto transport = std::make_unique<RaftTransport>(
                config_.node_id, peers_, owner.proactor(),
                [&owner](task<void> coro) { owner.spawn(std::move(coro)); });
            catalog_.manager(core).set_message_sink(transport.get());
            raft_transports_[static_cast<std::size_t>(core)] = std::move(transport);
        };
        if (core == 0) {
            setup();
        } else {
            main.submit_to(core, std::move(setup));
        }
    }
}

void Server::dispatch_raft_envelope(Reactor& source, const RaftEnvelope& envelope) {
    // El dueño de la partición (ADR-0026) recibe el RPC en SU hilo: lookup del portador +
    // on_message son reactor-locales. Si el dueño es el propio núcleo receptor, se entrega directo.
    const int owner = static_cast<int>(static_cast<std::size_t>(envelope.partition) %
                                       static_cast<std::size_t>(config_.num_reactors));
    auto deliver = [this, owner, env = envelope]() {
        RaftCarrier* carrier = catalog_.manager(owner).carrier_for(env.topic, env.partition);
        if (carrier != nullptr) {
            carrier->on_message(std::chrono::steady_clock::now(), env.message);
        }
    };
    if (owner == source.core_id()) {
        deliver();
    } else {
        source.submit_to(owner, std::move(deliver));
    }
}

task<void> Server::cluster_accept_loop(Reactor& reactor, Listener& listener) {
    while (true) {
        expected<Socket> client = co_await listener.async_accept(reactor.proactor());
        if (!client) {
            co_return;  // listener cerrado o error: dejamos de aceptar.
        }
        reactor.spawn(serve_raft_connection(
            reactor.proactor(), std::move(*client),
            [this, &reactor](const RaftEnvelope& env) { dispatch_raft_envelope(reactor, env); },
            kMaxRaftMessage));
    }
}

void Server::run() {
    if (!listener_ || !router_) {
        return;  // Precondición: `bind()` antes de `run()`; sin listener no hay nada que servir.
    }
    // Arranca el pool: workers 1..N-1 en sus hilos, núcleo 0 inline (lo corremos aquí). Las
    // conexiones se aceptan en el núcleo 0; cada operación de partición se enruta a su reactor
    // dueño (sharding ADR-0026), que opera sobre el `TopicManager` de ese núcleo sin compartir
    // estado.
    pool_.start_main_inline(config_.num_reactors, proactor_factory_);
    Reactor& main = pool_.reactor(0);
    main_reactor_.store(&main, std::memory_order_release);
    if (stop_requested_.load(std::memory_order_acquire)) {
        pool_.shutdown();  // `stop()` llegó durante el arranque: no servimos.
        return;
    }

    // Cablea el enrutado por partición (ADR-0026). En N=1 el dueño es el propio núcleo 0 y el
    // fast-path de `call_on` ejecuta inline; con N>1 cada partición va a su reactor dueño, que
    // opera sobre el `TopicManager` de ese núcleo (uno por núcleo, vía el catálogo fragmentado).
    std::vector<Reactor*> reactors;
    reactors.reserve(static_cast<std::size_t>(config_.num_reactors));
    for (int core = 0; core < config_.num_reactors; ++core) {
        reactors.push_back(&pool_.reactor(core));
    }
    partition_router_.emplace(std::move(reactors));
    router_->bind_cluster(main, *partition_router_, catalog_.managers(),
                          group_catalog_.all_groups(), group_catalog_.all_offsets());
    if (admin_api_) {
        // El admin REST también propaga crear/borrar topic a todos los núcleos (ADR-0026).
        admin_api_->bind_cluster(main, *partition_router_, catalog_.managers());
    }
    // El transporte se instala ANTES de los ticks para que la primera elección/heartbeat ya salga
    // por la red a los peers (ADR-0025).
    start_raft_transport(main);
    start_raft_ticks(main);

    main.spawn(accept_loop(main, *listener_, *router_));
    if (admin_listener_ && admin_router_) {
        main.spawn(admin_accept_loop(main, *admin_listener_, *admin_router_));
    }
    if (cluster_listener_) {
        main.spawn(cluster_accept_loop(main, *cluster_listener_));
    }
    health_.set_started(true);  // ya servimos: `/readyz` puede dar 200.
    main.run();                 // bloquea el hilo llamante hasta `stop()`.
    main_reactor_.store(nullptr, std::memory_order_release);
    // El apagado del pool (stop + join de workers + cierre de los proactors) NO se hace aquí: lo
    // hace `~ReactorPool` cuando se destruye el `Server`, en el hilo que lo destruye y DESPUÉS de
    // que el llamante haya unido el hilo de `run()`. Si se cerrara aquí (en este hilo), el
    // `close(eventfd)` competiría con el `write(eventfd)` de `stop()→wake()` del otro hilo (race
    // detectada por TSan). El `join` del llamante establece el happens-before que lo ordena.
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
