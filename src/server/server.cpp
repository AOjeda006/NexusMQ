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
#include "broker/partition_base.hpp"
#include "cluster/raft_receiver.hpp"
#include "common/fnv1a.hpp"
#include "consensus/raft_carrier.hpp"
#if defined(_WIN32)
#include "io/iocp_backend.hpp"  // factoría de proactor en Windows (ADR-0023/0028)
#else
#include "io/io_uring_backend.hpp"
#endif
#include "reactor/cross_core_call.hpp"
#include "server/admin_http.hpp"
#include "server/connection.hpp"
#include "server/kafka_connection.hpp"

namespace nexus {

// Declaración adelantada para el parámetro de `accept_loop` cuando se compila **sin** OpenSSL (sin
// `ingress/tls.hpp`): el puntero es siempre `nullptr` y nunca se desreferencia. Con OpenSSL, el
// tipo ya viene completo desde la cabecera; esto es una redeclaración inocua.
class TlsContext;

namespace {

/// Profundidad de la cola del anillo io_uring de cada reactor del servidor.
constexpr unsigned int kRingDepth = 256;

/// Tamaño máximo de un sobre de Raft en el wire inter-nodo (anti-DoS): 16 MiB (holgado para
/// `AppendEntries`/`InstallSnapshot` con lotes grandes).
constexpr std::size_t kMaxRaftMessage = std::size_t{16} * 1024 * 1024;

/// Factoría por defecto del `Proactor` de cada reactor (plano de control): io_uring en Linux, IOCP
/// en Windows. Ambos son proactores sobre el mismo puerto `Proactor` (ADR-0023), así que el bucle
/// del reactor no cambia; la elección es en compilación (ADR-0028).
std::unique_ptr<Proactor> make_default_proactor(int /*core_id*/) {
#if defined(_WIN32)
    return std::make_unique<IocpBackend>(kRingDepth);
#else
    return std::make_unique<IoUringBackend>(kRingDepth);
#endif
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

/// @brief Construye el tier local (ADR-0032) si `tier_dir` no está vacío; si no, `nullptr`.
[[nodiscard]] std::unique_ptr<LocalStorageTier> make_tier(const Server::Config& config) {
    if (config.tier_dir.empty()) {
        return nullptr;  // sin tiering (comportamiento por defecto).
    }
    return std::make_unique<LocalStorageTier>(config.tier_dir);
}

#ifdef NEXUS_HAVE_OPENSSL
/// @brief Sirve una conexión **cifrada**: envuelve el socket aceptado en una `TlsConnection`,
///   completa el handshake TLS y entra en el bucle de servicio sobre el flujo cifrado.
/// @details Un fallo (recurso SSL, verificación de certificado/mTLS, o EOF durante el handshake)
///   cierra la conexión sin servir (RAII cierra socket y SSL). Solo se compila con OpenSSL.
task<void> serve_tls_connection(Proactor& proactor, const TlsContext& tls, Socket sock,
                                RequestRouter& router) {
    expected<TlsConnection> conn = tls.accept(std::move(sock));
    if (!conn) {
        co_return;  // no se pudo crear el objeto SSL: cerramos.
    }
    if (const expected<void> handshaken = co_await conn->handshake(proactor); !handshaken) {
        co_return;  // handshake fallido (verificación/transporte): cerramos.
    }
    co_await serve_connection(proactor, std::move(*conn), router);
}
#endif

/// Bucle de aceptación del plano de datos: una corrutina de servicio por conexión. Si @p tls no es
/// nulo, cada conexión se termina con TLS antes de servirla (ADR-0019); si es nulo, texto plano.
task<void> accept_loop(Reactor& reactor, Listener& listener, RequestRouter& router,
                       [[maybe_unused]] const TlsContext* tls) {
    while (true) {
        expected<Socket> client = co_await listener.async_accept(reactor.proactor());
        if (!client) {
            co_return;  // listener cerrado o error: dejamos de aceptar.
        }
        client->set_nodelay(true);  // plano de datos petición/respuesta: sin Nagle (baja latencia).
#ifdef NEXUS_HAVE_OPENSSL
        if (tls != nullptr) {
            reactor.spawn(
                serve_tls_connection(reactor.proactor(), *tls, std::move(*client), router));
            continue;
        }
#endif
        reactor.spawn(serve_connection(reactor.proactor(), std::move(*client), router));
    }
}

/// @brief Sirve una conexión en **modo proxy** (ADR-0006/0027): elige un nodo aguas arriba, obtiene
///   una conexión del pool y releva las tramas del cliente a ese nodo hasta que el cliente cierra.
/// @details Al terminar limpiamente el relevo, la conexión aguas arriba se **devuelve al pool**
/// para
///   reúso; si el relevo falló (el nodo cayó a media operación), se descarta (se cierra al
///   destruirse). Sin nodos en el anillo o con fallo de dialado, la conexión de cliente se cierra.
///   @p conn_id es la clave de *consistent-hashing* (identidad de la conexión: enrutado estable por
///   conexión, coherente con el relevo a nivel de conexión de I18).
task<void> serve_proxy_connection(Proactor& proactor, Proxy& proxy, UpstreamPool& pool,
                                  Socket client, std::uint64_t conn_id) {
    const std::optional<NodeId> node = proxy.route(std::to_string(conn_id));
    if (!node) {
        co_return;  // sin nodos aguas arriba en el anillo: cerramos.
    }
    expected<Socket> upstream = co_await pool.acquire(proactor, *node);
    if (!upstream) {
        co_return;  // no se pudo dialar el nodo: cerramos.
    }
    // El plano de datos es petición/respuesta de baja latencia: desactiva Nagle en ambos extremos
    // del relevo para no acumular el clásico interbloqueo Nagle/delayed-ACK al partir cada trama en
    // cabecera + payload (normativa de redes: minimizar el impacto del RTT).
    client.set_nodelay(true);
    upstream->set_nodelay(true);
    const expected<void> relayed = co_await proxy.forward(proactor, client, *upstream);
    if (relayed) {
        pool.release(*node, std::move(*upstream));  // conexión sana: la devolvemos para reúso.
    }
}

/// Bucle de aceptación del plano de datos en **modo proxy**: una corrutina de relevo por conexión,
/// con una clave de enrutado monótona por conexión (consistent-hashing del nodo aguas arriba).
task<void> proxy_accept_loop(Reactor& reactor, Listener& listener, Proxy& proxy,
                             UpstreamPool& pool) {
    std::uint64_t next_conn_id = 0;
    while (true) {
        expected<Socket> client = co_await listener.async_accept(reactor.proactor());
        if (!client) {
            co_return;  // listener cerrado o error: dejamos de aceptar.
        }
        reactor.spawn(serve_proxy_connection(reactor.proactor(), proxy, pool, std::move(*client),
                                             next_conn_id++));
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

/// Bucle de aceptación del listener Kafka: una corrutina de servicio por conexión (F7f).
task<void> kafka_accept_loop(Reactor& reactor, Listener& listener, kafka::KafkaGateway& gateway) {
    while (true) {
        expected<Socket> client = co_await listener.async_accept(reactor.proactor());
        if (!client) {
            co_return;  // listener cerrado o error: dejamos de aceptar.
        }
        client->set_nodelay(true);  // petición/respuesta de baja latencia: sin Nagle.
        reactor.spawn(serve_kafka_connection(reactor.proactor(), std::move(*client), gateway));
    }
}

}  // namespace

Server::Server(Config config, ReactorPool::ProactorFactory proactor_factory)
    : config_(resolve_config(std::move(config))),
      peers_(config_.peers),
      tier_(make_tier(config_)),
      catalog_(config_.data_dir, config_.num_reactors, config_.node_id, config_.raft_config,
               peers_.node_ids(), config_.compaction, config_.encryption_key, tier_.get()),
      group_catalog_(config_.num_reactors),
      proactor_factory_(proactor_factory ? std::move(proactor_factory) : make_default_proactor) {
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
    AdminApi& api = admin_api_.emplace(
        catalog_.manager(0), config_.node_id, [this](Page page) { return list_groups(page); },
        [this](std::string group_id) { return describe_group(std::move(group_id)); });
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

task<std::int64_t> Server::partition_high_watermark(std::string topic, PartitionId pid) {
    const auto cores = static_cast<std::size_t>(config_.num_reactors);
    const int owner = static_cast<int>(static_cast<std::size_t>(pid) % cores);
    auto read = [this, owner, topic = std::move(topic), pid]() -> std::int64_t {
        Topic* found = catalog_.manager(owner).get(topic);
        if (found == nullptr) {
            return 0;
        }
        const PartitionBase* part = found->partition(pid);
        return part == nullptr ? 0 : part->high_watermark();
    };
    if (partition_router_) {
        Reactor& self = *main_reactor_.load(std::memory_order_acquire);
        co_return co_await call_on(self, partition_router_->reactor(owner), std::move(read));
    }
    co_return read();
}

task<expected<GroupDescription>> Server::describe_group(std::string group_id) {
    // El grupo vive en un único núcleo coordinador (`fnv1a_64(group_id) % N`, ADR-0026): se lee ahí
    // (más barato que agregar). El admin se sirve en el núcleo 0; con N=1 el `call_on` es local.
    const auto cores = static_cast<std::uint64_t>(group_catalog_.core_count());
    const auto owner = static_cast<int>(fnv1a_64(group_id) % cores);

    // Membresía + offsets confirmados del grupo, leídos en su núcleo coordinador (reactor-local).
    struct GroupData {
        bool found = false;
        std::string state;
        std::int32_t generation = 0;
        std::string leader_id;
        std::vector<GroupMemberInfo> members;
        std::vector<GroupOffsetEntry> offsets;
    };
    auto read_group = [this, owner, group_id]() -> GroupData {
        GroupData data;
        ConsumerGroup* group = group_catalog_.groups(owner).find(group_id);
        if (group == nullptr) {
            return data;  // grupo inexistente en su núcleo coordinador.
        }
        data.found = true;
        data.state = std::string{group_state_name(group->state())};
        data.generation = group->generation();
        data.leader_id = group->leader_id();
        for (const MemberInfo& member : group->members()) {
            data.members.push_back(GroupMemberInfo{
                .member_id = member.member_id,
                .subscription_bytes = static_cast<std::int64_t>(member.subscription.size())});
        }
        data.offsets = group_catalog_.offsets(owner).list_for_group(group_id);
        return data;
    };

    GroupData data;
    if (partition_router_) {
        Reactor& self = *main_reactor_.load(std::memory_order_acquire);
        data = co_await call_on(self, partition_router_->reactor(owner), read_group);
    } else {
        data = read_group();
    }
    if (!data.found) {
        co_return make_error(ErrorCode::NotFound, "grupo inexistente: " + group_id);
    }

    GroupDescription description;
    description.group_id = group_id;
    description.state = std::move(data.state);
    description.generation = data.generation;
    description.leader_id = std::move(data.leader_id);
    description.members = std::move(data.members);
    // Enriquece cada offset con el high-watermark de su partición (dueño = `partition % N`) → lag.
    for (const GroupOffsetEntry& entry : data.offsets) {
        const std::int64_t hwm = co_await partition_high_watermark(entry.topic, entry.partition);
        const std::int64_t lag = hwm > entry.offset ? hwm - entry.offset : 0;
        description.offsets.push_back(GroupPartitionOffset{.topic = entry.topic,
                                                           .partition = entry.partition,
                                                           .committed_offset = entry.offset,
                                                           .high_watermark = hwm,
                                                           .lag = lag});
    }
    co_return description;
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
    // Cablea las métricas del plano de datos (produce/fetch) al registro que expone `/metrics`
    // (ADR-0017). El router sirve en el núcleo 0, así que registra sin contención.
    router_->set_metrics(metrics_);
    // Cablea las métricas del plano de replicación (Raft) a los portadores de cada núcleo. Pre-run
    // (monohilo): los portadores creados al arrancar se cablean ya; los creados en runtime se
    // autocablean en su hilo dueño (guardamos el registro en cada TopicManager).
    for (int core = 0; core < config_.num_reactors; ++core) {
        catalog_.manager(core).set_metrics(metrics_);
    }

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
    if (config_.kafka_port) {
        expected<Listener> kafka = Listener::bind(config_.host, *config_.kafka_port);
        if (!kafka) {
            return std::unexpected(kafka.error());
        }
        kafka_listener_ = std::move(*kafka);
    }
#ifdef NEXUS_HAVE_OPENSSL
    // Termina el plano de datos con TLS si hay certificado/clave (y mTLS si hay CA de cliente). Se
    // valida en el borde (carga de PEM) antes de servir: un cert/clave inválido falla aquí.
    if (config_.tls.enabled()) {
        expected<TlsContext> ctx = TlsContext::server(
            config_.tls.cert_chain, config_.tls.private_key, config_.tls.client_ca);
        if (!ctx) {
            return std::unexpected(ctx.error());
        }
        tls_.emplace(std::move(*ctx));
    }
#endif
    // Modo proxy (opt-in, ADR-0006/0027): si hay nodos aguas arriba, construye el directorio del
    // plano de datos, el balanceador (con todos los nodos en el anillo), el pool de conexiones y el
    // proxy. El relevo lo sirve el bucle de aceptación del núcleo 0 (`proxy_accept_loop`).
    if (config_.proxy.enabled()) {
        upstream_dir_.emplace(config_.proxy.upstreams);
        LoadBalancer& balancer = balancer_.emplace(config_.proxy.strategy);
        for (const NodeId node : upstream_dir_->node_ids()) {
            balancer.add_node(node);
        }
        upstream_pool_.emplace(*upstream_dir_);
        proxy_.emplace(balancer);
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

std::uint16_t Server::kafka_port() const noexcept {
    return kafka_listener_ ? kafka_listener_->local_port() : 0;
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

    // listener_/router_ siempre presentes en start(); la guarda satisface a clang-tidy y es más
    // segura que derefar el `optional` a ciegas.
    if (listener_ && router_) {
        if (proxy_ && upstream_pool_) {
            // Modo proxy (opt-in, ADR-0006/0027): el núcleo 0 releva cada conexión a un nodo aguas
            // arriba en vez de servirla local. El relevo es en claro (no se cablea TLS en proxy).
            main.spawn(proxy_accept_loop(main, *listener_, *proxy_, *upstream_pool_));
        } else {
            const TlsContext* tls_ptr = nullptr;
#ifdef NEXUS_HAVE_OPENSSL
            if (tls_) {
                tls_ptr = &*tls_;  // contexto TLS compartido por el bucle de aceptación (núcleo 0).
            }
#endif
            main.spawn(accept_loop(main, *listener_, *router_, tls_ptr));
        }
    }
    if (admin_listener_ && admin_router_) {
        main.spawn(admin_accept_loop(main, *admin_listener_, *admin_router_));
    }
    if (cluster_listener_) {
        main.spawn(cluster_accept_loop(main, *cluster_listener_));
    }
    if (kafka_listener_ && partition_router_) {
        // Adaptador Kafka sobre el broker real (F7f): opera sobre el `TopicManager` del núcleo 0 y
        // reparte cada partición a su reactor dueño (ADR-0026), igual que el router nativo. Anuncia
        // en Metadata el host configurado y el puerto Kafka realmente enlazado (los clientes se
        // reconectan ahí para produce/fetch).
        kafka_broker_.emplace(catalog_.manager(0), config_.node_id, config_.advertised_host,
                              kafka_listener_->local_port());
        kafka_broker_->bind_cluster(main, *partition_router_, catalog_.managers());
        kafka_broker_->set_metrics(metrics_);  // P5e: métricas Kafka con protocol="kafka".
        kafka_gateway_.emplace(*kafka_broker_);
        main.spawn(kafka_accept_loop(main, *kafka_listener_, *kafka_gateway_));
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
