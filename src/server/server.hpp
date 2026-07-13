/// @file   server/server.hpp
/// @brief  Server: arranque del broker mono-nodo (reactor + listener + bucle de aceptación).
/// @ingroup server

#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "broker/group_catalog.hpp"
#include "broker/request_router.hpp"
#include "broker/topic_catalog.hpp"
#include "broker/topic_manager.hpp"
#include "cluster/peer_directory.hpp"
#include "cluster/raft_transport.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "consensus/raft_node.hpp"
#include "consensus/raft_wire.hpp"
#include "ingress/health_monitor.hpp"
#include "ingress/jwt.hpp"
#include "ingress/load_balancer.hpp"  // BalanceStrategy: estrategia del modo proxy (ADR-0006)
#include "ingress/proxy.hpp"          // Proxy: enrutado + relevo de tramas del modo proxy
#include "ingress/rest_gateway.hpp"
#include "ingress/upstream_pool.hpp"  // UpstreamPool: dial/reúso de conexiones aguas arriba (ADR-0027)
#ifdef NEXUS_HAVE_OPENSSL
#include "ingress/tls.hpp"  // TlsContext: terminación TLS del plano de datos (ADR-0019)
#endif
#include "io/socket.hpp"
#include "kafka/gateway.hpp"
#include "reactor/partition_router.hpp"
#include "reactor/reactor.hpp"
#include "reactor/reactor_pool.hpp"
#include "server/admin_api.hpp"
#include "server/admin_router.hpp"
#include "server/kafka_adapter.hpp"
#include "storage/local_storage_tier.hpp"  // LocalStorageTier: tier local de segmentos (ADR-0032)
#include "telemetry/metrics.hpp"

namespace nexus {

/// @brief Daemon del broker en un nodo, sobre un `ReactorPool` *thread-per-core* (ADR-0025).
///   Afinidad: `run` corre el **núcleo 0** en el hilo llamante (los demás en sus hilos); `stop` es
///   seguro desde otro hilo (incluido un *signal handler*).
/// @details Orquesta `TopicManager` + `RequestRouter` + un `ReactorPool` (io_uring) + un
/// `Listener`.
///   `bind` enlaza el puerto (plano de control), `run` arranca el pool (`start_main_inline`:
///   workers 1..N-1 en hilos, núcleo 0 inline), lanza el bucle de aceptación en el núcleo 0 y lo
///   corre (bloquea), `stop` lo despierta para salir y une el pool. Las conexiones se aceptan en el
///   núcleo 0; cada partición se enruta a su reactor dueño (`partition % N`) y cada grupo a su
///   núcleo coordinador (`hash(group_id) % N`) por paso de mensajes (sharding *shared-nothing*,
///   ADR-0026). `num_reactors` fija N (por defecto **auto** = todos los núcleos; fija un valor para
///   acotarlo —p. ej. 1 en pruebas deterministas—).
class Server {
public:
    struct Config {
        /// Interfaz de escucha (IPv4 punteada; vacío = todas).
        std::string host = "0.0.0.0";
        /// Puerto del plano de datos (protocolo binario). 0 = efímero, útil en tests.
        std::uint16_t port = 0;
        /// Raíz de los logs de partición.
        std::filesystem::path data_dir;
        /// Identidad del nodo (para metadata).
        NodeId node_id = 0;
        /// Host anunciado en metadata.
        std::string advertised_host = "127.0.0.1";
        /// Puerto del **plano de operación** (REST admin + /metrics + health). `nullopt` lo
        /// desactiva; `0` = efímero.
        std::optional<std::uint16_t> admin_port;
        /// Puerto del **listener compatible con Kafka** (subconjunto F7, separado del nativo).
        /// `nullopt` lo desactiva; `0` = efímero (útil en tests). Habla el wire de Kafka
        /// (`Size:INT32` + APIs ApiVersions/Metadata/ListOffsets/Produce/Fetch) con
        /// `kcat`/librdkafka.
        std::optional<std::uint16_t> kafka_port;
        /// Secreto HMAC del JWT que protege el REST admin. Vacío = sin autenticación.
        std::string jwt_secret;
        /// Mínimo de espacio libre en disco (bytes) para `/readyz`. `0` = sin chequeo de disco.
        std::uintmax_t min_free_disk_bytes = 0;
        /// Número de reactores del pool (uno por núcleo). `<= 0` = **auto** (todos los núcleos
        /// disponibles, `hardware_concurrency()`); fija un valor explícito para acotarlo.
        int num_reactors = 0;
        /// Parámetros de Raft de las particiones replicadas (`replication_factor > 1`): timeouts de
        /// elección/heartbeat y semilla. El periodo de tick del reactor sale del
        /// `heartbeat_interval`.
        RaftConfig raft_config;
        /// Puerto del **plano inter-nodo** (Raft entre nodos, ADR-0025). `nullopt` lo desactiva
        /// (nodo aislado: votante único); `0` = efímero (útil en tests).
        std::optional<std::uint16_t> cluster_port;
        /// Directorio de peers del clúster (`NodeId` -> dirección inter-nodo) para el transporte de
        /// Raft. Suele excluir a este nodo.
        PeerDirectory peers;
        /// @brief Configuración TLS del **plano de datos** (protocolo binario; ADR-0019).
        /// @details Si `cert_chain` y `private_key` están definidos **y** el broker se compiló con
        ///   OpenSSL, el plano de datos exige **TLS 1.3**; si además `client_ca` está definido,
        ///   exige **mTLS** (verifica el certificado del cliente contra esa CA). Rutas vacías —o un
        ///   build sin OpenSSL— dejan el plano en **texto plano**.
        struct TlsConfig {
            std::filesystem::path cert_chain;   ///< Cadena de certificado del servidor (PEM).
            std::filesystem::path private_key;  ///< Clave privada del servidor (PEM).
            std::filesystem::path client_ca;    ///< CA del cert de cliente (PEM), para mTLS.

            /// @brief ¿Se ha solicitado TLS? (certificado y clave presentes).
            [[nodiscard]] bool enabled() const noexcept {
                return !cert_chain.empty() && !private_key.empty();
            }
        };
        /// Configuración TLS del plano de datos (ADR-0019); ver `TlsConfig`.
        TlsConfig tls;
        /// @brief KEK de **cifrado en reposo** en hexadecimal (64 dígitos = 256 bits), como
        ///   **entrada** del *composition root* (ADR-0031).
        /// @details Si no está vacía, el `main` la resuelve en `encryption_key`. Preferir la
        /// variable
        ///   de entorno `NEXUS_ENCRYPTION_KEY` frente al flag `--encryption-key`: el flag deja la
        ///   clave visible en la línea de comandos (`ps`), la variable de entorno no.
        std::string encryption_key_hex;
        /// @brief KEK de cifrado en reposo **resuelta** (ADR-0031); la construye el *composition
        ///   root* desde `encryption_key_hex`/entorno.
        /// @details Si no es nula **y** el broker se compiló con OpenSSL, los logs de partición se
        ///   escriben cifrados con AES-256-GCM; nula (o build sin OpenSSL) = en claro (por
        ///   defecto).
        std::shared_ptr<const EncryptionKey> encryption_key;
        /// @brief Directorio raíz del **almacenamiento por niveles** (tiered storage, ADR-0032).
        /// @details Si no está vacío, el broker descarga los segmentos sellados a un `object dir`
        ///   local bajo esta raíz (`LocalStorageTier`) y los rehidrata de forma transparente al
        ///   leer; vacío = sin tiering (por defecto). Interopera con el cifrado (sube el ciphertext
        ///   tal cual). Un tier de objetos (S3) sería un adaptador futuro del mismo puerto.
        std::filesystem::path tier_dir;
        /// @brief Configuración del **modo proxy** del plano de datos (ADR-0006/0027).
        /// @details Modo **secundario y opt-in**: si `upstreams` no está vacío, el servidor
        /// **releva**
        ///   las tramas de cada cliente a un nodo aguas arriba elegido con `strategy` (consistent-
        ///   hashing por defecto), sobre conexiones reutilizables (`UpstreamPool`). Vacío = modo
        ///   nativo directo (el servidor atiende localmente). En este corte, el relevo es **en
        ///   claro** (terminar TLS y relevar es trabajo futuro): si se activa el proxy, se ignora
        ///   `tls`.
        struct ProxyConfig {
            /// Direcciones del **plano de datos** de los nodos aguas arriba (`NodeId →
            /// host:puerto`).
            std::unordered_map<NodeId, PeerAddress> upstreams;
            /// Estrategia de balanceo para elegir el nodo (ADR-0006); por defecto
            /// consistent-hashing.
            BalanceStrategy strategy = BalanceStrategy::ConsistentHashing;

            /// @brief ¿Está activo el modo proxy? (hay al menos un nodo aguas arriba).
            [[nodiscard]] bool enabled() const noexcept { return !upstreams.empty(); }
        };
        /// Configuración del modo proxy del plano de datos (ADR-0006/0027); ver `ProxyConfig`.
        ProxyConfig proxy;
        /// Umbral por defecto de compactación del log de Raft: entradas aplicadas por encima del
        /// snapshot que disparan `compact_to` en el portador (ADR-0024).
        static constexpr Index kDefaultCompactionThreshold = 10000;
        /// Política de compactación del log de Raft de las particiones replicadas (ADR-0024/0025):
        /// el portador dispara `compact_to(commit_index)` al superar el umbral de entradas
        /// aplicadas. Activa por defecto en el servidor vivo; umbral 0 la desactiva.
        CompactionPolicy compaction{.applied_entries_threshold = kDefaultCompactionThreshold};
    };

    /// @brief Crea las piezas del broker y **valida** que el proactor (io_uring) se puede crear.
    /// @param config Configuración del nodo.
    /// @param proactor_factory Factoría del `Proactor` de cada reactor (DIP, testable). Vacía =
    ///   io_uring por defecto. Se valida construyendo un proactor de prueba en el constructor.
    /// @throws std::system_error si io_uring no está disponible (plano de control).
    explicit Server(Config config, ReactorPool::ProactorFactory proactor_factory = {});
    ~Server() = default;
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) = delete;
    Server& operator=(Server&&) = delete;

    /// @brief Crea un topic con @p partition_count particiones (plano de control; antes de servir).
    /// @param replication_factor `> 1` crea particiones replicadas por Raft (con portador); `1` =
    ///   mono-nodo (`Partition`).
    [[nodiscard]] expected<void> create_topic(const std::string& name, std::int32_t partition_count,
                                              std::int16_t replication_factor = 1);

    /// Enlaza el listener y prepara el router con el puerto efectivo. Idempotente-no: llamar una
    /// vez.
    [[nodiscard]] expected<void> bind();

    /// Puerto realmente enlazado (útil con puerto efímero). `0` si no se ha llamado a `bind`.
    [[nodiscard]] std::uint16_t port() const noexcept;

    /// Puerto del plano de operación realmente enlazado; `0` si está desactivado o sin `bind`.
    [[nodiscard]] std::uint16_t admin_port() const noexcept;

    /// Puerto del listener Kafka realmente enlazado; `0` si está desactivado o sin `bind`.
    [[nodiscard]] std::uint16_t kafka_port() const noexcept;

    /// Puerto del plano inter-nodo realmente enlazado; `0` si está desactivado o sin `bind`.
    [[nodiscard]] std::uint16_t cluster_port() const noexcept;

    /// @brief Reemplaza el directorio de peers del plano inter-nodo (Raft).
    /// @details Permite fijar las **direcciones** de los peers **después** de `bind()`, cuando los
    ///   puertos inter-nodo efímeros (`cluster_port = 0`) ya se conocen (descubrimiento de puertos
    ///   en arranque, tests de clúster). La **membresía de votantes** se fijó al construir
    ///   (`peers.node_ids()`); este método solo debería cambiar direcciones, no el conjunto de
    ///   `NodeId`. Tras `run()` el directorio es compartido e inmutable entre reactores: no lo
    ///   llames con el plano inter-nodo en marcha.
    /// @pre Antes de `run()` (el transporte lee el directorio al arrancar), desde un solo hilo.
    void set_peers(PeerDirectory peers) noexcept { peers_ = std::move(peers); }

    /// Lanza el bucle de aceptación y corre el reactor hasta `stop()` (bloquea el hilo llamante).
    void run();

    /// Solicita el apagado (seguro desde otro hilo / *signal handler*: solo despierta el reactor).
    void stop() noexcept;

private:
    /// @brief Enumera los grupos de **todos** los núcleos (cada uno coordina los suyos,
    ///   `hash(group_id) % N`), traducidos a DTOs, ordenados por id y paginados (para el
    ///   `AdminApi`).
    /// @details Corrutina: agrega con `call_on` sobre cada núcleo desde el núcleo 0 (donde se sirve
    ///   el admin); con un solo núcleo el `call_on` es local e inline.
    [[nodiscard]] task<std::vector<GroupSummary>> list_groups(Page page);

    /// @brief Describe un grupo (miembros/offsets/lag) para el `AdminApi`. **Reactor-local:** lee su
    ///   **único** núcleo coordinador (`fnv1a_64(group_id) % N`, ADR-0026); el *high-watermark* de
    ///   cada partición se lee de su núcleo dueño (`partition % N`) para el *lag*.
    /// @details Corrutina: `call_on` al núcleo del grupo (membresía + offsets) y luego a cada núcleo
    ///   dueño de partición (high-watermark). Con N=1 los `call_on` son locales e inline. Pre-`run`
    ///   (monohilo) lee directamente el núcleo 0.
    [[nodiscard]] task<expected<GroupDescription>> describe_group(std::string group_id);

    /// @brief *High-watermark* de la partición @p pid de @p topic, leído en su núcleo dueño
    ///   (`pid % N`); `0` si el topic/partición no vive ahí. Para el cálculo de *lag* del grupo.
    [[nodiscard]] task<std::int64_t> partition_high_watermark(std::string topic, PartitionId pid);

    /// @brief Registra en cada reactor dueño un temporizador que conduce (`on_tick`) los portadores
    ///   Raft de sus particiones replicadas (cadencia = `raft_config.heartbeat_interval`).
    /// @details El núcleo 0 se registra inline (este hilo); los núcleos 1..N-1 reciben el registro
    ///   por su buzón (el conjunto de temporizadores es reactor-local, no thread-safe). Lo llama
    ///   `run` tras arrancar el pool. @p main es el reactor del núcleo 0.
    void start_raft_ticks(Reactor& main);

    /// @brief Crea el `RaftTransport` de cada núcleo (sobre su `Proactor`) y lo instala como
    /// sumidero
    ///   de los portadores de ese núcleo, para que la replicación salga por la red a los peers.
    /// @details Núcleo 0 inline; núcleos 1..N-1 por su buzón (el transporte y el sumidero son
    ///   reactor-locales). No-op si no hay plano inter-nodo (`cluster_port` sin fijar). @p main es
    ///   el reactor del núcleo 0.
    void start_raft_transport(Reactor& main);

    /// @brief Bucle de aceptación del plano inter-nodo: por cada conexión, sirve sus sobres de Raft
    ///   y los enruta al portador dueño. Corre en el núcleo 0.
    [[nodiscard]] task<void> cluster_accept_loop(Reactor& reactor, Listener& listener);

    /// @brief Enruta un `RaftEnvelope` recibido al `RaftCarrier` dueño de su partición
    ///   (`partition % N`), entregándoselo con `on_message` en el hilo de ese núcleo.
    /// @param[in,out] source Reactor que recibió el sobre (desde el que se hace el `submit_to`).
    void dispatch_raft_envelope(Reactor& source, const RaftEnvelope& envelope);

    Config config_;
    /// Directorio de peers del clúster (copia de `config_.peers`); lo referencian los transportes,
    /// así que se declara **antes** que ellos (se destruye después).
    PeerDirectory peers_;
    /// Transporte de Raft por núcleo (creado en `run`, sobre el `Proactor` de cada reactor).
    /// Declarado **antes** que `pool_` y `catalog_` para destruirse **después** (las corrutinas
    /// emisoras del pool y los portadores del catálogo lo referencian).
    std::vector<std::unique_ptr<RaftTransport>> raft_transports_;
    /// Almacén por niveles (ADR-0032): tier local de segmentos, poblado si `config.tier_dir` no
    /// está vacío (`nullptr` = sin tiering). Declarado **antes** que `catalog_` para destruirse
    /// **después**: los `PartitionLog` del catálogo guardan un `StorageTier*` no-propietario a él.
    std::unique_ptr<LocalStorageTier> tier_;
    /// Catálogo de topics fragmentado por núcleo (ADR-0026): un `TopicManager` por reactor. El del
    /// núcleo 0 atiende las conexiones; el plano de datos enruta cada partición a su dueño.
    TopicCatalog catalog_;
    /// Coordinación de grupos fragmentada por núcleo (ADR-0026): cada grupo se coordina en el
    /// núcleo `hash(group_id) % N`, donde viven su membresía y sus offsets.
    GroupCatalog group_catalog_;
    MetricsRegistry metrics_;
    HealthMonitor health_;
#ifdef NEXUS_HAVE_OPENSSL
    /// Contexto TLS del plano de datos (ADR-0019), poblado en `bind()` si `config.tls.enabled()`.
    /// THREAD-SAFE: el bucle de aceptación (núcleo 0) abre cada `TlsConnection` desde él. `nullopt`
    /// = texto plano. Declarado **antes** que `pool_` para destruirse **después** (las corrutinas
    /// de servicio del pool lo referencian a través de `accept_loop`).
    std::optional<TlsContext> tls_;
#endif
    /// Piezas del **modo proxy** (ADR-0006/0027), pobladas en `bind()` si `config.proxy.enabled()`.
    /// REACTOR-LOCAL: solo el núcleo 0 acepta y releva. Declaradas **antes** que `pool_` para
    /// destruirse **después** (las corrutinas de relevo del pool las referencian). El orden interno
    /// importa: `upstream_pool_` referencia a `upstream_dir_` y `proxy_` a `balancer_`, así que el
    /// directorio y el balanceador van **antes** del pool y del proxy.
    std::optional<PeerDirectory> upstream_dir_;  ///< Direcciones del plano de datos aguas arriba.
    std::optional<LoadBalancer> balancer_;       ///< Selección de nodo (estrategia configurable).
    std::optional<UpstreamPool>
        upstream_pool_;           ///< Pool de conexiones aguas arriba (sobre el dir).
    std::optional<Proxy> proxy_;  ///< Enrutado + relevo de tramas (sobre el balanc.).
    /// Adaptador del puerto Kafka sobre el broker real (F7f) y su dispatcher, poblados en `run` si
    /// hay `kafka_port`. REACTOR-LOCAL: el núcleo 0 acepta y sirve las conexiones Kafka. Declarados
    /// **antes** que `pool_` para destruirse **después** (las corrutinas de servicio del pool
    /// referencian el gateway). El orden interno importa: el gateway referencia al broker, así que
    /// el broker va **antes** que el gateway.
    std::optional<KafkaServerBroker> kafka_broker_;
    std::optional<kafka::KafkaGateway> kafka_gateway_;
    ReactorPool pool_;
    ReactorPool::ProactorFactory proactor_factory_;
    /// Núcleo 0 (lo corre `run` inline), publicado para que `stop` lo despierte desde otro hilo /
    /// signal handler. `nullptr` hasta que `run` arranca el pool (carrera resuelta con
    /// `stop_requested_`).
    std::atomic<Reactor*> main_reactor_{nullptr};
    /// Marca de apagado: si `stop` llega antes de que `run` publique el núcleo 0, `run` no arranca.
    std::atomic<bool> stop_requested_{false};
    std::optional<JwtVerifier> jwt_;
    std::optional<AdminApi> admin_api_;
    std::optional<RestGateway> rest_;
    std::optional<AdminRouter> admin_router_;
    std::optional<Listener> listener_;
    std::optional<Listener> admin_listener_;
    std::optional<Listener> cluster_listener_;
    std::optional<Listener> kafka_listener_;
    std::optional<RequestRouter> router_;
    /// Enruta las operaciones de partición a su reactor dueño (se puebla en `run`, tras el pool).
    std::optional<PartitionRouter> partition_router_;
};

}  // namespace nexus
