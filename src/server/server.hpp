/// @file   server/server.hpp
/// @brief  Server: arranque del broker mono-nodo (reactor + listener + bucle de aceptación).
/// @ingroup server

#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "broker/group_catalog.hpp"
#include "broker/request_router.hpp"
#include "broker/topic_catalog.hpp"
#include "broker/topic_manager.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "ingress/health_monitor.hpp"
#include "ingress/jwt.hpp"
#include "ingress/rest_gateway.hpp"
#include "io/socket.hpp"
#include "reactor/partition_router.hpp"
#include "reactor/reactor.hpp"
#include "reactor/reactor_pool.hpp"
#include "server/admin_api.hpp"
#include "server/admin_router.hpp"
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
///   núcleo 0; cada partición se enruta a su reactor dueño (`partition % N`) y cada grupo a su núcleo
///   coordinador (`hash(group_id) % N`) por paso de mensajes (sharding *shared-nothing*, ADR-0026).
///   `num_reactors` fija N (por defecto 1; N>1 es opt-in y reparte el plano de datos entre núcleos).
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
        /// Secreto HMAC del JWT que protege el REST admin. Vacío = sin autenticación.
        std::string jwt_secret;
        /// Mínimo de espacio libre en disco (bytes) para `/readyz`. `0` = sin chequeo de disco.
        std::uintmax_t min_free_disk_bytes = 0;
        /// Número de reactores del pool (uno por núcleo). `<= 0` se trata como 1.
        int num_reactors = 1;
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

    /// Crea un topic con @p partition_count particiones (plano de control; antes de servir).
    [[nodiscard]] expected<void> create_topic(const std::string& name,
                                              std::int32_t partition_count);

    /// Enlaza el listener y prepara el router con el puerto efectivo. Idempotente-no: llamar una
    /// vez.
    [[nodiscard]] expected<void> bind();

    /// Puerto realmente enlazado (útil con puerto efímero). `0` si no se ha llamado a `bind`.
    [[nodiscard]] std::uint16_t port() const noexcept;

    /// Puerto del plano de operación realmente enlazado; `0` si está desactivado o sin `bind`.
    [[nodiscard]] std::uint16_t admin_port() const noexcept;

    /// Lanza el bucle de aceptación y corre el reactor hasta `stop()` (bloquea el hilo llamante).
    void run();

    /// Solicita el apagado (seguro desde otro hilo / *signal handler*: solo despierta el reactor).
    void stop() noexcept;

private:
    /// Enumera los grupos coordinados en el núcleo 0, traducidos a DTOs y paginados (para el
    /// `AdminApi`). No es `const`: accede al `GroupCatalog` (mutable) del propio hilo del admin.
    [[nodiscard]] std::vector<GroupSummary> list_groups(Page page);

    Config config_;
    /// Catálogo de topics fragmentado por núcleo (ADR-0026): un `TopicManager` por reactor. El del
    /// núcleo 0 atiende las conexiones; el plano de datos enruta cada partición a su dueño.
    TopicCatalog catalog_;
    /// Coordinación de grupos fragmentada por núcleo (ADR-0026): cada grupo se coordina en el
    /// núcleo `hash(group_id) % N`, donde viven su membresía y sus offsets.
    GroupCatalog group_catalog_;
    MetricsRegistry metrics_;
    HealthMonitor health_;
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
    std::optional<RequestRouter> router_;
    /// Enruta las operaciones de partición a su reactor dueño (se puebla en `run`, tras el pool).
    std::optional<PartitionRouter> partition_router_;
};

}  // namespace nexus
