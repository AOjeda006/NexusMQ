/// @file   server/server.hpp
/// @brief  Server: arranque del broker mono-nodo (reactor + listener + bucle de aceptación).
/// @ingroup server

#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "broker/request_router.hpp"
#include "broker/topic_manager.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "ingress/health_monitor.hpp"
#include "ingress/jwt.hpp"
#include "ingress/rest_gateway.hpp"
#include "io/socket.hpp"
#include "reactor/reactor.hpp"
#include "server/admin_api.hpp"
#include "server/admin_router.hpp"
#include "telemetry/metrics.hpp"

namespace nexus {

/// @brief Daemon del broker en un nodo (Fase 1b: un solo reactor). Afinidad: el bucle corre en el
///   hilo del reactor; `stop` es seguro desde otro hilo (incluido un *signal handler*).
/// @details Orquesta `TopicManager` + `RequestRouter` + un `Reactor` (io_uring) + un `Listener`.
///   `bind` enlaza el puerto (plano de control), `run` lanza el bucle de aceptación y corre el
///   reactor (bloquea), `stop` lo despierta para salir. El multi-reactor (un reactor por núcleo
///   con routing por partición) llega con la escalada thread-per-core; aquí N=1.
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
    };

    /// Crea el reactor (io_uring) y el resto de piezas. Lanza `std::system_error` si io_uring no
    /// está disponible (plano de control).
    explicit Server(Config config);
    ~Server() = default;
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) = delete;
    Server& operator=(Server&&) = delete;

    /// Crea un topic con @p partition_count particiones (plano de control; antes de servir).
    [[nodiscard]] expected<void> create_topic(std::string name, std::int32_t partition_count);

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
    /// Enumera los grupos del reactor traducidos a DTOs y paginados (para el `AdminApi`).
    [[nodiscard]] std::vector<GroupSummary> list_groups(Page page) const;

    Config config_;
    TopicManager topics_;
    MetricsRegistry metrics_;
    HealthMonitor health_;
    Reactor reactor_;
    std::optional<JwtVerifier> jwt_;
    std::optional<AdminApi> admin_api_;
    std::optional<RestGateway> rest_;
    std::optional<AdminRouter> admin_router_;
    std::optional<Listener> listener_;
    std::optional<Listener> admin_listener_;
    std::optional<RequestRouter> router_;
};

}  // namespace nexus
