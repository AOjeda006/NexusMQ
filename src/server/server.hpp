/// @file   server/server.hpp
/// @brief  Server: arranque del broker mono-nodo (reactor + listener + bucle de aceptación).
/// @ingroup server

#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include "broker/request_router.hpp"
#include "broker/topic_manager.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "io/socket.hpp"
#include "reactor/reactor.hpp"

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
        /// Puerto (0 = efímero, útil en tests).
        std::uint16_t port = 0;
        /// Raíz de los logs de partición.
        std::filesystem::path data_dir;
        /// Identidad del nodo (para metadata).
        NodeId node_id = 0;
        /// Host anunciado en metadata.
        std::string advertised_host = "127.0.0.1";
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

    /// Lanza el bucle de aceptación y corre el reactor hasta `stop()` (bloquea el hilo llamante).
    void run();

    /// Solicita el apagado (seguro desde otro hilo / *signal handler*: solo despierta el reactor).
    void stop() noexcept;

private:
    Config config_;
    TopicManager topics_;
    Reactor reactor_;
    std::optional<Listener> listener_;
    std::optional<RequestRouter> router_;
};

}  // namespace nexus
