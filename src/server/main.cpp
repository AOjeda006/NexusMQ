/// @file   server/main.cpp
/// @brief  nexusd: punto de entrada del daemon del broker (bootstrap + señales).
/// @ingroup server

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>  // SetConsoleCtrlHandler
#endif

#include <atomic>
#if !defined(_WIN32)
#include <csignal>
#endif
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "server/daemon_args.hpp"
#include "server/server.hpp"

namespace {

/// Servidor activo, para el manejador de señales (solo lo despierta: async-signal-safe). Global a
/// propósito: un manejador de señales POSIX no recibe contexto y debe alcanzarlo por aquí.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<nexus::Server*> g_server{nullptr};

#if defined(_WIN32)
/// Manejador de eventos de consola de Windows (ADR-0028). El SO lo invoca en un **hilo aparte**
/// ante Ctrl-C, Ctrl-Break, cierre de la consola, cierre de sesión o apagado; solo despierta el
/// servidor
/// (`stop()` es thread-safe: marca un flag y hace `PostQueuedCompletionStatus` vía
/// `Proactor::wake()`, sin tocar estado reactor-local). Equivale al manejador de señales POSIX,
/// pero capta también los eventos de cierre que `std::signal` no cubre en Windows.
BOOL WINAPI on_console_ctrl(DWORD ctrl_type) {
    switch (ctrl_type) {
        case CTRL_C_EVENT:
        case CTRL_BREAK_EVENT:
        case CTRL_CLOSE_EVENT:
        case CTRL_LOGOFF_EVENT:
        case CTRL_SHUTDOWN_EVENT:
            if (nexus::Server* server = g_server.load(std::memory_order_acquire);
                server != nullptr) {
                server->stop();
            }
            return TRUE;  // manejado: no se invoca al siguiente manejador.
        default:
            return FALSE;
    }
}
#else
extern "C" void on_signal(int /*signal*/) {
    nexus::Server* server = g_server.load(std::memory_order_acquire);
    if (server != nullptr) {
        server->stop();  // stop() solo escribe un eventfd (async-signal-safe) y marca un flag.
    }
}
#endif

}  // namespace

int main(int argc, char** argv) {
    try {
        nexus::Server::Config config;
        config.port = 9092;
        config.data_dir = "./nexus-data";
        std::vector<nexus::DaemonTopicSpec> topics;

        const std::span<char*> args{argv, static_cast<std::size_t>(argc)};
        if (!nexus::parse_daemon_args(args, config, topics)) {
            std::cerr << nexus::kDaemonUsage;
            return EXIT_FAILURE;
        }

        nexus::Server server{std::move(config)};
        for (auto& [name, partitions] : topics) {
            if (const nexus::expected<void> created = server.create_topic(name, partitions);
                !created) {
                std::cerr << "no se pudo crear el topic '" << name
                          << "': " << created.error().message() << "\n";
                return EXIT_FAILURE;
            }
        }
        if (const nexus::expected<void> bound = server.bind(); !bound) {
            std::cerr << "no se pudo enlazar: " << bound.error().message() << "\n";
            return EXIT_FAILURE;
        }
        g_server.store(&server, std::memory_order_release);
#if defined(_WIN32)
        ::SetConsoleCtrlHandler(on_console_ctrl, TRUE);
#else
        std::signal(SIGINT, on_signal);
        std::signal(SIGTERM, on_signal);
#endif
        std::cout << "NexusMQ escuchando en " << server.port();
        if (server.admin_port() != 0) {
            std::cout << " (operación en " << server.admin_port() << ")";
        }
        if (server.kafka_port() != 0) {
            std::cout << " (Kafka en " << server.kafka_port() << ")";
        }
        std::cout << " (Ctrl-C para parar)\n";
        server.run();
        std::cout << "NexusMQ detenido.\n";
    } catch (const std::exception& ex) {
        std::cerr << "fallo de arranque: " << ex.what() << "\n";
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
