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
#include <charconv>
#if !defined(_WIN32)
#include <csignal>
#endif
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

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

/// Convierte @p text a entero; devuelve @p fallback si no es un número decimal completo y válido.
int parse_int(std::string_view text, int fallback) {
    int value = fallback;
    const char* begin = text.data();
    const char* end = begin + text.size();
    const std::from_chars_result result = std::from_chars(begin, end, value);
    if (result.ec != std::errc{} || result.ptr != end) {
        return fallback;
    }
    return value;
}

/// Parsea "nombre:particiones" (p. ej. "eventos:4"); particiones=1 si se omite o es inválido.
std::pair<std::string, std::int32_t> parse_topic_spec(std::string_view spec) {
    const std::size_t colon = spec.find(':');
    if (colon == std::string_view::npos) {
        return {std::string{spec}, 1};
    }
    const std::string name{spec.substr(0, colon)};
    const int partitions = parse_int(spec.substr(colon + 1), 1);
    return {name, partitions > 0 ? partitions : 1};
}

}  // namespace

int main(int argc, char** argv) {
    try {
        nexus::Server::Config config;
        config.port = 9092;
        config.data_dir = "./nexus-data";
        std::vector<std::pair<std::string, std::int32_t>> topics;

        const std::span<char*> args{argv, static_cast<std::size_t>(argc)};
        for (std::size_t i = 1; i < args.size(); ++i) {
            const std::string_view arg{args[i]};
            const bool has_next = i + 1 < args.size();
            if (arg == "--port" && has_next) {
                config.port = static_cast<std::uint16_t>(parse_int(args[++i], config.port));
            } else if (arg == "--data-dir" && has_next) {
                config.data_dir = args[++i];
            } else if (arg == "--host" && has_next) {
                config.host = args[++i];
                config.advertised_host = config.host;
            } else if (arg == "--admin-port" && has_next) {
                config.admin_port = static_cast<std::uint16_t>(parse_int(args[++i], 0));
            } else if (arg == "--jwt-secret" && has_next) {
                config.jwt_secret = args[++i];
            } else if (arg == "--node-id" && has_next) {
                config.node_id = static_cast<nexus::NodeId>(parse_int(args[++i], config.node_id));
            } else if (arg == "--topic" && has_next) {
                topics.push_back(parse_topic_spec(args[++i]));
            } else {
                std::cerr << "uso: nexusd [--port N] [--admin-port N] [--data-dir DIR] [--host H] "
                             "[--node-id N] [--jwt-secret S] [--topic nombre:parts]\n";
                return EXIT_FAILURE;
            }
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
        std::cout << " (Ctrl-C para parar)\n";
        server.run();
        std::cout << "NexusMQ detenido.\n";
    } catch (const std::exception& ex) {
        std::cerr << "fallo de arranque: " << ex.what() << "\n";
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
