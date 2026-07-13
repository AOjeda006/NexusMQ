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
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "server/daemon_args.hpp"
#include "server/server.hpp"
#include "storage/segment_crypto.hpp"

namespace {

/// @brief Lee la variable de entorno @p name de forma **portable**.
/// @details En POSIX usa `std::getenv`; en MSVC usa `_dupenv_s` —la API «segura» que evita el aviso
///   C4996 (tratado como error por `/WX`)— y confina el búfer que el CRT reserva en un propietario
///   RAII. Devuelve `nullopt` si la variable no está definida (cadena vacía **sí** es un valor).
[[nodiscard]] std::optional<std::string> read_env(const char* name) {
#ifdef _MSC_VER
    char* raw = nullptr;
    std::size_t len = 0;
    if (_dupenv_s(&raw, &len, name) != 0 || raw == nullptr) {
        return std::nullopt;
    }
    const std::unique_ptr<char, decltype(&std::free)> owned{raw, &std::free};
    return std::string(owned.get());
#else
    if (const char* env = std::getenv(name); env != nullptr) {
        return std::string(env);
    }
    return std::nullopt;
#endif
}

/// @brief Resuelve la KEK de cifrado en reposo (ADR-0031) sobre @p config.
/// @details Toma la clave del flag `--encryption-key` o, preferido, de la variable de entorno
///   `NEXUS_ENCRYPTION_KEY` (no expone la clave en `ps`); el flag tiene prioridad. Si hay clave, la
///   valida y construye; si es inválida (o el broker no tiene OpenSSL), devuelve el error para
///   **abortar el arranque** en vez de degradar en silencio a texto plano. Sin clave, no hace nada
///   (logs en claro, comportamiento por defecto). Limpia el hex de la config tras resolverlo.
[[nodiscard]] nexus::expected<void> resolve_encryption_key(nexus::Server::Config& config) {
    std::string hex = config.encryption_key_hex;
    config.encryption_key_hex.clear();
    if (hex.empty()) {
        if (const auto env = read_env("NEXUS_ENCRYPTION_KEY"); env.has_value()) {
            hex = *env;
        }
    }
    if (hex.empty()) {
        return {};  // sin cifrado: logs en claro.
    }
    auto key = nexus::EncryptionKey::from_hex(hex);
    if (!key) {
        return std::unexpected(key.error());
    }
    config.encryption_key = std::make_shared<const nexus::EncryptionKey>(*key);
    return {};
}

/// @brief Resuelve el directorio del tiered storage (ADR-0032): el flag `--tier-dir` o, si no, la
///   variable de entorno `NEXUS_TIER_DIR`. Vacío = sin tiering (por defecto).
void resolve_tier_dir(nexus::Server::Config& config) {
    if (!config.tier_dir.empty()) {
        return;  // el flag tiene prioridad.
    }
    if (const auto env = read_env("NEXUS_TIER_DIR"); env.has_value() && !env->empty()) {
        config.tier_dir = *env;
    }
}

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
        if (const nexus::expected<void> resolved = resolve_encryption_key(config); !resolved) {
            std::cerr << "clave de cifrado en reposo inválida: " << resolved.error().message()
                      << "\n";
            return EXIT_FAILURE;
        }
        resolve_tier_dir(config);

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
