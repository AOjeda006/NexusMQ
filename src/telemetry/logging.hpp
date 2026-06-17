/// @file   telemetry/logging.hpp
/// @brief  Logger estructurado en JSON con niveles y correlation IDs (§7.6, ADR-0017).
/// @ingroup telemetry

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace nexus {

/// @brief Severidad de un registro de log. Afinidad: INMUTABLE.
enum class LogLevel : std::uint8_t { Trace, Debug, Info, Warn, Error };

/// @brief Campo estructurado (clave→valor) de una línea de log. Afinidad: INMUTABLE.
/// @details `value` admite cadena, entero, doble o booleano; se serializa al tipo JSON correcto.
struct Field {
    std::string key;
    std::variant<std::string, std::int64_t, double, bool> value;
};

/// @name Constructores de `Field` (evitan la ambigüedad de `variant` con `const char*` → `bool`).
/// @{
[[nodiscard]] inline Field field(std::string key, std::string value) {
    return Field{.key = std::move(key), .value = std::move(value)};
}
[[nodiscard]] inline Field field(std::string key, const char* value) {
    return Field{.key = std::move(key), .value = std::string(value)};
}
[[nodiscard]] inline Field field(std::string key, std::int64_t value) {
    return Field{.key = std::move(key), .value = value};
}
[[nodiscard]] inline Field field(std::string key, int value) {
    return Field{.key = std::move(key), .value = static_cast<std::int64_t>(value)};
}
[[nodiscard]] inline Field field(std::string key, double value) {
    return Field{.key = std::move(key), .value = value};
}
[[nodiscard]] inline Field field(std::string key, bool value) {
    return Field{.key = std::move(key), .value = value};
}
/// @}

/// @brief Logger estructurado que emite **una línea JSON por registro** (§7.6). Afinidad:
///   THREAD-SAFE.
/// @details Cada `log` produce un objeto JSON `{"ts":..,"level":..,"msg":..,<contexto>,<campos>}` y
///   lo escribe **atómicamente** (una sola escritura bajo *mutex*) en el `sink`, de modo que las
///   líneas de varios reactores no se entrelazan. El **reloj se inyecta** (por defecto
///   `system_clock::now`) para pruebas deterministas; el *timestamp* se formatea en **RFC 3339
///   UTC** con milisegundos. El **contexto base** (`add_context`, p. ej. `node`, `service`) se
///   emite en cada línea; el *correlation id* es un campo más que el llamante adjunta por petición.
/// @invariant `min_level` es atómico (se puede ajustar en caliente); un registro se emite solo si
/// su
///   nivel `>= min_level`.
class Logger {
public:
    using ClockFn = std::function<std::chrono::system_clock::time_point()>;

    /// @param sink Flujo de salida (no propietario; debe vivir más que el logger).
    /// @param min_level Nivel mínimo emitido.
    /// @param clock Fuente de tiempo (por defecto el reloj de sistema).
    explicit Logger(std::ostream& sink, LogLevel min_level = LogLevel::Info, ClockFn clock = {});

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    Logger(Logger&&) = delete;
    Logger& operator=(Logger&&) = delete;
    ~Logger() = default;

    void set_min_level(LogLevel level) noexcept {
        min_level_.store(level, std::memory_order_relaxed);
    }
    [[nodiscard]] LogLevel min_level() const noexcept {
        return min_level_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] bool enabled(LogLevel level) const noexcept { return level >= min_level(); }

    /// @brief Añade un campo de **contexto base** que se emitirá en cada línea (no thread-safe con
    ///   `log` concurrente: configúrese al construir, antes de compartir el logger).
    void add_context(Field context) { context_.push_back(std::move(context)); }

    /// @brief Emite un registro de nivel @p level con @p message y @p fields (si está habilitado).
    void log(LogLevel level, std::string_view message, std::span<const Field> fields = {});

    void trace(std::string_view message, std::span<const Field> fields = {}) {
        log(LogLevel::Trace, message, fields);
    }
    void debug(std::string_view message, std::span<const Field> fields = {}) {
        log(LogLevel::Debug, message, fields);
    }
    void info(std::string_view message, std::span<const Field> fields = {}) {
        log(LogLevel::Info, message, fields);
    }
    void warn(std::string_view message, std::span<const Field> fields = {}) {
        log(LogLevel::Warn, message, fields);
    }
    void error(std::string_view message, std::span<const Field> fields = {}) {
        log(LogLevel::Error, message, fields);
    }

    /// @brief Nombre textual del nivel (`trace`/`debug`/`info`/`warn`/`error`).
    [[nodiscard]] static std::string_view level_name(LogLevel level) noexcept;

private:
    std::ostream& sink_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::atomic<LogLevel> min_level_;
    ClockFn clock_;
    std::vector<Field> context_;
    std::mutex mutex_;
};

}  // namespace nexus
