/// @file   ingress/circuit_breaker.hpp
/// @brief  CircuitBreaker: estabilidad ante fallos en cascada (Nygard, §6.4, ADR-0006).
/// @ingroup ingress

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/types.hpp"

namespace nexus {

/// @brief Estado del cortacircuitos (Nygard). Afinidad: INMUTABLE (enum).
enum class CircuitState : std::uint8_t {
    Closed,   ///< Operación normal: deja pasar y vigila la tasa de error reciente.
    Open,     ///< Disparado: rechaza de inmediato hasta que vence el *timeout* de apertura.
    HalfOpen  ///< Sondeo: deja pasar un número acotado de pruebas para tantear la recuperación.
};

/// @brief Parámetros del cortacircuitos. Afinidad: INMUTABLE.
struct CircuitBreakerConfig {
    /// Tamaño de la ventana deslizante de resultados (en Closed).
    std::size_t window_size = 20;
    /// Mínimo de muestras en la ventana antes de poder disparar (evita abrir con pocos datos).
    std::size_t min_samples = 10;
    /// Tasa de error `[0,1]` que dispara la apertura (en Closed) o reabre (en HalfOpen).
    double failure_ratio = 0.5;
    /// Tiempo en Open antes de admitir sondas (pasar a HalfOpen).
    std::chrono::milliseconds open_timeout{5000};
    /// Número de sondas admitidas en HalfOpen para decidir cerrar o reabrir.
    std::size_t half_open_probes = 3;
};

/// @brief Cortacircuitos con ventana deslizante de errores (patrón Nygard). Afinidad:
/// REACTOR-LOCAL.
/// @details Tres estados (§6.4): **Closed** deja pasar y registra cada resultado en una ventana
///   deslizante; si con suficientes muestras la tasa de error alcanza `failure_ratio`, **dispara**
///   a Open. **Open** rechaza de inmediato (`allow` = `false`) hasta que transcurre `open_timeout`,
///   momento en que la siguiente `allow` pasa a **HalfOpen**. **HalfOpen** admite hasta
///   `half_open_probes` sondas; cuando todas resuelven, decide por su tasa de error: cierra si se
///   recuperó, reabre si no. Aísla un dependiente caído evitando martillearlo y reintenta con
///   tiento.
/// @note **Reloj inyectado** (`MonoTime now`), no leído internamente (ADR-0015): pruebas
///   deterministas y control del tiempo por el reactor. No es thread-safe: vive en su reactor.
/// @invariant En Open, `allow` rechaza hasta `opened_at_ + open_timeout`; en HalfOpen, el nº de
///   sondas admitidas no excede `half_open_probes`.
class CircuitBreaker {
public:
    explicit CircuitBreaker(CircuitBreakerConfig config);

    /// @brief ¿Se admite una operación ahora? Puede transicionar Open→HalfOpen al vencer el
    /// timeout.
    /// @return `true` si se admite (en HalfOpen, además, contabiliza la sonda); `false` si se
    /// rechaza.
    [[nodiscard]] bool allow(MonoTime now);

    /// @brief Registra un éxito (cierra desde HalfOpen si ya resolvieron todas las sondas con
    /// éxito).
    void on_success();

    /// @brief Registra un fallo (dispara desde Closed por tasa de error; reabre desde HalfOpen).
    void on_failure(MonoTime now);

    [[nodiscard]] CircuitState state() const noexcept { return state_; }

private:
    void trip_open(MonoTime now);
    void enter_half_open();
    void close();
    /// Anota un resultado en la ventana deslizante (Closed); mantiene el contador de fallos.
    void record(bool failure);
    /// Tasa de error de la ventana, o 0 si está por debajo de `min_samples`.
    [[nodiscard]] double window_failure_ratio() const;

    CircuitBreakerConfig config_;
    CircuitState state_ = CircuitState::Closed;

    // Ventana deslizante de resultados (anillo de `window_size`): `true` = fallo.
    std::vector<bool> window_;
    std::size_t head_ = 0;      ///< próxima posición a escribir.
    std::size_t count_ = 0;     ///< muestras válidas en la ventana (<= window_size).
    std::size_t failures_ = 0;  ///< fallos vivos en la ventana (contador incremental).

    MonoTime opened_at_;  ///< instante de la última apertura (base del timeout).

    // Contabilidad de sondas en HalfOpen.
    std::size_t probes_issued_ = 0;
    std::size_t probe_successes_ = 0;
};

}  // namespace nexus
