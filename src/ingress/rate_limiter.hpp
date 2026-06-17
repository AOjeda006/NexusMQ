/// @file   ingress/rate_limiter.hpp
/// @brief  TokenBucket: limitación de tasa por cliente/topic (§6.4, ADR-0006).
/// @ingroup ingress

#pragma once

#include <algorithm>
#include <chrono>

#include "common/types.hpp"

namespace nexus {

/// @brief Cubo de fichas (*token bucket*) para *rate limiting*. Afinidad: REACTOR-LOCAL.
/// @details Realiza el patrón del §6.4: un cubo de capacidad `burst` se rellena a `rate` fichas por
///   segundo (con tope en la capacidad) y cada petición consume `cost` fichas; si no hay
///   suficientes, se rechaza. El relleno es **perezoso** (se calcula al consultar, sin
///   temporizador): en `allow` se acreditan las fichas acumuladas desde el último instante
///   observado. Permite ráfagas hasta `burst` y, en régimen, una tasa media de `rate`.
/// @note **Reloj inyectado** (`MonoTime now` en `allow`), no leído internamente: igual que el resto
///   de la FSM del proyecto (ADR-0015), para pruebas deterministas y para que el reactor controle
///   el tiempo. No es thread-safe: un cubo vive en su reactor (uno por cliente/topic).
/// @invariant `0 <= tokens_ <= cap_`; `last_` nunca retrocede (un `now` anterior no acredita nada).
class TokenBucket {
public:
    /// @param rate_per_sec Fichas acreditadas por segundo (>= 0; 0 = sin relleno, solo la ráfaga).
    /// @param burst Capacidad del cubo (ráfaga máxima); arranca **lleno**.
    /// @param now Instante de referencia inicial.
    TokenBucket(double rate_per_sec, double burst, MonoTime now) noexcept
        : tokens_(std::max(0.0, burst)),
          rate_(std::max(0.0, rate_per_sec)),
          cap_(std::max(0.0, burst)),
          last_(now) {}

    /// @brief Reconfigura tasa y ráfaga; recorta las fichas actuales a la nueva capacidad.
    void configure(double rate_per_sec, double burst) noexcept {
        rate_ = std::max(0.0, rate_per_sec);
        cap_ = std::max(0.0, burst);
        tokens_ = std::min(tokens_, cap_);
    }

    /// @brief Acredita el relleno hasta @p now y consume @p cost fichas si las hay.
    /// @return `true` si la petición se admite (había `>= cost` fichas, ya descontadas); `false` si
    ///   se rechaza (sin descontar nada).
    [[nodiscard]] bool allow(MonoTime now, double cost = 1.0) noexcept {
        refill(now);
        if (tokens_ >= cost) {
            tokens_ -= cost;
            return true;
        }
        return false;
    }

    /// @brief Fichas disponibles tras acreditar el relleno hasta @p now (no consume).
    [[nodiscard]] double available(MonoTime now) noexcept {
        refill(now);
        return tokens_;
    }

    [[nodiscard]] double rate() const noexcept { return rate_; }
    [[nodiscard]] double capacity() const noexcept { return cap_; }

private:
    /// Acredita `rate_ * segundos transcurridos` desde `last_`, con tope en `cap_`, y avanza
    /// `last_`.
    void refill(MonoTime now) noexcept {
        if (now <= last_) {
            return;  // un instante anterior o igual no acredita (el reloj no retrocede).
        }
        const std::chrono::duration<double> elapsed = now - last_;
        tokens_ = std::min(cap_, tokens_ + rate_ * elapsed.count());
        last_ = now;
    }

    double tokens_;
    double rate_;  ///< fichas por segundo.
    double cap_;   ///< capacidad (ráfaga máxima).
    MonoTime last_;
};

}  // namespace nexus
