// CircuitBreaker (Nygard, §6.4): reloj inyectado y determinista. Verifica el ciclo
// Closed→Open→HalfOpen→{Closed|Open}: no dispara con pocas muestras, dispara al superar la tasa de
// error, rechaza mientras dura el timeout, admite sondas acotadas en HalfOpen, cierra si se
// recupera y reabre si una sonda falla.
#include "ingress/circuit_breaker.hpp"

#include <gtest/gtest.h>

#include <chrono>

#include "common/types.hpp"

namespace {

using namespace std::chrono_literals;

nexus::MonoTime t0() {
    return nexus::MonoTime{};
}

nexus::CircuitBreakerConfig small_config() {
    nexus::CircuitBreakerConfig cfg;
    cfg.window_size = 10;
    cfg.min_samples = 4;
    cfg.failure_ratio = 0.5;
    cfg.open_timeout = 1s;
    cfg.half_open_probes = 3;
    return cfg;
}

TEST(CircuitBreaker, Closed_PocasMuestras_NoDispara) {
    nexus::CircuitBreaker cb(small_config());
    // 3 fallos < min_samples(4): aún no evalúa la tasa, sigue cerrado.
    for (int i = 0; i < 3; ++i) {
        cb.on_failure(t0());
    }
    EXPECT_EQ(cb.state(), nexus::CircuitState::Closed);
    EXPECT_TRUE(cb.allow(t0()));
}

TEST(CircuitBreaker, Closed_SuperaTasaDeError_Dispara) {
    nexus::CircuitBreaker cb(small_config());
    cb.on_success();  // 1 de 5 muestras es éxito.
    for (int i = 0; i < 4; ++i) {
        cb.on_failure(t0());  // 4/5 = 0,8 >= 0,5 con >= 4 muestras → dispara.
    }
    EXPECT_EQ(cb.state(), nexus::CircuitState::Open);
    EXPECT_FALSE(cb.allow(t0()));  // rechaza en Open.
}

TEST(CircuitBreaker, Open_TrasTimeout_AdmiteSondaEnHalfOpen) {
    nexus::CircuitBreaker cb(small_config());
    for (int i = 0; i < 4; ++i) {
        cb.on_failure(t0());
    }
    ASSERT_EQ(cb.state(), nexus::CircuitState::Open);
    EXPECT_FALSE(cb.allow(t0() + 999ms));  // aún dentro del timeout.

    EXPECT_TRUE(cb.allow(t0() + 1s));  // vence el timeout: pasa a HalfOpen y admite la 1ª sonda.
    EXPECT_EQ(cb.state(), nexus::CircuitState::HalfOpen);
}

TEST(CircuitBreaker, HalfOpen_LimitaElNumeroDeSondas) {
    nexus::CircuitBreaker cb(small_config());
    for (int i = 0; i < 4; ++i) {
        cb.on_failure(t0());
    }
    const nexus::MonoTime probe_time = t0() + 1s;
    EXPECT_TRUE(cb.allow(probe_time));   // sonda 1
    EXPECT_TRUE(cb.allow(probe_time));   // sonda 2
    EXPECT_TRUE(cb.allow(probe_time));   // sonda 3 (= half_open_probes)
    EXPECT_FALSE(cb.allow(probe_time));  // ya no admite más hasta que resuelvan.
}

TEST(CircuitBreaker, HalfOpen_SondasConExito_Cierra) {
    nexus::CircuitBreaker cb(small_config());
    for (int i = 0; i < 4; ++i) {
        cb.on_failure(t0());
    }
    const nexus::MonoTime probe_time = t0() + 1s;
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(cb.allow(probe_time));
    }
    cb.on_success();
    cb.on_success();
    EXPECT_EQ(cb.state(), nexus::CircuitState::HalfOpen);
    cb.on_success();  // 3ª sonda resuelta, todas con éxito → cierra.
    EXPECT_EQ(cb.state(), nexus::CircuitState::Closed);
    EXPECT_TRUE(cb.allow(probe_time));
}

TEST(CircuitBreaker, HalfOpen_UnaSondaFalla_Reabre) {
    nexus::CircuitBreaker cb(small_config());
    for (int i = 0; i < 4; ++i) {
        cb.on_failure(t0());
    }
    const nexus::MonoTime probe_time = t0() + 1s;
    ASSERT_TRUE(cb.allow(probe_time));
    cb.on_failure(probe_time);  // un fallo en sondeo reabre de inmediato.
    EXPECT_EQ(cb.state(), nexus::CircuitState::Open);
    EXPECT_FALSE(cb.allow(probe_time));  // de nuevo dentro del nuevo timeout.
}

}  // namespace
