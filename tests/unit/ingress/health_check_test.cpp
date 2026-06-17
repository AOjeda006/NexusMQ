// HealthChecker (§6.4/§7.5): salud de nodos con histéresis, sin reloj y determinista. Un nodo no
// visto se considera sano; cae tras `failure_threshold` fallos consecutivos; un éxito reinicia el
// contador de fallos; y se recupera tras `success_threshold` éxitos consecutivos.
#include "ingress/health_check.hpp"

#include <gtest/gtest.h>

namespace {

nexus::HealthCheckConfig small_config() {
    nexus::HealthCheckConfig cfg;
    cfg.failure_threshold = 3;
    cfg.success_threshold = 2;
    return cfg;
}

TEST(HealthChecker, Healthy_NodoNoVisto_OptimistaSano) {
    nexus::HealthChecker hc(small_config());
    EXPECT_TRUE(hc.healthy(42));
    EXPECT_EQ(hc.consecutive_failures(42), 0U);
}

TEST(HealthChecker, Observe_FallosConsecutivos_MarcaCaido) {
    nexus::HealthChecker hc(small_config());
    hc.observe(1, false);
    hc.observe(1, false);
    EXPECT_TRUE(hc.healthy(1));  // 2 < umbral(3).
    hc.observe(1, false);
    EXPECT_FALSE(hc.healthy(1));  // 3 fallos consecutivos.
}

TEST(HealthChecker, Observe_UnExitoReiniciaElContadorDeFallos) {
    nexus::HealthChecker hc(small_config());
    hc.observe(1, false);
    hc.observe(1, false);
    hc.observe(1, true);  // reinicia los fallos.
    hc.observe(1, false);
    hc.observe(1, false);
    EXPECT_TRUE(hc.healthy(1));  // no llega a 3 fallos seguidos.
    EXPECT_EQ(hc.consecutive_failures(1), 2U);
}

TEST(HealthChecker, Observe_RecuperaTrasExitosConsecutivos) {
    nexus::HealthChecker hc(small_config());
    for (int i = 0; i < 3; ++i) {
        hc.observe(1, false);
    }
    ASSERT_FALSE(hc.healthy(1));
    hc.observe(1, true);
    EXPECT_FALSE(hc.healthy(1));  // 1 < success_threshold(2).
    hc.observe(1, true);
    EXPECT_TRUE(hc.healthy(1));  // 2 éxitos consecutivos: vuelve a sano.
}

TEST(HealthChecker, Observe_UnFalloDuranteLaRecuperacion_ReiniciaExitos) {
    nexus::HealthChecker hc(small_config());
    for (int i = 0; i < 3; ++i) {
        hc.observe(1, false);
    }
    hc.observe(1, true);   // 1 éxito.
    hc.observe(1, false);  // reinicia los éxitos; sigue caído.
    hc.observe(1, true);
    EXPECT_FALSE(hc.healthy(1));  // solo 1 éxito tras el reinicio.
}

}  // namespace
