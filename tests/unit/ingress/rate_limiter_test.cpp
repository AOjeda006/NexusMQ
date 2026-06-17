// TokenBucket (rate limiter, §6.4): reloj inyectado y determinista. Verifica la ráfaga inicial, el
// rechazo al agotar, el relleno perezoso a `rate` fichas/segundo con tope en la capacidad, el coste
// variable y la reconfiguración (recorte a la nueva capacidad).
#include "ingress/rate_limiter.hpp"

#include <gtest/gtest.h>

#include <chrono>

#include "common/types.hpp"

namespace {

using namespace std::chrono_literals;

nexus::MonoTime t0() {
    return nexus::MonoTime{};
}

TEST(TokenBucket, Allow_CuboLleno_AdmiteHastaLaRafaga) {
    nexus::TokenBucket bucket(10.0, 5.0, t0());  // 10 fichas/s, ráfaga 5; arranca lleno.
    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(bucket.allow(t0())) << "ficha " << i;
    }
    EXPECT_FALSE(bucket.allow(t0()));  // agotada la ráfaga, sin tiempo transcurrido.
}

TEST(TokenBucket, Allow_TrasAgotar_SeRellenaConElTiempo) {
    nexus::TokenBucket bucket(10.0, 5.0, t0());
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(bucket.allow(t0()));
    }
    ASSERT_FALSE(bucket.allow(t0()));

    // 10 fichas/s × 0,2 s = 2 fichas acreditadas.
    const nexus::MonoTime later = t0() + 200ms;
    EXPECT_TRUE(bucket.allow(later));
    EXPECT_TRUE(bucket.allow(later));
    EXPECT_FALSE(bucket.allow(later));
}

TEST(TokenBucket, Refill_NoSuperaLaCapacidad) {
    nexus::TokenBucket bucket(100.0, 3.0, t0());
    // Mucho tiempo, pero el cubo no acumula por encima de la capacidad (3).
    const nexus::MonoTime far = t0() + 10s;
    EXPECT_DOUBLE_EQ(bucket.available(far), 3.0);
}

TEST(TokenBucket, Allow_CosteVariable_DescuentaLoIndicado) {
    nexus::TokenBucket bucket(0.0, 10.0, t0());  // sin relleno: solo la ráfaga.
    EXPECT_TRUE(bucket.allow(t0(), 7.0));
    EXPECT_FALSE(bucket.allow(t0(), 4.0));  // quedan 3 < 4.
    EXPECT_TRUE(bucket.allow(t0(), 3.0));
    EXPECT_DOUBLE_EQ(bucket.available(t0()), 0.0);
}

TEST(TokenBucket, Allow_RelojAnterior_NoAcredita) {
    nexus::TokenBucket bucket(10.0, 1.0, t0() + 1s);
    ASSERT_TRUE(bucket.allow(t0() + 1s));
    // Un `now` anterior no debe acreditar fichas (el reloj monótono no retrocede).
    EXPECT_FALSE(bucket.allow(t0()));
}

TEST(TokenBucket, Configure_RecortaFichasALaNuevaCapacidad) {
    nexus::TokenBucket bucket(10.0, 100.0, t0());
    EXPECT_DOUBLE_EQ(bucket.available(t0()), 100.0);
    bucket.configure(5.0, 4.0);  // baja la ráfaga: recorta a 4.
    EXPECT_DOUBLE_EQ(bucket.available(t0()), 4.0);
    EXPECT_DOUBLE_EQ(bucket.rate(), 5.0);
    EXPECT_DOUBLE_EQ(bucket.capacity(), 4.0);
}

}  // namespace
