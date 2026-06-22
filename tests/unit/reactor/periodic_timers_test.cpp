// Pruebas de PeriodicTimers: lógica de vencimiento periódica, determinista con `now` inyectado
// (sin reloj real). Cubre primer disparo tras el intervalo, reprogramación, acotado del deadline,
// varios temporizadores y cancelación.
#include "reactor/periodic_timers.hpp"

#include <gtest/gtest.h>

#include <chrono>

#include "common/types.hpp"

namespace {

using namespace std::chrono_literals;

// Origen de tiempo estable para construir instantes deterministas.
const nexus::MonoTime kBase{};

TEST(PeriodicTimers, Add_NoVenceAntesDelIntervalo) {
    nexus::PeriodicTimers timers;
    int fired = 0;
    timers.add(kBase, 10ms, [&fired](nexus::MonoTime) { ++fired; });

    EXPECT_EQ(timers.fire_due(kBase + 9ms), 0U);  // aún no vence
    EXPECT_EQ(fired, 0);
    EXPECT_EQ(timers.fire_due(kBase + 10ms), 1U);  // vence exactamente en el intervalo
    EXPECT_EQ(fired, 1);
}

TEST(PeriodicTimers, FireDue_ReprogramaAlSiguienteIntervalo) {
    nexus::PeriodicTimers timers;
    int fired = 0;
    timers.add(kBase, 10ms, [&fired](nexus::MonoTime) { ++fired; });

    EXPECT_EQ(timers.fire_due(kBase + 10ms), 1U);
    EXPECT_EQ(timers.fire_due(kBase + 15ms), 0U);  // ya reprogramado a +20ms
    EXPECT_EQ(timers.fire_due(kBase + 20ms), 1U);
    EXPECT_EQ(fired, 2);
}

TEST(PeriodicTimers, FireDue_PasaElInstanteDeDisparoAlCallback) {
    nexus::PeriodicTimers timers;
    nexus::MonoTime seen{};
    timers.add(kBase, 10ms, [&seen](nexus::MonoTime now) { seen = now; });

    timers.fire_due(kBase + 12ms);
    EXPECT_EQ(seen, kBase + 12ms);
}

TEST(PeriodicTimers, NextDeadline_DevuelveElMenorAcotadoAlTope) {
    nexus::PeriodicTimers timers;
    timers.add(kBase, 30ms, [](nexus::MonoTime) {});
    timers.add(kBase, 10ms, [](nexus::MonoTime) {});

    EXPECT_EQ(timers.next_deadline(kBase + 1s), kBase + 10ms);  // el más próximo
}

TEST(PeriodicTimers, NextDeadline_SinTemporizadores_DevuelveElTope) {
    const nexus::PeriodicTimers timers;
    EXPECT_EQ(timers.next_deadline(kBase + 1s), kBase + 1s);
}

TEST(PeriodicTimers, FireDue_VariosVencidos_DisparaTodos) {
    nexus::PeriodicTimers timers;
    int a = 0;
    int b = 0;
    timers.add(kBase, 10ms, [&a](nexus::MonoTime) { ++a; });
    timers.add(kBase, 5ms, [&b](nexus::MonoTime) { ++b; });

    EXPECT_EQ(timers.fire_due(kBase + 10ms), 2U);  // ambos vencidos
    EXPECT_EQ(a, 1);
    EXPECT_EQ(b, 1);
}

TEST(PeriodicTimers, Cancel_DetieneElTemporizador) {
    nexus::PeriodicTimers timers;
    int fired = 0;
    const nexus::PeriodicTimers::Id id =
        timers.add(kBase, 10ms, [&fired](nexus::MonoTime) { ++fired; });

    timers.cancel(id);
    EXPECT_TRUE(timers.empty());
    EXPECT_EQ(timers.fire_due(kBase + 100ms), 0U);
    EXPECT_EQ(fired, 0);
}

}  // namespace
