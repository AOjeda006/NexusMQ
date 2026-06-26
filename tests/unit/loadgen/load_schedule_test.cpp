// Test del horario open-loop: instantes previstos a tasa fija (corrección de coordinated omission).
#include "load_schedule.hpp"

#include <gtest/gtest.h>

#include <chrono>

#include "common/types.hpp"

namespace {

using nexus::MonoTime;
using nexus::loadgen::OpenLoopSchedule;

// Epoch arbitrario pero fijo, para que las aserciones sean deterministas.
MonoTime fixed_epoch() {
    return MonoTime{} + std::chrono::seconds{100};
}

TEST(OpenLoopSchedule, TasaCero_NoEsRitmadoYSiempreEpoch) {
    const OpenLoopSchedule schedule{0.0, fixed_epoch()};
    EXPECT_FALSE(schedule.is_paced());
    EXPECT_EQ(schedule.intended_at(0), fixed_epoch());
    EXPECT_EQ(schedule.intended_at(1'000), fixed_epoch());  // sin ritmo: enviar cuanto antes
}

TEST(OpenLoopSchedule, TasaNegativa_SeTrataComoNoRitmado) {
    const OpenLoopSchedule schedule{-5.0, fixed_epoch()};
    EXPECT_FALSE(schedule.is_paced());
    EXPECT_EQ(schedule.intended_at(7), fixed_epoch());
}

TEST(OpenLoopSchedule, TasaFija_RepartePeticionesPorIntervalo) {
    // 1000 req/s → intervalo de 1 ms entre instantes previstos.
    const OpenLoopSchedule schedule{1'000.0, fixed_epoch()};
    EXPECT_TRUE(schedule.is_paced());
    EXPECT_EQ(schedule.intended_at(0), fixed_epoch());
    EXPECT_EQ(schedule.intended_at(1), fixed_epoch() + std::chrono::milliseconds{1});
    EXPECT_EQ(schedule.intended_at(1'000), fixed_epoch() + std::chrono::seconds{1});
}

TEST(OpenLoopSchedule, InstantesPrevistos_SonMonotonosCrecientes) {
    const OpenLoopSchedule schedule{250.0, fixed_epoch()};
    MonoTime previous = schedule.intended_at(0);
    for (std::uint64_t i = 1; i < 50; ++i) {
        const MonoTime current = schedule.intended_at(i);
        EXPECT_GT(current, previous);
        previous = current;
    }
}

}  // namespace
