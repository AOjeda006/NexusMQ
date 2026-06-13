// Pruebas de CreditWindow: backpressure por créditos (§7.11 #4). Se conducen las corrutinas a mano
// (arranque por `handle().resume()`, concesión por `grant`) sin necesidad de un reactor.
#include "broker/credit_window.hpp"

#include <gtest/gtest.h>

#include <cstdint>

#include "common/task.hpp"

namespace {

// Emisor de prueba: reserva `cost` créditos y, al lograrlo, incrementa `progress`.
nexus::task<void> acquire_once(nexus::CreditWindow& window, int& progress, std::int32_t cost) {
    co_await window.acquire(cost);
    ++progress;
}

// Emisor que reserva dos veces (frena entre medias si no hay créditos).
nexus::task<void> acquire_twice(nexus::CreditWindow& window, int& progress, std::int32_t cost) {
    co_await window.acquire(cost);
    ++progress;
    co_await window.acquire(cost);
    ++progress;
}

TEST(CreditWindow, Acquire_ConCreditos_NoSuspende) {
    nexus::CreditWindow window{10};
    int progress = 0;
    nexus::task<void> sender = acquire_once(window, progress, 4);
    sender.handle().resume();  // arranca; hay créditos → no se frena.
    EXPECT_EQ(progress, 1);
    EXPECT_TRUE(sender.done());
    EXPECT_EQ(window.available(), 6);  // 10 - 4
    EXPECT_FALSE(window.has_waiter());
}

TEST(CreditWindow, Acquire_SinCreditos_SeFrenaHastaConceder) {
    nexus::CreditWindow window{0};
    int progress = 0;
    nexus::task<void> sender = acquire_once(window, progress, 5);
    sender.handle().resume();  // arranca; sin créditos → se frena en acquire.
    EXPECT_EQ(progress, 0);
    EXPECT_FALSE(sender.done());
    EXPECT_TRUE(window.has_waiter());

    window.grant(5);  // concede justo lo necesario → reanuda al emisor.
    EXPECT_EQ(progress, 1);
    EXPECT_TRUE(sender.done());
    EXPECT_EQ(window.available(), 0);  // 0 + 5 - 5
    EXPECT_FALSE(window.has_waiter());
}

TEST(CreditWindow, Grant_Insuficiente_MantieneFrenado) {
    nexus::CreditWindow window{0};
    int progress = 0;
    nexus::task<void> sender = acquire_once(window, progress, 5);
    sender.handle().resume();
    ASSERT_FALSE(sender.done());

    window.grant(3);  // no alcanza para el coste de 5.
    EXPECT_EQ(progress, 0);
    EXPECT_FALSE(sender.done());
    EXPECT_TRUE(window.has_waiter());
    EXPECT_EQ(window.available(), 3);

    window.grant(2);  // ahora 3 + 2 = 5 ≥ 5 → reanuda.
    EXPECT_EQ(progress, 1);
    EXPECT_TRUE(sender.done());
    EXPECT_EQ(window.available(), 0);
}

TEST(CreditWindow, DosReservas_SeFrenaEnLaSegunda) {
    nexus::CreditWindow window{5};
    int progress = 0;
    nexus::task<void> sender = acquire_twice(window, progress, 5);
    sender.handle().resume();  // primera reserva consume los 5 créditos; segunda se frena.
    EXPECT_EQ(progress, 1);
    EXPECT_FALSE(sender.done());
    EXPECT_TRUE(window.has_waiter());
    EXPECT_EQ(window.available(), 0);

    window.grant(5);  // reanuda la segunda reserva.
    EXPECT_EQ(progress, 2);
    EXPECT_TRUE(sender.done());
    EXPECT_EQ(window.available(), 0);
}

}  // namespace
