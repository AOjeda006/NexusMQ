#include "reactor/spsc_queue.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <optional>
#include <thread>
#include <vector>

namespace {

TEST(SpscQueue, TryPop_Vacia_DevuelveNullopt) {
    nexus::SpscQueue<int, 4> queue;
    EXPECT_FALSE(queue.try_pop().has_value());
    EXPECT_EQ(queue.size_approx(), 0U);
}

TEST(SpscQueue, TryPush_Llena_DevuelveFalse) {
    nexus::SpscQueue<int, 4> queue;
    EXPECT_TRUE(queue.try_push(1));
    EXPECT_TRUE(queue.try_push(2));
    EXPECT_TRUE(queue.try_push(3));
    EXPECT_TRUE(queue.try_push(4));
    EXPECT_FALSE(queue.try_push(5));  // capacidad agotada
    EXPECT_EQ(queue.size_approx(), 4U);
}

TEST(SpscQueue, PushPop_RespetaOrdenFIFO) {
    nexus::SpscQueue<int, 4> queue;
    ASSERT_TRUE(queue.try_push(10));
    ASSERT_TRUE(queue.try_push(20));
    EXPECT_EQ(queue.try_pop().value(), 10);
    EXPECT_EQ(queue.try_pop().value(), 20);
    EXPECT_FALSE(queue.try_pop().has_value());
}

TEST(SpscQueue, VaciaHueco_PermiteSeguirEncolando) {
    nexus::SpscQueue<int, 2> queue;
    ASSERT_TRUE(queue.try_push(1));
    ASSERT_TRUE(queue.try_push(2));
    EXPECT_FALSE(queue.try_push(3));
    EXPECT_EQ(queue.try_pop().value(), 1);  // libera un hueco
    EXPECT_TRUE(queue.try_push(3));         // y se reaprovecha (anillo)
    EXPECT_EQ(queue.try_pop().value(), 2);
    EXPECT_EQ(queue.try_pop().value(), 3);
}

// Estrés: un productor y un consumidor concurrentes transfieren N enteros sin pérdida
// ni reordenación. La comprobación es por conservación (no por temporización): robusto.
TEST(SpscQueue, ProductorConsumidor_TransfiereTodoEnOrden) {
    constexpr int kCount = 100'000;
    nexus::SpscQueue<int, 1024> queue;
    std::vector<int> received;
    received.reserve(kCount);

    std::thread producer([&] {
        for (int i = 0; i < kCount; ++i) {
            while (!queue.try_push(i)) {
                // anillo lleno: ceder y reintentar
                std::this_thread::yield();
            }
        }
    });

    int popped = 0;
    while (popped < kCount) {
        if (auto value = queue.try_pop()) {
            received.push_back(*value);
            ++popped;
        } else {
            std::this_thread::yield();
        }
    }
    producer.join();

    ASSERT_EQ(received.size(), static_cast<std::size_t>(kCount));
    for (int i = 0; i < kCount; ++i) {
        ASSERT_EQ(received[static_cast<std::size_t>(i)], i);  // FIFO estricto
    }
}

}  // namespace
