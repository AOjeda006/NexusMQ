#include "reactor/mpmc_queue.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <optional>
#include <thread>
#include <vector>

namespace {

TEST(MpmcQueue, TryPop_Vacia_DevuelveNullopt) {
    nexus::MpmcQueue<int, 4> queue;
    EXPECT_FALSE(queue.try_pop().has_value());
}

TEST(MpmcQueue, TryPush_Llena_DevuelveFalse) {
    nexus::MpmcQueue<int, 4> queue;
    EXPECT_TRUE(queue.try_push(1));
    EXPECT_TRUE(queue.try_push(2));
    EXPECT_TRUE(queue.try_push(3));
    EXPECT_TRUE(queue.try_push(4));
    EXPECT_FALSE(queue.try_push(5));  // capacidad agotada
}

TEST(MpmcQueue, PushPop_RespetaOrdenFIFO) {
    nexus::MpmcQueue<int, 4> queue;
    ASSERT_TRUE(queue.try_push(10));
    ASSERT_TRUE(queue.try_push(20));
    ASSERT_TRUE(queue.try_push(30));
    EXPECT_EQ(queue.try_pop().value(), 10);
    EXPECT_EQ(queue.try_pop().value(), 20);
    EXPECT_EQ(queue.try_pop().value(), 30);
    EXPECT_FALSE(queue.try_pop().has_value());
}

TEST(MpmcQueue, AnilloReaprovecha_TrasVaciar) {
    nexus::MpmcQueue<int, 2> queue;
    ASSERT_TRUE(queue.try_push(1));
    ASSERT_TRUE(queue.try_push(2));
    EXPECT_FALSE(queue.try_push(3));
    EXPECT_EQ(queue.try_pop().value(), 1);
    EXPECT_TRUE(queue.try_push(3));  // hueco liberado, secuencia avanza
    EXPECT_EQ(queue.try_pop().value(), 2);
    EXPECT_EQ(queue.try_pop().value(), 3);
}

// Estrés MPMC: varios productores y consumidores. Cada entero 0..N-1 se produce una vez;
// se verifica que cada uno se consume **exactamente una vez** (sin pérdidas ni duplicados).
// Comprobación por conservación → no es flaky aunque el entrelazado sea no determinista.
TEST(MpmcQueue, MultiProductorConsumidor_CadaItemUnaVez) {
    constexpr int kProducers = 4;
    constexpr int kConsumers = 4;
    constexpr int kPerProducer = 20'000;
    constexpr int kTotal = kProducers * kPerProducer;

    nexus::MpmcQueue<int, 1024> queue;
    std::vector<std::atomic<int>> seen(kTotal);  // contador de veces que se ve cada valor
    std::atomic<int> consumed{0};

    std::vector<std::thread> threads;
    threads.reserve(kProducers + kConsumers);

    for (int p = 0; p < kProducers; ++p) {
        threads.emplace_back([&, p] {
            for (int i = 0; i < kPerProducer; ++i) {
                const int value = (p * kPerProducer) + i;
                while (!queue.try_push(value)) {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (int c = 0; c < kConsumers; ++c) {
        threads.emplace_back([&] {
            while (consumed.load(std::memory_order_relaxed) < kTotal) {
                if (auto value = queue.try_pop()) {
                    seen[static_cast<std::size_t>(*value)].fetch_add(1, std::memory_order_relaxed);
                    consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(consumed.load(), kTotal);
    for (int i = 0; i < kTotal; ++i) {
        ASSERT_EQ(seen[static_cast<std::size_t>(i)].load(), 1) << "valor " << i;
    }
}

}  // namespace
