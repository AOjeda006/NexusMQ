#include "reactor/scheduler.hpp"

#include <gtest/gtest.h>

#include <vector>

#include "common/task.hpp"

namespace {

nexus::task<void> count_with_yield(int& counter, nexus::CoroScheduler& scheduler) {
    ++counter;
    co_await nexus::yield_to(scheduler);
    ++counter;
}

nexus::task<void> label_yield(std::vector<int>& order, int label, nexus::CoroScheduler& scheduler) {
    order.push_back(label);
    co_await nexus::yield_to(scheduler);
    order.push_back(label);
}

TEST(CoroScheduler, RunReady_ReanudaTrasYield) {
    nexus::CoroScheduler scheduler;
    int counter = 0;
    auto coro = count_with_yield(counter, scheduler);
    scheduler.schedule(coro.handle());
    EXPECT_EQ(counter, 0);  // perezosa: nada se ejecuta hasta run_ready

    scheduler.run_ready();
    EXPECT_EQ(counter, 2);
    EXPECT_TRUE(scheduler.empty());
    EXPECT_TRUE(coro.done());
}

TEST(CoroScheduler, IntercalaDosCorrutinas_OrdenFIFO) {
    nexus::CoroScheduler scheduler;
    std::vector<int> order;
    auto a = label_yield(order, 1, scheduler);
    auto b = label_yield(order, 2, scheduler);
    scheduler.schedule(a.handle());
    scheduler.schedule(b.handle());

    scheduler.run_ready();
    // a corre hasta el yield, luego b; al reprogramarse, otra vez a y b.
    EXPECT_EQ(order, (std::vector<int>{1, 2, 1, 2}));
    EXPECT_TRUE(a.done());
    EXPECT_TRUE(b.done());
}

TEST(CoroScheduler, RunReady_ColaVacia_NoHaceNada) {
    nexus::CoroScheduler scheduler;
    EXPECT_EQ(scheduler.run_ready(), 0U);
    EXPECT_TRUE(scheduler.empty());
}

}  // namespace
