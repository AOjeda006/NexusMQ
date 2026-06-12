#include "reactor/task.hpp"

#include <gtest/gtest.h>

#include <stdexcept>

namespace {

nexus::task<int> answer() {
    co_return 42;
}

nexus::task<int> plus_one(int value) {
    co_return value + 1;
}

// Encadena dos tasks por co_await: a=42, b=43 -> 85.
nexus::task<int> nested() {
    const int a = co_await answer();
    const int b = co_await plus_one(a);
    co_return a + b;
}

nexus::task<void> set_flag(bool& flag) {
    co_await answer();
    flag = true;
}

nexus::task<void> throws() {
    co_await answer();
    throw std::runtime_error("boom");
}

TEST(Task, SyncWait_CoReturn_DevuelveElValor) {
    EXPECT_EQ(nexus::sync_wait(answer()), 42);
}

TEST(Task, AwaitAnidado_PropagaResultados) {
    EXPECT_EQ(nexus::sync_wait(nested()), 85);
}

TEST(Task, TaskVoid_EjecutaElCuerpo) {
    bool flag = false;
    nexus::sync_wait(set_flag(flag));
    EXPECT_TRUE(flag);
}

TEST(Task, Excepcion_SePropagaAlEsperar) {
    EXPECT_THROW(nexus::sync_wait(throws()), std::runtime_error);
}

}  // namespace
