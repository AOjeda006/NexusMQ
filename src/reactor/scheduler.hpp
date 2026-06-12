/// @file   reactor/scheduler.hpp
/// @brief  CoroScheduler: cola de corrutinas listas + cesión cooperativa.
/// @ingroup reactor

#pragma once

#include <coroutine>
#include <cstddef>
#include <deque>

namespace nexus {

/// @brief Cola FIFO de `coroutine_handle` listas para reanudar. Afinidad: REACTOR-LOCAL.
/// @details El reactor drena esta cola en cada vuelta. `run_ready` reanuda hasta vaciarla,
///   procesando también las que se programen durante la propia vuelta (cesión cooperativa).
class CoroScheduler {
public:
    /// Programa @p handle para reanudarse.
    void schedule(std::coroutine_handle<> handle) { ready_.push_back(handle); }

    /// @brief Reanuda las corrutinas listas hasta vaciar la cola.
    /// @return Cuántas reanudaciones se hicieron.
    std::size_t run_ready() {
        std::size_t resumed = 0;
        while (!ready_.empty()) {
            const std::coroutine_handle<> handle = ready_.front();
            ready_.pop_front();
            if (handle && !handle.done()) {
                handle.resume();
            }
            ++resumed;
        }
        return resumed;
    }

    [[nodiscard]] bool empty() const noexcept { return ready_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return ready_.size(); }

private:
    std::deque<std::coroutine_handle<>> ready_;
};

/// @brief Awaitable de **cesión cooperativa**: suspende la corrutina y la reprograma.
/// @details `co_await yield_to(sched)` devuelve el control al scheduler y se reanuda en una
///   vuelta posterior, permitiendo intercalar corrutinas en el mismo hilo.
struct YieldTo {
    // Vista no propietaria; el awaitable es efímero (vive dentro del co_await).
    CoroScheduler& scheduler;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)

    [[nodiscard]] static bool await_ready() noexcept { return false; }
    void await_suspend(std::coroutine_handle<> handle) const { scheduler.schedule(handle); }
    static void await_resume() noexcept {}
};

[[nodiscard]] inline YieldTo yield_to(CoroScheduler& scheduler) noexcept {
    return YieldTo{scheduler};
}

}  // namespace nexus
