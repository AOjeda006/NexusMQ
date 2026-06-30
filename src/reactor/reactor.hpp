/// @file   reactor/reactor.hpp
/// @brief  Reactor: bucle thread-per-core dueño de proactor, scheduler, allocator y buzón.
/// @ingroup reactor

#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "common/move_only_function.hpp"
#include "common/task.hpp"
#include "common/types.hpp"
#include "io/proactor.hpp"
#include "reactor/allocator.hpp"
#include "reactor/cross_core.hpp"
#include "reactor/periodic_timers.hpp"
#include "reactor/scheduler.hpp"

namespace nexus {

/// @brief Reactor de un núcleo: dueño de proactor, scheduler, arena y buzón. REACTOR-LOCAL.
/// @details Materializa el modelo *thread-per-core* shared-nothing (ADR-0005): un reactor por
///   núcleo, todo su estado reactor-local y comunicación con otros reactores **solo** por el
///   `CrossCoreMailbox`. El bucle `run()` repite `poll_once()`: reanuda corrutinas listas, entrega
///   el trabajo cross-core y espera E/S de forma **bloqueante** cuando está ocioso (cede la CPU; un
///   `wake` —cross-core o `stop`— lo despierta al instante). Las completions del proactor reanudan
///   sus corrutinas en el acto (la E/S asíncrona se sirve sin pasar por el scheduler).
/// @invariant `spawn`, `poll_once`, `run` y los accesores van **solo** en el hilo del reactor
///   (el estado interno no es thread-safe). El único camino seguro desde otro hilo es `submit_to`
///   (vía buzón SPSC) y `stop` (atómico + `wake`).
/// @note No copiable ni movible (posee átomos, buzón y corrutinas en vuelo).
class Reactor {
public:
    /// Trabajo diferido que se ejecuta en el hilo del reactor destino (cross-core).
    using Work = MoveOnlyFunction<void()>;

    /// @brief Crea el reactor del núcleo @p core_id, con @p num_cores buzones de entrada (uno por
    ///   núcleo origen) y el @p proactor que poseerá.
    Reactor(int core_id, int num_cores, std::unique_ptr<Proactor> proactor);
    ~Reactor();

    Reactor(const Reactor&) = delete;
    Reactor& operator=(const Reactor&) = delete;
    Reactor(Reactor&&) = delete;
    Reactor& operator=(Reactor&&) = delete;

    /// @brief Cablea los peers (indexados por `core_id`) para `submit_to`. Lo llama el pool una vez
    ///   creados todos; el reactor **no** los posee (referencias no propietarias).
    void connect_peers(std::vector<Reactor*> peers);

    /// Ejecuta el bucle hasta `stop()`; al salir drena lo pendiente (apagado limpio).
    void run();

    /// @brief Un giro del bucle: lista → buzón → espera E/S. @return si hizo algún trabajo.
    /// @note Público para poder embeber el reactor en otro bucle y para tests deterministas paso a
    ///   paso (el diseño original lo marcaba privado).
    bool poll_once();

    /// @brief Programa @p coro en **este** reactor (arranque diferido). El reactor posee su frame y
    ///   lo libera cuando la corrutina termina. Solo desde el hilo del reactor.
    void spawn(task<void> coro);

    /// @brief Registra @p callback para dispararse cada @p interval desde el bucle del reactor
    ///   (fuente del `on_tick` de Raft). Solo desde el hilo del reactor.
    /// @return El id del temporizador (para `cancel_timer`).
    PeriodicTimers::Id every(PeriodicTimers::Duration interval, PeriodicTimers::Callback callback);

    /// @brief Cancela el temporizador @p id. Solo desde el hilo del reactor; no desde un callback.
    void cancel_timer(PeriodicTimers::Id id);

    /// @brief Envía @p work al reactor @p core_id (a su buzón) y lo despierta. Cross-core.
    void submit_to(int core_id, Work work);

    /// @brief Solicita el apagado: marca el flag y despierta el bucle (seguro desde otro hilo).
    void stop() noexcept;

    [[nodiscard]] int core_id() const noexcept { return core_id_; }
    [[nodiscard]] ArenaAllocator& allocator() noexcept { return alloc_; }
    [[nodiscard]] Proactor& proactor() noexcept { return *proactor_; }

private:
    int core_id_;
    std::unique_ptr<Proactor> proactor_;
    CoroScheduler sched_;
    ArenaAllocator alloc_;
    CrossCoreMailbox mailbox_;
    PeriodicTimers timers_;  // temporizadores periódicos conducidos por poll_once (tick de Raft)
    std::vector<task<void>> spawned_;  // frames de corrutinas detached que el reactor posee
    std::vector<Reactor*> peers_;  // no propietarios; los cablea el pool (indexados por core_id)
    std::atomic<bool> stopping_{false};
};

}  // namespace nexus
