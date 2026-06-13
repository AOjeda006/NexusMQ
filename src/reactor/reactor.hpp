/// @file   reactor/reactor.hpp
/// @brief  Reactor: bucle thread-per-core dueño de proactor, scheduler, allocator y buzón.
/// @ingroup reactor

#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "common/move_only_function.hpp"
#include "common/task.hpp"
#include "io/proactor.hpp"
#include "reactor/allocator.hpp"
#include "reactor/cross_core.hpp"
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
    ///   paso (el desglose lo marcaba privado; ajuste anotado en la hoja de ruta).
    bool poll_once();

    /// @brief Programa @p coro en **este** reactor (arranque diferido). El reactor posee su frame y
    ///   lo libera cuando la corrutina termina. Solo desde el hilo del reactor.
    void spawn(task<void> coro);

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
    std::vector<task<void>> spawned_;  // frames de corrutinas detached que el reactor posee
    std::vector<Reactor*> peers_;  // no propietarios; los cablea el pool (indexados por core_id)
    std::atomic<bool> stopping_{false};
};

}  // namespace nexus
