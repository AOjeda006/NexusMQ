/// @file   reactor/periodic_timers.hpp
/// @brief  PeriodicTimers: temporizadores periódicos del reactor (fuente del tick de Raft).
/// @ingroup reactor

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/move_only_function.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Conjunto de temporizadores periódicos conducidos por el bucle del reactor. Afinidad:
///   REACTOR-LOCAL.
/// @details El reactor no tenía cadencia propia (solo despertaba por E/S o `wake`); este componente
///   le da temporizadores periódicos para conducir trabajo por tiempo —en particular el `on_tick`
///   del `RaftCarrier` (vence *election*/*heartbeat*) desde el reactor dueño de la partición
///   (ADR-0025). La **lógica de vencimiento** es pura (recibe el `now`), de modo que se prueba de
///   forma determinista sin reloj real; el reactor le inyecta `steady_clock::now()`.
/// @invariant Cada temporizador tiene un `id` único y estable mientras viva; tras `fire_due` su
///   próximo vencimiento es `now + interval` (cadencia sin ráfaga de recuperación si se retrasa).
/// @note Confinado a un hilo (el del reactor): no es *thread-safe*. Un *callback* en curso **no**
///   debe `add`/`cancel` temporizadores (mutaría el conjunto que se está recorriendo).
class PeriodicTimers {
public:
    /// Función de tiempo: recibe el instante de disparo (evita releer el reloj en el callback).
    using Callback = MoveOnlyFunction<void(MonoTime)>;
    using Id = std::uint64_t;
    using Duration = MonoTime::duration;

    /// @brief Registra @p callback para dispararse cada @p interval; primer disparo en
    ///   @p now + @p interval.
    /// @return El identificador del temporizador (para `cancel`).
    Id add(MonoTime now, Duration interval, Callback callback);

    /// @brief Cancela el temporizador @p id (no-op si no existe).
    /// @pre No invocar desde dentro de un *callback* en ejecución.
    void cancel(Id id);

    /// @brief Menor vencimiento próximo, **acotado** a @p cap.
    /// @return El menor `next_due` si es anterior a @p cap; si no (o sin temporizadores), @p cap.
    [[nodiscard]] MonoTime next_deadline(MonoTime cap) const;

    /// @brief Dispara los temporizadores vencidos a @p now (invoca `callback(now)`) y los
    /// reprograma
    ///   a `now + interval`.
    /// @return Cuántos se dispararon.
    std::size_t fire_due(MonoTime now);

    [[nodiscard]] bool empty() const noexcept { return timers_.empty(); }

private:
    struct Timer {
        Id id;
        Duration interval;
        MonoTime next_due;
        Callback callback;
    };

    std::vector<Timer> timers_;
    Id next_id_ = 1;
};

}  // namespace nexus
