/// @file   reactor/periodic_timers.cpp
/// @brief  Implementación de PeriodicTimers (vencimiento y disparo periódico).
/// @ingroup reactor

#include "reactor/periodic_timers.hpp"

#include <algorithm>
#include <utility>

namespace nexus {

PeriodicTimers::Id PeriodicTimers::add(MonoTime now, Duration interval, Callback callback) {
    const Id id = next_id_++;
    timers_.push_back(Timer{.id = id,
                            .interval = interval,
                            .next_due = now + interval,
                            .callback = std::move(callback)});
    return id;
}

void PeriodicTimers::cancel(Id id) {
    std::erase_if(timers_, [id](const Timer& timer) { return timer.id == id; });
}

MonoTime PeriodicTimers::next_deadline(MonoTime cap) const {
    MonoTime earliest = cap;
    for (const Timer& timer : timers_) {
        earliest = std::min(earliest, timer.next_due);
    }
    return earliest;
}

std::size_t PeriodicTimers::fire_due(MonoTime now) {
    std::size_t fired = 0;
    // Recorrido por índice (no por iterador): el conjunto no se muta durante el disparo (los
    // callbacks no añaden/cancelan, ver invariante). Reprograma a `now + interval` para no
    // encadenar disparos de recuperación si una ronda llegó tarde.
    for (Timer& timer : timers_) {
        if (timer.next_due <= now) {
            timer.callback(now);
            timer.next_due = now + timer.interval;
            ++fired;
        }
    }
    return fired;
}

}  // namespace nexus
