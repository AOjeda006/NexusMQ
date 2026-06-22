/// @file   reactor/reactor.cpp
/// @brief  Implementación del bucle del Reactor (poll_once/run/spawn/submit_to/stop).
/// @ingroup reactor

#include "reactor/reactor.hpp"

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <utility>
#include <vector>

namespace nexus {

namespace {

// Tope de completions a drenar por giro y backstop de inactividad (cuando no hay nada que hacer, el
// reactor bloquea hasta este tope; un wake —cross-core o stop— lo despierta antes igualmente).
constexpr int kMaxCompletions = 256;
constexpr auto kIdleBackstop = std::chrono::seconds(1);

// Ejecuta el trabajo de un mensaje cross-core (vacío si el mensaje no lleva trabajo).
void run_message(Message& msg) {
    if (msg.work) {
        msg.work();
    }
}

}  // namespace

Reactor::Reactor(int core_id, int num_cores, std::unique_ptr<Proactor> proactor)
    : core_id_(core_id), proactor_(std::move(proactor)), mailbox_(num_cores, *proactor_) {}

Reactor::~Reactor() = default;

void Reactor::connect_peers(std::vector<Reactor*> peers) {
    peers_ = std::move(peers);
}

PeriodicTimers::Id Reactor::every(PeriodicTimers::Duration interval,
                                  PeriodicTimers::Callback callback) {
    return timers_.add(std::chrono::steady_clock::now(), interval, std::move(callback));
}

void Reactor::cancel_timer(PeriodicTimers::Id id) {
    timers_.cancel(id);
}

void Reactor::spawn(task<void> coro) {
    const std::coroutine_handle<> handle = coro.handle();
    if (!handle) {
        return;
    }
    spawned_.push_back(std::move(coro));  // el reactor posee el frame hasta que la corrutina acaba
    sched_.schedule(handle);              // arranque diferido al scheduler
}

void Reactor::submit_to(int core_id, Work work) {
    Reactor* target = peers_[static_cast<std::size_t>(core_id)];
    target->mailbox_.post(core_id_, Message{.target_core = core_id, .work = std::move(work)});
}

void Reactor::stop() noexcept {
    stopping_.store(true, std::memory_order_release);
    proactor_->wake();  // interrumpe la espera bloqueante para que run() vea el flag
}

bool Reactor::poll_once() {
    // 1) Reanuda corrutinas listas (spawn/yield) antes de bloquear.
    std::size_t resumed = sched_.run_ready();
    // 2) Entrega trabajo cross-core; puede programar corrutinas → vuelve a drenar las listas.
    const int delivered = mailbox_.drain(run_message);
    resumed += sched_.run_ready();
    // 3) Espera E/S: si hubo trabajo no bloquea (deadline = ahora); si no, bloquea hasta una
    //    completion / wake / el próximo temporizador (o el backstop). Las completions reanudan sus
    //    corrutinas en el acto.
    const bool busy = resumed > 0 || delivered > 0;
    const auto now = std::chrono::steady_clock::now();
    const auto deadline = busy ? now : timers_.next_deadline(now + kIdleBackstop);
    const int completions = proactor_->wait_completions(kMaxCompletions, deadline);
    // 4) Dispara los temporizadores vencidos (p. ej. el tick de Raft) y reanuda lo que encolen.
    const std::size_t fired = timers_.fire_due(std::chrono::steady_clock::now());
    // 5) Las completions/timers pueden haber programado corrutinas (p. ej. yield_to) → reanúdalas.
    resumed += sched_.run_ready();
    // 6) Recoge los frames de las corrutinas detached ya terminadas (libera su memoria).
    std::erase_if(spawned_, [](const task<void>& coro) { return coro.done(); });
    return resumed > 0 || delivered > 0 || completions > 0 || fired > 0;
}

void Reactor::run() {
    while (!stopping_.load(std::memory_order_acquire)) {
        poll_once();
    }
    // Apagado limpio: drena una última vez lo que quede listo / en el buzón.
    sched_.run_ready();
    mailbox_.drain(run_message);
    sched_.run_ready();
    std::erase_if(spawned_, [](const task<void>& coro) { return coro.done(); });
}

}  // namespace nexus
