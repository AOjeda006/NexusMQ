/// @file   reactor/cross_core_call.hpp
/// @brief  call_on: ejecuta trabajo en otro reactor y reanuda al llamante con el resultado.
/// @ingroup reactor

#pragma once

#include <coroutine>
#include <optional>
#include <type_traits>
#include <utility>

#include "reactor/reactor.hpp"

namespace nexus {

/// @brief *Awaiter* de una llamada cross-core: corre @p fn en el reactor destino y reanuda la
///   corrutina llamante (en su propio reactor) con el resultado. Afinidad: CROSS-CORE.
/// @details Materializa una **petición/respuesta** entre núcleos sobre el paso de mensajes
///   (`Reactor::submit_to`, ADR-0005): al suspenderse, el reactor origen postea @p fn al destino;
///   el destino la ejecuta **en su hilo** (accede a su estado reactor-local) y postea de vuelta la
///   reanudación al origen, que entrega el resultado. La sincronización la da el buzón SPSC
///   (release al postear, acquire al drenar): el `result_` escrito en el destino es visible en el
///   origen tras la reanudación, sin candados. El *awaiter* vive en el frame de la corrutina (vivo
///   mientras está suspendida), así que las capturas de `this` son válidas en ambos núcleos.
/// @note @p fn debe tocar **solo** estado del reactor destino (o inmutable): corre en su hilo. El
///   resultado no puede ser `void` (devuelve por valor); para un efecto sin valor, devuelve un tipo
///   trivial (p. ej. un `bool`).
/// @invariant Un único viaje de ida y vuelta por `co_await`; el llamante se reanuda exactamente una
///   vez, en su propio reactor.
template <class Fn>
class CrossCoreCall {
public:
    using Result = std::invoke_result_t<Fn>;
    static_assert(!std::is_void_v<Result>, "call_on: fn debe devolver un valor (no void)");

    CrossCoreCall(Reactor& self, Reactor& target, Fn fn)
        : self_(self), target_(target), fn_(std::move(fn)) {}

    [[nodiscard]] bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> awaiting) {
        // Origen → destino: ejecuta `fn_` en el hilo del destino y postea de vuelta la reanudación.
        self_.submit_to(target_.core_id(), [this, awaiting]() mutable {
            result_.emplace(fn_());
            target_.submit_to(self_.core_id(), [awaiting]() mutable { awaiting.resume(); });
        });
    }

    [[nodiscard]] Result await_resume() { return std::move(*result_); }

private:
    Reactor& self_;    // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Reactor& target_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Fn fn_;
    std::optional<Result> result_;
};

/// @brief Ejecuta @p fn en el reactor @p target y reanuda al llamante (en @p self) con su
/// resultado.
/// @details Azúcar para `co_await call_on(self, target, fn)`. Es el primitivo de **enrutado
///   cross-core**: el dueño de una partición (`ReactorPool::reactor_for`) ejecuta la operación en
///   su propio núcleo y devuelve la respuesta al núcleo que atiende la conexión.
template <class Fn>
[[nodiscard]] CrossCoreCall<std::decay_t<Fn>> call_on(Reactor& self, Reactor& target, Fn&& fn) {
    return CrossCoreCall<std::decay_t<Fn>>{self, target, std::forward<Fn>(fn)};
}

}  // namespace nexus
