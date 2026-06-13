/// @file   broker/credit_window.hpp
/// @brief  CreditWindow: control de flujo por créditos (backpressure, §6.3 / §7.11 #4).
/// @ingroup broker

#pragma once

#include <coroutine>
#include <cstdint>
#include <utility>

namespace nexus {

/// @brief Ventana de créditos para *backpressure* end-to-end. Afinidad: REACTOR-LOCAL.
/// @details Realiza el patrón emisor del §7.11 #4: un emisor `co_await acquire(cost)` antes de
///   escribir; si no hay créditos suficientes **se frena** (se suspende, no descarta ni crece sin
///   límite). El receptor concede créditos al drenar su cola con `grant(n)`, que reanuda al emisor
///   frenado. Así las colas quedan **acotadas** y la latencia controlada bajo sobrecarga.
/// @note **Un solo emisor** por ventana (un `waiter_`), que es el modelo por conexión/partición de
///   esta fase. El cableado completo en la ruta de *push* a consumidores (gating de envíos según
///   los créditos concedidos) llega con el consumidor *streaming* asíncrono de Fase 2; aquí se
///   entrega y prueba el mecanismo. No es thread-safe: emisor y concesor viven en el mismo reactor.
/// @invariant `available() >= 0`; hay como mucho un `waiter_` suspendido.
class CreditWindow {
public:
    /// Crea la ventana con @p initial créditos (la ventana inicial anunciada).
    explicit CreditWindow(std::int32_t initial) noexcept : credits_(initial) {}

    /// @brief Awaitable de `acquire`: listo si hay créditos para @p cost; si no, suspende al
    /// emisor.
    /// @details Al reanudar (créditos suficientes) descuenta @p cost. Vive en el *frame* de la
    ///   corrutina mientras dura la suspensión (es el operando de `co_await`).
    class Acquire {
    public:
        Acquire(CreditWindow& window, std::int32_t cost) noexcept : window_(window), cost_(cost) {}

        [[nodiscard]] bool await_ready() const noexcept { return window_.credits_ >= cost_; }

        void await_suspend(std::coroutine_handle<> awaiting) const noexcept {
            window_.waiter_ = awaiting;
            window_.pending_cost_ = cost_;
        }

        void await_resume() const noexcept { window_.credits_ -= cost_; }

    private:
        CreditWindow& window_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
        std::int32_t cost_;
    };

    /// @brief Reserva @p cost créditos; suspende la corrutina llamante si no los hay (se frena).
    [[nodiscard]] Acquire acquire(std::int32_t cost) noexcept { return Acquire{*this, cost}; }

    /// @brief Concede @p n créditos; si con ello el emisor frenado ya cabe, lo reanuda.
    void grant(std::int32_t n) {
        credits_ += n;
        if (waiter_ && credits_ >= pending_cost_) {
            const std::coroutine_handle<> resumed = std::exchange(waiter_, {});
            pending_cost_ = 0;
            resumed.resume();  // ejecuta await_resume (descuenta el coste); puede volver a acquire.
        }
    }

    /// Créditos disponibles ahora mismo.
    [[nodiscard]] std::int32_t available() const noexcept { return credits_; }

    /// ¿Hay un emisor frenado esperando créditos?
    [[nodiscard]] bool has_waiter() const noexcept { return static_cast<bool>(waiter_); }

private:
    std::int32_t credits_;
    /// Coste que espera el emisor frenado (si lo hay).
    std::int32_t pending_cost_ = 0;
    /// Emisor suspendido esperando crédito (vacío si ninguno).
    std::coroutine_handle<> waiter_;
};

}  // namespace nexus
