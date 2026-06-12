/// @file   reactor/task.hpp
/// @brief  task<T>: tipo de retorno de corrutina perezoso con transferencia simétrica.
/// @ingroup reactor

#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

namespace nexus {

template <class T = void>
class task;  // NOLINT(readability-identifier-naming): tipo de vocabulario en minúscula (estilo std)

namespace detail {

/// Al terminar la corrutina, transfiere simétricamente a su continuación (o a noop si no hay).
struct FinalAwaiter {
    [[nodiscard]] static bool await_ready() noexcept { return false; }

    template <class Promise>
    [[nodiscard]] static std::coroutine_handle<> await_suspend(
        std::coroutine_handle<Promise> coro) noexcept {
        const std::coroutine_handle<> continuation = coro.promise().continuation;
        return continuation ? continuation : std::noop_coroutine();
    }

    static void await_resume() noexcept {}
};

/// Promesa de `task<T>` (T != void): guarda el valor o la excepción.
template <class T>
class TaskPromise {
public:
    task<T> get_return_object() noexcept;
    [[nodiscard]] static std::suspend_always initial_suspend() noexcept { return {}; }
    [[nodiscard]] static FinalAwaiter final_suspend() noexcept { return {}; }
    void return_value(T value) { value_.emplace(std::move(value)); }
    void unhandled_exception() noexcept { exception_ = std::current_exception(); }

    [[nodiscard]] T result() {
        if (exception_) {
            std::rethrow_exception(exception_);
        }
        return std::move(*value_);
    }

    std::coroutine_handle<> continuation;

private:
    std::optional<T> value_;
    std::exception_ptr exception_;
};

/// Especialización de la promesa para `task<void>`.
template <>
class TaskPromise<void> {
public:
    task<void> get_return_object() noexcept;
    [[nodiscard]] static std::suspend_always initial_suspend() noexcept { return {}; }
    [[nodiscard]] static FinalAwaiter final_suspend() noexcept { return {}; }
    static void return_void() noexcept {}
    void unhandled_exception() noexcept { exception_ = std::current_exception(); }

    void result() const {
        if (exception_) {
            std::rethrow_exception(exception_);
        }
    }

    std::coroutine_handle<> continuation;

private:
    std::exception_ptr exception_;
};

}  // namespace detail

/// @brief Corrutina perezosa que produce un `T`. Afinidad: REACTOR-LOCAL.
/// @details Solo movible: posee el `coroutine_handle` y lo destruye en el destructor. **Perezosa**
///   (`initial_suspend` suspende): no ejecuta nada hasta que se `co_await` o se arranca. Al
///   completarse reanuda su continuación por **transferencia simétrica** (sin crecer la pila).
/// @invariant Tras `co_await`, devuelve el valor o relanza la excepción capturada.
template <class T>
class [[nodiscard]] task {
public:
    // NOLINTNEXTLINE(readability-identifier-naming): nombre exigido por el lenguaje.
    using promise_type = detail::TaskPromise<T>;
    using Handle = std::coroutine_handle<promise_type>;

    task() noexcept = default;
    explicit task(Handle handle) noexcept : handle_(handle) {}

    task(task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    task& operator=(task&& other) noexcept {
        if (this != &other) {
            destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }
    task(const task&) = delete;
    task& operator=(const task&) = delete;
    ~task() { destroy(); }

    /// `co_await` sobre la task: registra al que espera como continuación y reanuda la task.
    auto operator co_await() && noexcept {
        struct Awaiter {
            Handle coro;

            [[nodiscard]] bool await_ready() const noexcept { return !coro || coro.done(); }

            [[nodiscard]] std::coroutine_handle<> await_suspend(
                std::coroutine_handle<> awaiting) const noexcept {
                coro.promise().continuation = awaiting;
                return coro;  // transferencia simétrica: arranca la task
            }

            decltype(auto) await_resume() const { return coro.promise().result(); }
        };
        return Awaiter{handle_};
    }

    [[nodiscard]] bool done() const noexcept { return !handle_ || handle_.done(); }
    [[nodiscard]] Handle handle() const noexcept { return handle_; }
    /// Cede la propiedad del handle (p. ej. para que lo gestione el scheduler).
    [[nodiscard]] Handle release() noexcept { return std::exchange(handle_, {}); }

private:
    void destroy() noexcept {
        if (handle_) {
            handle_.destroy();
        }
    }

    Handle handle_;
};

namespace detail {

template <class T>
task<T> TaskPromise<T>::get_return_object() noexcept {
    return task<T>{std::coroutine_handle<TaskPromise<T>>::from_promise(*this)};
}

inline task<void> TaskPromise<void>::get_return_object() noexcept {
    return task<void>{std::coroutine_handle<TaskPromise<void>>::from_promise(*this)};
}

}  // namespace detail

/// @brief Ejecuta @p coro hasta completarse en el hilo actual y devuelve su resultado.
/// @details Para tests y la raíz de un flujo síncrono: solo es válido si la task no se suspende
///   en E/S asíncrona pendiente (las que solo encadenan otras tasks completan aquí mismo).
template <class T>
decltype(auto) sync_wait(task<T> coro) {
    coro.handle().resume();
    return coro.handle().promise().result();
}

}  // namespace nexus
