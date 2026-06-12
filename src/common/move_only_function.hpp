/// @file   common/move_only_function.hpp
/// @brief  MoveOnlyFunction: callable type-erased solo-movible (portátil GCC/Clang).
/// @ingroup common

#pragma once

#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace nexus {

template <class Signature>
class MoveOnlyFunction;  // sin definición primaria: solo se usa la especialización R(Args...)

/// @brief Envoltura de un callable **solo-movible** con borrado de tipo. Afinidad: INMUTABLE.
/// @details Equivale a `std::move_only_function` (C++23), que **no está en libc++ 21** (sí en
///   libstdc++); se reimplementa para compilar con ambos toolchains. Permite almacenar lambdas
///   que capturan recursos no copiables (handles, `unique_ptr`…), justo lo que necesitan las
///   *completions* del `Proactor` y el trabajo del `CrossCoreMailbox`. El callable vive tras un
///   `unique_ptr` (toda la gestión cruda confinada en RAII; sin `new`/`delete` a la vista).
/// @invariant Invocar una instancia vacía es **comportamiento indefinido** (igual que el estándar);
///   usar `operator bool` para comprobarlo.
template <class R, class... Args>
class MoveOnlyFunction<R(Args...)> {
public:
    MoveOnlyFunction() noexcept = default;
    MoveOnlyFunction(std::nullptr_t) noexcept {}  // NOLINT(google-explicit-constructor): como std

    /// Construye a partir de cualquier callable invocable como `R(Args...)`.
    template <class F>
        requires(!std::is_same_v<std::remove_cvref_t<F>, MoveOnlyFunction> &&
                 std::is_invocable_r_v<R, std::remove_cvref_t<F>&, Args...>)
    MoveOnlyFunction(F&& callable)  // NOLINT(google-explicit-constructor): conversión, como std
        : impl_(std::make_unique<Holder<std::remove_cvref_t<F>>>(std::forward<F>(callable))) {}

    MoveOnlyFunction(MoveOnlyFunction&&) noexcept = default;
    MoveOnlyFunction& operator=(MoveOnlyFunction&&) noexcept = default;
    MoveOnlyFunction(const MoveOnlyFunction&) = delete;
    MoveOnlyFunction& operator=(const MoveOnlyFunction&) = delete;
    ~MoveOnlyFunction() = default;

    /// Invoca el callable almacenado. @pre `*this` no está vacío.
    R operator()(Args... args) { return impl_->call(std::forward<Args>(args)...); }

    [[nodiscard]] explicit operator bool() const noexcept { return impl_ != nullptr; }

private:
    /// Interfaz de borrado de tipo: oculta el tipo concreto del callable.
    struct Concept {
        Concept() = default;
        Concept(const Concept&) = delete;
        Concept& operator=(const Concept&) = delete;
        Concept(Concept&&) = delete;
        Concept& operator=(Concept&&) = delete;
        virtual ~Concept() = default;
        virtual R call(Args... args) = 0;
    };

    /// Implementación concreta para un callable `F`.
    template <class F>
    struct Holder final : Concept {
        explicit Holder(F callable) : fn(std::move(callable)) {}
        R call(Args... args) override { return std::invoke(fn, std::forward<Args>(args)...); }

        F fn;
    };

    std::unique_ptr<Concept> impl_;
};

}  // namespace nexus
