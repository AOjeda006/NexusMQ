/// @file   reactor/allocator.hpp
/// @brief  ArenaAllocator: arena de asignación monótona por núcleo (gestión cruda confinada).
/// @ingroup reactor

#pragma once

#include <cstddef>
#include <memory>
#include <memory_resource>
#include <type_traits>
#include <utility>

namespace nexus {

/// @brief Arena de asignación **monótona**, propia de un reactor. Afinidad: REACTOR-LOCAL.
/// @details Envuelve un `std::pmr::monotonic_buffer_resource`: las asignaciones avanzan un
///   puntero dentro de bloques que crecen geométricamente desde el `upstream`, y se liberan
///   **todas de golpe** con `reset()` (coste O(1) amortizado, ideal para el ciclo de vida por
///   petición del hot-path). Es el tipo RAII que **confina** la gestión de memoria cruda
///   (sin `new`/`delete` a la vista; `make` usa `construct_at`), según la normativa.
/// @invariant **No ejecuta destructores**: `reset()` y el destructor reclaman la memoria en
///   bloque sin destruir los objetos. Por eso `make<T>` exige `T` trivialmente destruible —
///   colocar en la arena objetos que posean recursos (handles, `unique_ptr`…) los filtraría.
/// @note No es thread-safe ni movible (el `monotonic_buffer_resource` no lo es): un reactor por
///   instancia, accedida solo desde su propio hilo.
class ArenaAllocator {
public:
    /// Tamaño del primer bloque que la arena pide al upstream (luego crece geométricamente).
    static constexpr std::size_t kDefaultArenaBytes = std::size_t{64} * 1024;

    /// Construye sobre @p upstream con el tamaño de bloque inicial por defecto.
    explicit ArenaAllocator(std::pmr::memory_resource* upstream = std::pmr::get_default_resource())
        : ArenaAllocator(kDefaultArenaBytes, upstream) {}

    /// Construye con un bloque inicial de @p initial_size bytes pedido a @p upstream.
    explicit ArenaAllocator(std::size_t initial_size,
                            std::pmr::memory_resource* upstream = std::pmr::get_default_resource())
        : arena_(initial_size, upstream) {}

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&&) = delete;
    ArenaAllocator& operator=(ArenaAllocator&&) = delete;
    ~ArenaAllocator() = default;

    /// Recurso PMR de la arena (para contenedores `std::pmr::*` reactor-locales).
    [[nodiscard]] std::pmr::memory_resource* resource() noexcept { return &arena_; }

    /// @brief Libera **de golpe** toda la memoria asignada. No ejecuta destructores (ver arriba).
    void reset() noexcept { arena_.release(); }

    /// @brief Construye un `T` en la arena reenviando @p args a su constructor.
    /// @return Puntero al objeto; válido hasta el siguiente `reset()` o la destrucción de la arena.
    /// @note `T` ha de ser trivialmente destruible: la arena nunca llama a su destructor.
    template <class T, class... Args>
    [[nodiscard]] T* make(Args&&... args) {
        static_assert(std::is_trivially_destructible_v<T>,
                      "ArenaAllocator::make exige T trivialmente destruible (no ejecuta dtores)");
        void* storage = arena_.allocate(sizeof(T), alignof(T));
        return std::construct_at(static_cast<T*>(storage), std::forward<Args>(args)...);
    }

private:
    std::pmr::monotonic_buffer_resource arena_;
};

}  // namespace nexus
