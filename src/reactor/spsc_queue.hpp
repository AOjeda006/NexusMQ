/// @file   reactor/spsc_queue.hpp
/// @brief  SpscQueue<T, Cap>: anillo lock-free de un productor / un consumidor.
/// @ingroup reactor

#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <optional>
#include <utility>

#include "reactor/cache_line.hpp"

namespace nexus {

/// @brief Cola anular lock-free SPSC (un productor, un consumidor). Afinidad: CROSS-CORE.
/// @details `head_` (consumidor) y `tail_` (productor) son contadores **monótonos**; el
///   índice físico en `buf_` es `pos & kMask`. Cada átomo vive en su propia línea de caché
///   (`alignas(kCacheLineSize)`) para evitar *false sharing*: el productor solo escribe
///   `tail_`, el consumidor solo `head_`. El acoplamiento release/acquire publica el dato
///   antes de hacer visible el avance del índice al otro hilo.
/// @invariant `Cap` es potencia de dos y `>= 2`; a lo sumo `Cap` elementos en vuelo.
/// @note `T` debe ser por defecto-construible (lo exige `std::array`) y movible.
template <class T, std::size_t Cap>
class SpscQueue {
    static_assert(Cap >= 2, "Cap debe ser >= 2");
    static_assert((Cap & (Cap - 1)) == 0, "Cap debe ser potencia de dos");

public:
    /// @brief Encola @p value. Solo el **productor** puede llamarlo.
    /// @return `false` si la cola está llena (no se modifica nada).
    [[nodiscard]] bool try_push(T value) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);  // propio del productor
        const std::size_t head = head_.load(std::memory_order_acquire);
        if (tail - head == Cap) {
            return false;  // llena
        }
        buf_[tail & kMask] = std::move(value);
        tail_.store(tail + 1, std::memory_order_release);  // publica el dato escrito arriba
        return true;
    }

    /// @brief Desencola el siguiente elemento. Solo el **consumidor** puede llamarlo.
    /// @return `std::nullopt` si la cola está vacía.
    [[nodiscard]] std::optional<T> try_pop() {
        const std::size_t head = head_.load(std::memory_order_relaxed);  // propio del consumidor
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        if (head == tail) {
            return std::nullopt;  // vacía
        }
        std::optional<T> value(std::move(buf_[head & kMask]));
        head_.store(head + 1, std::memory_order_release);  // libera la celda al productor
        return value;
    }

    /// @brief Tamaño aproximado: orientativo, puede quedar obsoleto en cuanto se lee.
    [[nodiscard]] std::size_t size_approx() const noexcept {
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        const std::size_t head = head_.load(std::memory_order_acquire);
        return tail - head;
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Cap; }

private:
    static constexpr std::size_t kMask = Cap - 1;

    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};  // lo avanza el consumidor
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};  // lo avanza el productor
    std::array<T, Cap> buf_{};
};

}  // namespace nexus
