/// @file   reactor/mpmc_queue.hpp
/// @brief  MpmcQueue<T, Cap>: anillo lock-free multi-productor / multi-consumidor (Vyukov).
/// @ingroup reactor

#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <optional>
#include <utility>

#include "reactor/cache_line.hpp"

namespace nexus {

/// @brief Cola anular lock-free MPMC (Vyukov). Afinidad: THREAD-SAFE.
/// @details Anillo acotado de celdas, cada una con un **número de secuencia** propio que
///   actúa de contador de versión: distingue las generaciones de una misma posición y
///   elimina el problema **ABA** sin necesidad de punteros con etiqueta. `enqueue_pos_` y
///   `dequeue_pos_` (cada uno en su línea de caché) reparten huecos entre productores y
///   consumidores vía `compare_exchange`; el `release` sobre la secuencia publica el dato
///   y el `acquire` del lado contrario lo observa. Algoritmo de Dmitry Vyukov.
/// @invariant `Cap` es potencia de dos y `>= 2`.
/// @note `T` debe ser por defecto-construible (lo exige `std::array`) y movible.
template <class T, std::size_t Cap>
class MpmcQueue {
    static_assert(Cap >= 2, "Cap debe ser >= 2");
    static_assert((Cap & (Cap - 1)) == 0, "Cap debe ser potencia de dos");

public:
    MpmcQueue() {
        for (std::size_t i = 0; i < Cap; ++i) {
            cells_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    /// @brief Encola @p value. Llamable desde **varios productores** a la vez.
    /// @return `false` si la cola está llena.
    [[nodiscard]] bool try_push(T value) {
        std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        Cell* cell = nullptr;
        for (;;) {
            cell = &cells_[pos & kMask];
            const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);
            if (diff == 0) {
                // Celda libre para esta generación: intenta reservar el hueco.
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;  // llena (la celda aún la ocupa una generación anterior)
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);  // otro productor se adelantó
            }
        }
        cell->storage = std::move(value);
        cell->sequence.store(pos + 1, std::memory_order_release);  // visible al consumidor
        return true;
    }

    /// @brief Desencola un elemento. Llamable desde **varios consumidores** a la vez.
    /// @return `std::nullopt` si la cola está vacía.
    [[nodiscard]] std::optional<T> try_pop() {
        std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        Cell* cell = nullptr;
        for (;;) {
            cell = &cells_[pos & kMask];
            const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            const auto diff =
                static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + 1);
            if (diff == 0) {
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return std::nullopt;  // vacía
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);  // otro consumidor se adelantó
            }
        }
        std::optional<T> value(std::move(cell->storage));
        cell->sequence.store(pos + Cap,
                             std::memory_order_release);  // libera para la próxima vuelta
        return value;
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Cap; }

private:
    struct Cell {
        std::atomic<std::size_t> sequence;
        T storage{};
    };
    static constexpr std::size_t kMask = Cap - 1;

    alignas(kCacheLineSize) std::array<Cell, Cap> cells_{};
    alignas(kCacheLineSize) std::atomic<std::size_t> enqueue_pos_{0};
    alignas(kCacheLineSize) std::atomic<std::size_t> dequeue_pos_{0};
};

}  // namespace nexus
