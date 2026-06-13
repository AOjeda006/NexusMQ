/// @file   reactor/cross_core.hpp
/// @brief  CrossCoreMailbox: buzones SPSC entrantes hacia un reactor + wake del destino.
/// @ingroup reactor

#pragma once

#include <cstddef>
#include <functional>
#include <vector>

#include "common/move_only_function.hpp"
#include "io/proactor.hpp"
#include "reactor/spsc_queue.hpp"

namespace nexus {

/// @brief Unidad de trabajo enviada de un reactor a otro. Afinidad: CROSS-CORE.
/// @details El modelo shared-nothing (ADR-0005) prohíbe el estado compartido: los reactores se
///   comunican **solo** pasando trabajo (`work`) que se ejecutará en el hilo del reactor destino.
struct Message {
    int target_core = -1;           ///< Núcleo destino (dueño del buzón que lo recibirá).
    MoveOnlyFunction<void()> work;  ///< Trabajo a ejecutar en el reactor destino.
};

/// @brief Buzón de entrada de **un** reactor: N colas SPSC (una por núcleo origen) + wake.
///   Afinidad: CROSS-CORE.
/// @details Cada núcleo origen escribe **solo** en su propia cola (`inboxes_[from_core]`), y el
///   reactor dueño es el **único** consumidor que las drena: así cada cola respeta el contrato
///   SPSC sin candados. Tras encolar, `post` despierta al destino (`Proactor::wake`) por si estaba
///   bloqueado esperando E/S. Es la materialización del paso de mensajes entre núcleos.
/// @invariant `inboxes_[i]` tiene un único productor (el núcleo `i`) y un único consumidor (el
///   reactor dueño). El número de buzones es fijo tras la construcción.
/// @note No copiable ni movible (contiene `SpscQueue` con átomos): un buzón por reactor.
class CrossCoreMailbox {
public:
    /// Capacidad de cada cola SPSC entrante (potencia de dos; ver `SpscQueue`).
    static constexpr std::size_t kInboxCapacity = 1024;

    /// Crea @p num_cores buzones (uno por núcleo origen) y guarda el proactor del destino.
    CrossCoreMailbox(int num_cores, Proactor& proactor);

    CrossCoreMailbox(const CrossCoreMailbox&) = delete;
    CrossCoreMailbox& operator=(const CrossCoreMailbox&) = delete;
    CrossCoreMailbox(CrossCoreMailbox&&) = delete;
    CrossCoreMailbox& operator=(CrossCoreMailbox&&) = delete;
    ~CrossCoreMailbox() = default;

    /// @brief Encola @p msg desde @p from_core y despierta al reactor destino.
    /// @details Solo el núcleo @p from_core debe llamar a esta sobre su propio buzón (SPSC). Si el
    ///   buzón estuviera lleno, aplica contrapresión cediendo el hilo hasta que haya hueco.
    void post(int from_core, Message msg);

    /// @brief Drena **todos** los buzones invocando @p handler por mensaje (orden FIFO por origen).
    /// @return Cuántos mensajes se procesaron. Solo lo llama el reactor dueño (único consumidor).
    int drain(const std::function<void(Message&)>& handler);

    /// Número de buzones (uno por núcleo origen).
    [[nodiscard]] int core_count() const noexcept { return static_cast<int>(inboxes_.size()); }

private:
    using Inbox = SpscQueue<Message, kInboxCapacity>;

    std::vector<Inbox> inboxes_;
    // Referencia al puerto del reactor destino (para wake). El buzón es no-movible, así que la
    // referencia-miembro es segura aquí.
    Proactor& proactor_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

}  // namespace nexus
