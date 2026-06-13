/// @file   reactor/cross_core.cpp
/// @brief  Implementación de CrossCoreMailbox (post con contrapresión + drain FIFO por origen).
/// @ingroup reactor

#include "reactor/cross_core.hpp"

#include <cassert>
#include <cstddef>
#include <optional>
#include <thread>
#include <utility>

namespace nexus {

CrossCoreMailbox::CrossCoreMailbox(int num_cores, Proactor& proactor)
    : inboxes_(static_cast<std::size_t>(num_cores)), proactor_(proactor) {}

void CrossCoreMailbox::post(int from_core, Message msg) {
    Inbox& inbox = inboxes_[static_cast<std::size_t>(from_core)];

    // Como único productor de este buzón, si está lleno cedemos el hilo y pedimos al destino que
    // drene; `size_approx()` sobreestima la ocupación (el consumidor solo libera huecos), así que
    // en cuanto baja de la capacidad hay sitio garantizado para nuestro push.
    while (inbox.size_approx() >= Inbox::capacity()) {
        proactor_.wake();
        std::this_thread::yield();
    }

    const bool pushed = inbox.try_push(std::move(msg));
    assert(pushed && "buzón SPSC: productor único con hueco comprobado");
    static_cast<void>(pushed);  // usado por el assert; evita aviso de no-usado con NDEBUG

    proactor_.wake();  // hay trabajo nuevo: despierta al reactor destino si estaba bloqueado
}

int CrossCoreMailbox::drain(const std::function<void(Message&)>& handler) {
    int drained = 0;
    for (Inbox& inbox : inboxes_) {
        while (std::optional<Message> msg = inbox.try_pop()) {
            handler(*msg);
            ++drained;
        }
    }
    return drained;
}

}  // namespace nexus
