/// @file   consensus/deferred_message_sink.hpp
/// @brief  DeferredMessageSink: sumidero de Raft que reenvía a un objetivo fijable (ADR-0025).
/// @ingroup consensus

#pragma once

#include "consensus/raft_carrier.hpp"  // RaftMessageSink
#include "consensus/raft_wire.hpp"

namespace nexus {

/// @brief Sumidero de Raft que **reenvía** a otro `RaftMessageSink` fijado a posteriori. Afinidad:
///   REACTOR-LOCAL.
/// @details Resuelve el desfase de tiempos del cableado: los `RaftCarrier` se construyen en el
/// plano
///   de control (al crear el topic), **antes** de que exista el transporte inter-nodo real (que
///   necesita el `Proactor` del reactor, disponible solo al arrancar). Los portadores referencian
///   este sumidero estable; el *composition root* le instala el `RaftTransport` con `set_target` al
///   arrancar el reactor. Sin objetivo, **descarta** los sobres (equivale al `NullMessageSink`): es
///   el comportamiento de un nodo aún sin transporte (votante único que se autoconfirma).
/// @note El objetivo se fija y se lee en el **hilo del reactor** (reactor-local): sin
/// sincronización.
class DeferredMessageSink final : public RaftMessageSink {
public:
    /// @brief Fija (o limpia con `nullptr`) el sumidero real al que se reenvían los sobres.
    void set_target(RaftMessageSink* target) noexcept { target_ = target; }

    /// @brief ¿Hay un objetivo instalado? (Sin él los sobres se descartan.)
    [[nodiscard]] bool has_target() const noexcept { return target_ != nullptr; }

    void send(const RaftEnvelope& envelope) override {
        if (target_ != nullptr) {
            target_->send(envelope);
        }
    }

private:
    RaftMessageSink* target_ = nullptr;  ///< Sumidero real (no propietario); `nullptr` = descarta.
};

}  // namespace nexus
