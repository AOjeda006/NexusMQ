/// @file   broker/producer_session.hpp
/// @brief  ProducerSession: máquina de estado de idempotencia por (producer_id, partición) (§5.9).
/// @ingroup broker

#pragma once

#include <cstdint>

#include "common/types.hpp"

namespace nexus {

/// @brief Estado de idempotencia de un productor en una partición. Afinidad: REACTOR-LOCAL.
/// @details Rastrea la última secuencia aceptada para descartar reintentos duplicados y detectar
///   huecos (§5.9): dado el `base_sequence` de un batch, `expected = last_sequence + 1`; si
///   coincide se **acepta**, si es menor el batch ya se consumió (**duplicado**, se reconoce sin
///   re-append), y si es mayor hay un **hueco** (`OUT_OF_ORDER_SEQUENCE`). El `epoch` permite
///   *fencing* de productores antiguos en una capa superior (no interviene en la comprobación de
///   secuencia).
/// @invariant Tras un batch aceptado `[base, base+count)`, `last_sequence` avanza a `base+count-1`.
class ProducerSession {
public:
    /// Secuencia centinela "sin registros aún": el primer `base_sequence` esperado es `0`.
    static constexpr Sequence kNoSequence = -1;

    /// Veredicto de la comprobación de secuencia de un batch entrante.
    enum class SeqCheck : std::uint8_t {
        Accept,     ///< Secuencia esperada: se anexa.
        Duplicate,  ///< Ya consumida por completo: se reconoce sin re-anexar.
        Gap,        ///< Hueco (o solapamiento parcial): `OUT_OF_ORDER_SEQUENCE`.
    };

    ProducerSession(ProducerId producer_id, std::int16_t epoch) noexcept
        : producer_id_(producer_id), epoch_(epoch) {}

    /// @brief Clasifica un batch `[base_seq, base_seq+count)` frente a la última secuencia
    /// aceptada.
    /// @details `Duplicate` solo si el batch entero ya se consumió; un solapamiento parcial
    ///   (`base_seq < expected < base_seq+count`) es irreconciliable y se trata como `Gap`.
    [[nodiscard]] SeqCheck check(Sequence base_seq, std::int32_t count) const noexcept {
        const std::int64_t expected = static_cast<std::int64_t>(last_sequence_) + 1;
        const std::int64_t base = base_seq;
        if (base == expected) {
            return SeqCheck::Accept;
        }
        if (base > expected) {
            return SeqCheck::Gap;
        }
        // base < expected: duplicado solo si todo el batch ya se consumió; si solapa, es hueco.
        const std::int64_t batch_end = base + static_cast<std::int64_t>(count);  // exclusivo
        return batch_end <= expected ? SeqCheck::Duplicate : SeqCheck::Gap;
    }

    /// Registra @p last_seq como la última secuencia aceptada (= `base_sequence + count - 1`).
    void advance(Sequence last_seq) noexcept { last_sequence_ = last_seq; }

    [[nodiscard]] ProducerId producer_id() const noexcept { return producer_id_; }
    [[nodiscard]] std::int16_t epoch() const noexcept { return epoch_; }
    [[nodiscard]] Sequence last_sequence() const noexcept { return last_sequence_; }

private:
    ProducerId producer_id_;
    std::int16_t epoch_;
    Sequence last_sequence_ = kNoSequence;
};

}  // namespace nexus
