/// @file   broker/producer_session.hpp
/// @brief  ProducerSession: idempotencia *effectively-once* por (producer_id, partición).
/// @ingroup broker

#pragma once

#include <cstdint>

#include "common/types.hpp"

namespace nexus {

/// @brief Estado de idempotencia *effectively-once* de un productor en una partición (§5.9).
///   Afinidad: REACTOR-LOCAL.
/// @details Rastrea la **época** y la **última secuencia** aceptadas para dar *effectively-once*:
///   - **Fencing por época.** Una época entrante **inferior** a la registrada pertenece a una
///     encarnación obsoleta del productor (reiniciado/expulsado) y se **rechaza** (`Fenced`). Una
///     época **superior** es una encarnación nueva: la secuencia se **reinicia** (el primer batch
///     debe empezar en `0`).
///   - **Dedup por secuencia.** Con la misma época, dado el `base_sequence` de un batch,
///     `expected = last_sequence + 1`; si coincide se **acepta**, si el batch entero ya se consumió
///     es **duplicado** (se reconoce sin re-anexar, devolviendo su offset **original**), y si hay
///     hueco o solape parcial es `Gap` (`OUT_OF_ORDER_SEQUENCE`).
/// @invariant Tras aceptar `[base, base+count)`, `last_sequence == base+count-1` y se memoriza el
///   `base_offset` asignado a ese batch (para responder a su reintento con el offset real).
class ProducerSession {
public:
    /// Secuencia centinela "sin registros aún": el primer `base_sequence` esperado es `0`.
    static constexpr Sequence kNoSequence = -1;

    /// Veredicto de la comprobación de (época, secuencia) de un batch entrante.
    enum class SeqCheck : std::uint8_t {
        Accept,     ///< Época y secuencia esperadas: se anexa.
        Duplicate,  ///< Ya consumido por completo: se reconoce sin re-anexar.
        Gap,        ///< Hueco (o solape parcial): `OUT_OF_ORDER_SEQUENCE`.
        Fenced,     ///< Época obsoleta: productor expulsado (`INVALID_PRODUCER_EPOCH`).
    };

    ProducerSession(ProducerId producer_id, std::int16_t epoch) noexcept
        : producer_id_(producer_id), epoch_(epoch) {}

    /// @brief Clasifica un batch `[base_seq, base_seq+count)` con época @p epoch, **sin mutar**.
    /// @details Una época inferior es `Fenced`; una superior reinicia la secuencia (el primer batch
    ///   debe empezar en `0`, en otro caso `Gap`); con la misma época aplica el dedup por secuencia
    ///   (un solape parcial, `base_seq < expected < base_seq+count`, es irreconciliable → `Gap`).
    [[nodiscard]] SeqCheck check(std::int16_t epoch, Sequence base_seq,
                                 std::int32_t count) const noexcept {
        if (epoch < epoch_) {
            return SeqCheck::Fenced;
        }
        if (epoch > epoch_) {
            // Nueva encarnación del productor: la secuencia se reinicia desde 0.
            return base_seq == 0 ? SeqCheck::Accept : SeqCheck::Gap;
        }
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

    /// @brief Registra un batch aceptado `[base_seq, base_seq+count)` ubicado en @p base_offset.
    /// @details Adopta @p epoch (igual o superior; `check` ya descartó las inferiores): una época
    ///   superior reinicia la secuencia. Memoriza `(base_seq → base_offset)` del último batch para
    ///   responder a su reintento con el offset original.
    void accept(std::int16_t epoch, Sequence base_seq, std::int32_t count,
                Offset base_offset) noexcept {
        epoch_ = epoch;
        last_sequence_ = base_seq + count - 1;
        last_base_sequence_ = base_seq;
        last_base_offset_ = base_offset;
    }

    /// @brief Offset base asignado a un batch ya aceptado cuyo `base_seq` recordamos; -1 si no.
    /// @details Solo se recuerda el **último** batch aceptado (caso típico: el productor reintenta
    ///   el batch en vuelo); un duplicado más antiguo devuelve -1 y el llamante recurre al log.
    [[nodiscard]] Offset duplicate_base_offset(Sequence base_seq) const noexcept {
        return base_seq == last_base_sequence_ ? last_base_offset_ : -1;
    }

    [[nodiscard]] ProducerId producer_id() const noexcept { return producer_id_; }
    [[nodiscard]] std::int16_t epoch() const noexcept { return epoch_; }
    [[nodiscard]] Sequence last_sequence() const noexcept { return last_sequence_; }

private:
    ProducerId producer_id_;
    std::int16_t epoch_;
    Sequence last_sequence_ = kNoSequence;
    /// Última asignación recordada para responder a un reintento duplicado con su offset real.
    Sequence last_base_sequence_ = kNoSequence;
    Offset last_base_offset_ = -1;
};

}  // namespace nexus
