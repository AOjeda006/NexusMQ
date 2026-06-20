/// @file   telemetry/tracing.hpp
/// @brief  Tracing distribuido: contexto de traza W3C + spans con propagación (§7.6, F8).
/// @ingroup telemetry

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "common/error.hpp"
#include "telemetry/logging.hpp"

/// @brief **Tracing distribuido** (F8): identifica una operación que cruza componentes/servicios
/// con
///   un `trace_id` común y un árbol de `span`s. La propagación entre saltos usa el formato **W3C
///   Trace Context** (`traceparent`), que es el de facto y entiende cualquier *collector* moderno.
namespace nexus {

/// @brief Identificador de **traza** de 128 bits (W3C). Afinidad: INMUTABLE.
/// @details Se almacena como dos mitades de 64 bits (big-endian al formatear a hex). Todo-cero es
/// el
///   valor **inválido** que define la norma.
struct TraceId {
    std::uint64_t hi = 0;
    std::uint64_t lo = 0;

    [[nodiscard]] bool valid() const noexcept { return hi != 0 || lo != 0; }
    bool operator==(const TraceId&) const = default;
};

/// @brief Identificador de **span** de 64 bits (W3C). Afinidad: INMUTABLE. Todo-cero = inválido.
struct SpanId {
    std::uint64_t value = 0;

    [[nodiscard]] bool valid() const noexcept { return value != 0; }
    bool operator==(const SpanId&) const = default;
};

/// Bit 0 de las *trace-flags* W3C: la traza está **muestreada** (debe registrarse/exportarse).
inline constexpr std::uint8_t kTraceFlagSampled = 0x01;

/// @brief Contexto de traza que se **propaga** entre saltos (lo que viaja en `traceparent`).
///   Afinidad: INMUTABLE.
struct SpanContext {
    TraceId trace_id;
    SpanId span_id;  ///< El span "actual" (el padre del siguiente salto).
    std::uint8_t flags = 0;

    [[nodiscard]] bool sampled() const noexcept { return (flags & kTraceFlagSampled) != 0; }
    [[nodiscard]] bool valid() const noexcept { return trace_id.valid() && span_id.valid(); }
    bool operator==(const SpanContext&) const = default;
};

/// @brief Serializa @p ctx al encabezado **W3C `traceparent`** (versión `00`):
///   `00-<trace_id:32hex>-<span_id:16hex>-<flags:2hex>`.
[[nodiscard]] std::string format_traceparent(const SpanContext& ctx);

/// @brief Parsea un encabezado **W3C `traceparent`** (versión `00`) a un `SpanContext`.
/// @return `InvalidArgument` si la longitud/forma, los guiones, los dígitos hex, la versión o los
///   ids (todo-cero) son inválidos. Decodificador defensivo (entrada no confiable).
[[nodiscard]] expected<SpanContext> parse_traceparent(std::string_view header);

/// @brief Registro de un span **terminado** (lo que se entrega al *sink* para exportar). Afinidad:
///   INMUTABLE.
struct SpanData {
    SpanContext context;
    SpanId parent_id;  ///< Span padre (inválido si es raíz).
    std::string name;
    std::chrono::system_clock::time_point start;
    std::chrono::system_clock::time_point end;
    std::vector<Field> attributes;

    /// Duración del span (no negativa si `end >= start`).
    [[nodiscard]] std::chrono::nanoseconds duration() const noexcept { return end - start; }
};

/// @brief Generador de identificadores de traza/span. Afinidad: depende de la implementación.
/// @details Se **inyecta** para que las pruebas sean deterministas (un contador) y producción use
///   aleatoriedad. Un id generado debe ser **válido** (no todo-cero).
class IdGenerator {
public:
    IdGenerator() = default;
    IdGenerator(const IdGenerator&) = delete;
    IdGenerator& operator=(const IdGenerator&) = delete;
    IdGenerator(IdGenerator&&) = delete;
    IdGenerator& operator=(IdGenerator&&) = delete;
    virtual ~IdGenerator() = default;

    [[nodiscard]] virtual TraceId new_trace_id() = 0;
    [[nodiscard]] virtual SpanId new_span_id() = 0;
};

/// @brief Generador aleatorio de ids (producción): `mt19937_64` sembrado por `random_device`.
///   Afinidad: REACTOR-LOCAL (no thread-safe; uno por núcleo).
class RandomIdGenerator final : public IdGenerator {
public:
    RandomIdGenerator();
    explicit RandomIdGenerator(std::uint64_t seed) noexcept;

    [[nodiscard]] TraceId new_trace_id() override;
    [[nodiscard]] SpanId new_span_id() override;

private:
    [[nodiscard]] std::uint64_t next_nonzero() noexcept;

    std::uint64_t state_;
};

class Tracer;

/// @brief Span en curso (RAII): al destruirse —o al llamar `end()`— entrega su `SpanData` al *sink*
///   del `Tracer`. Afinidad: REACTOR-LOCAL. **Move-only** (un span no se copia; el movido-de no
///   emite).
class Span {
public:
    Span(const Span&) = delete;
    Span& operator=(const Span&) = delete;
    Span(Span&& other) noexcept;
    Span& operator=(Span&& other) noexcept;
    ~Span();

    /// Contexto a propagar a los hijos / al siguiente salto (`traceparent`).
    [[nodiscard]] const SpanContext& context() const noexcept { return data_.context; }
    [[nodiscard]] bool sampled() const noexcept { return data_.context.sampled(); }

    /// Adjunta un atributo (clave→valor) al span (se ignora tras `end()`).
    void set_attribute(Field attribute);

    /// Cierra el span y lo emite al *sink* (idempotente: un segundo `end()` no hace nada).
    /// `noexcept`: un span se cierra desde el destructor, así que **nunca** propaga excepciones
    /// (un fallo al exportar una traza no debe tumbar el proceso; se descarta).
    void end() noexcept;

private:
    friend class Tracer;
    Span(Tracer& tracer, SpanData data) noexcept;

    Tracer* tracer_;  ///< Nulo cuando ya se emitió o se movió de él.
    SpanData data_;
};

/// @brief Crea spans (raíz o hijos) y, al terminar, los entrega a un *sink*. Afinidad:
///   REACTOR-LOCAL.
/// @details Inyecta el generador de ids, el reloj y el *sink* (todo *test-friendly*). Un span hijo
///   **hereda** `trace_id` y `flags` del padre y estrena `span_id`; su `parent_id` es el `span_id`
///   del padre. Así se propaga la traza dentro del proceso; entre procesos viaja el `SpanContext`
///   en `traceparent` y se reanuda con `start_from_remote`.
class Tracer {
public:
    using Clock = std::function<std::chrono::system_clock::time_point()>;
    using SpanSink = std::function<void(const SpanData&)>;

    /// @param id_gen Generador de ids (no propietario; debe vivir más que el tracer).
    /// @param sink   Destino de los spans terminados (p. ej. log/exporter); por defecto, ninguno.
    /// @param clock  Fuente de tiempo (por defecto el reloj de sistema).
    explicit Tracer(IdGenerator& id_gen, SpanSink sink = {}, Clock clock = {});

    /// Inicia una traza **nueva** (span raíz): estrena `trace_id` y `span_id`; sin padre.
    /// @param sampled Si la traza se muestrea (se exporta).
    [[nodiscard]] Span start_root(std::string name, bool sampled = true);

    /// Inicia un span **hijo** de @p parent (hereda `trace_id`/`flags`, estrena `span_id`).
    [[nodiscard]] Span start_child(const SpanContext& parent, std::string name);

    /// Reanuda una traza recibida de otro proceso (mismo que `start_child`, semántica explícita).
    [[nodiscard]] Span start_from_remote(const SpanContext& remote, std::string name) {
        return start_child(remote, std::move(name));
    }

private:
    friend class Span;
    void emit(const SpanData& data);

    IdGenerator& id_gen_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    SpanSink sink_;
    Clock clock_;
};

}  // namespace nexus
