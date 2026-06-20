/// @file   telemetry/tracing.cpp
/// @brief  Implementación del tracing distribuido: codec W3C `traceparent` + Tracer/Span (F8).
/// @ingroup telemetry

#include "telemetry/tracing.hpp"

#include <cstddef>
#include <random>
#include <utility>

namespace nexus {
namespace {

constexpr std::size_t kTraceparentLen = 55;  // 2 + 1 + 32 + 1 + 16 + 1 + 2
constexpr std::size_t kVersionEnd = 2;
constexpr std::size_t kTraceIdBegin = 3;
constexpr std::size_t kTraceIdEnd = 35;
constexpr std::size_t kSpanIdBegin = 36;
constexpr std::size_t kSpanIdEnd = 52;
constexpr std::size_t kFlagsBegin = 53;
constexpr int kHexDigitsPerU64 = 16;

/// Añade @p value como @p width dígitos hex en minúscula (relleno con ceros) al final de @p out.
void append_hex(std::string& out, std::uint64_t value, int width) {
    constexpr std::string_view kDigits = "0123456789abcdef";
    for (int shift = (width - 1) * 4; shift >= 0; shift -= 4) {
        out.push_back(kDigits[(value >> static_cast<unsigned>(shift)) & 0xFU]);
    }
}

/// Valor de un dígito hex, o -1 si @p ch no es hexadecimal.
[[nodiscard]] int hex_value(char ch) noexcept {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    if (ch >= 'a' && ch <= 'f') {
        return ch - 'a' + 10;
    }
    return -1;  // mayúsculas excluidas a propósito: W3C exige minúsculas.
}

/// Parsea @p text (solo dígitos hex) a un entero sin signo; `InvalidArgument` si hay un no-hex.
[[nodiscard]] expected<std::uint64_t> parse_hex(std::string_view text) {
    std::uint64_t value = 0;
    for (const char ch : text) {
        const int digit = hex_value(ch);
        if (digit < 0) {
            return make_error(ErrorCode::InvalidArgument, "traceparent: dígito hex inválido");
        }
        value = (value << 4U) | static_cast<std::uint64_t>(digit);
    }
    return value;
}

}  // namespace

std::string format_traceparent(const SpanContext& ctx) {
    std::string out;
    out.reserve(kTraceparentLen);
    append_hex(out, 0, 2);  // versión 00
    out.push_back('-');
    append_hex(out, ctx.trace_id.hi, kHexDigitsPerU64);
    append_hex(out, ctx.trace_id.lo, kHexDigitsPerU64);
    out.push_back('-');
    append_hex(out, ctx.span_id.value, kHexDigitsPerU64);
    out.push_back('-');
    append_hex(out, ctx.flags, 2);
    return out;
}

expected<SpanContext> parse_traceparent(std::string_view header) {
    if (header.size() != kTraceparentLen || header[kVersionEnd] != '-' ||
        header[kTraceIdEnd] != '-' || header[kSpanIdEnd] != '-') {
        return make_error(ErrorCode::InvalidArgument, "traceparent: forma inválida");
    }
    const expected<std::uint64_t> version = parse_hex(header.substr(0, kVersionEnd));
    if (!version || *version != 0) {
        return make_error(ErrorCode::InvalidArgument, "traceparent: versión no soportada");
    }
    const expected<std::uint64_t> hi = parse_hex(header.substr(kTraceIdBegin, kHexDigitsPerU64));
    const expected<std::uint64_t> lo =
        parse_hex(header.substr(kTraceIdBegin + kHexDigitsPerU64, kHexDigitsPerU64));
    const expected<std::uint64_t> span = parse_hex(header.substr(kSpanIdBegin, kHexDigitsPerU64));
    const expected<std::uint64_t> flags = parse_hex(header.substr(kFlagsBegin, 2));
    if (!hi || !lo || !span || !flags) {
        return make_error(ErrorCode::InvalidArgument, "traceparent: campo hex inválido");
    }

    SpanContext ctx;
    ctx.trace_id = TraceId{.hi = *hi, .lo = *lo};
    ctx.span_id = SpanId{.value = *span};
    ctx.flags = static_cast<std::uint8_t>(*flags);
    if (!ctx.valid()) {  // trace-id/span-id todo-cero son inválidos por norma.
        return make_error(ErrorCode::InvalidArgument, "traceparent: id todo-cero");
    }
    return ctx;
}

namespace {

/// Semilla a partir del dispositivo aleatorio del sistema (para el ctor por defecto).
[[nodiscard]] std::uint64_t seed_from_device() {
    std::random_device device;
    return (static_cast<std::uint64_t>(device()) << 32U) ^ device();
}

}  // namespace

RandomIdGenerator::RandomIdGenerator() : state_(seed_from_device()) {}

RandomIdGenerator::RandomIdGenerator(std::uint64_t seed) noexcept : state_(seed) {}

std::uint64_t RandomIdGenerator::next_nonzero() noexcept {
    // splitmix64: rápido, sin estado externo y bien distribuido (los ids no son criptográficos).
    // Se repite en el caso (astronómicamente raro) de obtener cero, que es un id inválido.
    std::uint64_t result = 0;
    while (result == 0) {
        state_ += 0x9e3779b97f4a7c15ULL;
        std::uint64_t mixed = state_;
        mixed = (mixed ^ (mixed >> 30U)) * 0xbf58476d1ce4e5b9ULL;
        mixed = (mixed ^ (mixed >> 27U)) * 0x94d049bb133111ebULL;
        result = mixed ^ (mixed >> 31U);
    }
    return result;
}

TraceId RandomIdGenerator::new_trace_id() {
    return TraceId{.hi = next_nonzero(), .lo = next_nonzero()};
}

SpanId RandomIdGenerator::new_span_id() {
    return SpanId{.value = next_nonzero()};
}

Span::Span(Tracer& tracer, SpanData data) noexcept : tracer_(&tracer), data_(std::move(data)) {}

Span::Span(Span&& other) noexcept : tracer_(other.tracer_), data_(std::move(other.data_)) {
    other.tracer_ = nullptr;
}

Span& Span::operator=(Span&& other) noexcept {
    if (this != &other) {
        end();  // cierra el span que se estuviera sosteniendo antes de adoptar el otro.
        tracer_ = other.tracer_;
        data_ = std::move(other.data_);
        other.tracer_ = nullptr;
    }
    return *this;
}

Span::~Span() {
    end();
}

void Span::set_attribute(Field attribute) {
    if (tracer_ != nullptr) {
        data_.attributes.push_back(std::move(attribute));
    }
}

void Span::end() noexcept {
    if (tracer_ == nullptr) {
        return;
    }
    Tracer* tracer = tracer_;
    tracer_ = nullptr;  // idempotente aun si el sink lanza: no se reintenta.
    try {
        data_.end = tracer->clock_();
        tracer->emit(data_);
    } catch (...) {  // NOLINT(bugprone-empty-catch): una traza no debe tumbar el proceso.
        // El reloj/sink inyectados podrían lanzar; se descarta el span silenciosamente.
    }
}

Tracer::Tracer(IdGenerator& id_gen, SpanSink sink, Clock clock)
    : id_gen_(id_gen),
      sink_(std::move(sink)),
      clock_(clock ? std::move(clock) : Clock{[] { return std::chrono::system_clock::now(); }}) {}

Span Tracer::start_root(std::string name, bool sampled) {
    SpanData data;
    data.context.trace_id = id_gen_.new_trace_id();
    data.context.span_id = id_gen_.new_span_id();
    data.context.flags = sampled ? kTraceFlagSampled : 0;
    data.parent_id = SpanId{};  // raíz: sin padre.
    data.name = std::move(name);
    data.start = clock_();
    return Span{*this, std::move(data)};
}

Span Tracer::start_child(const SpanContext& parent, std::string name) {
    SpanData data;
    data.context.trace_id = parent.trace_id;  // hereda la traza.
    data.context.span_id = id_gen_.new_span_id();
    data.context.flags = parent.flags;  // hereda el muestreo.
    data.parent_id = parent.span_id;
    data.name = std::move(name);
    data.start = clock_();
    return Span{*this, std::move(data)};
}

void Tracer::emit(const SpanData& data) {
    if (sink_) {
        sink_(data);
    }
}

}  // namespace nexus
