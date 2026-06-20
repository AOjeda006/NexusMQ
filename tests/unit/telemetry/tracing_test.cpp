// Pruebas del tracing distribuido: codec W3C traceparent + Tracer/Span con propagación — F8.
#include "telemetry/tracing.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace {

using nexus::format_traceparent;
using nexus::IdGenerator;
using nexus::parse_traceparent;
using nexus::SpanContext;
using nexus::SpanData;
using nexus::SpanId;
using nexus::TraceId;
using nexus::Tracer;

/// Generador determinista: entrega ids de un guion incremental (pruebas reproducibles).
class CountingIdGenerator : public IdGenerator {
public:
    TraceId new_trace_id() override {
        ++traces_;
        return TraceId{.hi = traces_, .lo = traces_ + 100};
    }
    SpanId new_span_id() override {
        ++spans_;
        return SpanId{.value = spans_};
    }

private:
    std::uint64_t traces_ = 0;
    std::uint64_t spans_ = 0;
};

TEST(Traceparent, Format_ProducesCanonicalW3cString) {
    SpanContext ctx;
    ctx.trace_id = TraceId{.hi = 0x0af7651916cd43ddULL, .lo = 0x8448eb211c80319cULL};
    ctx.span_id = SpanId{.value = 0xb7ad6b7169203331ULL};
    ctx.flags = nexus::kTraceFlagSampled;

    EXPECT_EQ(format_traceparent(ctx), "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
}

TEST(Traceparent, Parse_RoundTripsTheCanonicalExample) {
    const std::string header = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    const auto ctx = parse_traceparent(header);
    ASSERT_TRUE(ctx.has_value());
    EXPECT_EQ(ctx->trace_id.hi, 0x0af7651916cd43ddULL);
    EXPECT_EQ(ctx->trace_id.lo, 0x8448eb211c80319cULL);
    EXPECT_EQ(ctx->span_id.value, 0xb7ad6b7169203331ULL);
    EXPECT_TRUE(ctx->sampled());
    EXPECT_EQ(format_traceparent(*ctx), header);
}

TEST(Traceparent, Parse_RejectsMalformedHeaders) {
    EXPECT_FALSE(parse_traceparent("").has_value());                 // vacío
    EXPECT_FALSE(parse_traceparent("00-too-short-01").has_value());  // longitud
    // versión no soportada (ff)
    EXPECT_FALSE(
        parse_traceparent("ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01").has_value());
    // trace-id todo-cero
    EXPECT_FALSE(
        parse_traceparent("00-00000000000000000000000000000000-b7ad6b7169203331-01").has_value());
    // span-id todo-cero
    EXPECT_FALSE(
        parse_traceparent("00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01").has_value());
    // dígito no hex (mayúscula prohibida)
    EXPECT_FALSE(
        parse_traceparent("00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01").has_value());
}

TEST(Tracer, StartRoot_NewTraceWithoutParent) {
    CountingIdGenerator ids;
    Tracer tracer{ids};

    const auto span = tracer.start_root("recv");
    EXPECT_TRUE(span.context().valid());
    EXPECT_TRUE(span.sampled());
    // Raíz: span_id válido pero parent_id inválido lo verifica el sink al terminar (más abajo).
}

TEST(Tracer, StartChild_InheritsTraceAndSetsParent) {
    CountingIdGenerator ids;
    Tracer tracer{ids};

    const auto root = tracer.start_root("recv");
    const SpanContext root_ctx = root.context();
    const auto child = tracer.start_child(root_ctx, "handle");

    EXPECT_EQ(child.context().trace_id, root_ctx.trace_id);  // misma traza
    EXPECT_NE(child.context().span_id, root_ctx.span_id);    // span nuevo
    EXPECT_EQ(child.sampled(), root_ctx.sampled());          // hereda el muestreo
}

TEST(Tracer, EndEmitsSpanToSinkWithDurationAndParent) {
    CountingIdGenerator ids;
    std::vector<SpanData> emitted;
    auto sink = [&emitted](const SpanData& data) { emitted.push_back(data); };

    // Reloj inyectado: avanza 5 ms entre llamadas (determinista).
    std::int64_t tick_ms = 0;
    auto clock = [&tick_ms] {
        const auto now =
            std::chrono::system_clock::time_point{} + std::chrono::milliseconds{tick_ms};
        tick_ms += 5;
        return now;
    };

    Tracer tracer{ids, sink, clock};
    SpanContext root_ctx;
    {
        auto root = tracer.start_root("recv");                      // start = 0 ms
        auto child = tracer.start_child(root.context(), "handle");  // start = 5 ms
        root_ctx = root.context();
        child.set_attribute(nexus::field("topic", "orders"));
        child.end();  // end = 10 ms → se emite el hijo
        root.end();   // end = 15 ms → se emite la raíz
    }

    ASSERT_EQ(emitted.size(), 2U);
    // El hijo se emitió primero.
    EXPECT_EQ(emitted[0].name, "handle");
    EXPECT_EQ(emitted[0].parent_id, root_ctx.span_id);  // padre = span de la raíz
    EXPECT_EQ(emitted[0].context.trace_id, root_ctx.trace_id);
    EXPECT_EQ(emitted[0].duration(), std::chrono::milliseconds{5});
    ASSERT_EQ(emitted[0].attributes.size(), 1U);
    EXPECT_EQ(emitted[0].attributes[0].key, "topic");
    // La raíz no tiene padre.
    EXPECT_EQ(emitted[1].name, "recv");
    EXPECT_FALSE(emitted[1].parent_id.valid());
}

TEST(Tracer, RaiiAutoEnd_EmitsExactlyOnce) {
    CountingIdGenerator ids;
    int emitted = 0;
    auto sink = [&emitted](const SpanData&) { ++emitted; };
    Tracer tracer{ids, sink};

    {
        auto span = tracer.start_root("scope");
        span.end();  // emite una vez
        // el destructor no debe re-emitir (idempotente)
    }
    EXPECT_EQ(emitted, 1);

    {
        auto span = tracer.start_root("scope2");
        // sin end() explícito: el destructor emite una vez
    }
    EXPECT_EQ(emitted, 2);
}

TEST(Tracer, RemoteContextResumesTrace) {
    CountingIdGenerator ids;
    std::vector<SpanData> emitted;
    Tracer tracer{ids, [&emitted](const SpanData& data) { emitted.push_back(data); }};

    // Contexto llegado por la red (traceparent de otro proceso).
    const auto remote =
        parse_traceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    ASSERT_TRUE(remote.has_value());

    {
        auto span = tracer.start_from_remote(*remote, "broker.handle");
        EXPECT_EQ(span.context().trace_id, remote->trace_id);  // continúa la traza remota
    }
    ASSERT_EQ(emitted.size(), 1U);
    EXPECT_EQ(emitted[0].parent_id, remote->span_id);  // padre = span remoto
}

}  // namespace
