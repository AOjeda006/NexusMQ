// MetricsRegistry (§7.6, ADR-0017): contadores/gauges/histogramas y exposición Prometheus
// determinista. Verifica get-or-create idempotente por (nombre, etiquetas), canonicalización de
// etiquetas, render con # TYPE/# HELP, histograma con cubos acumulativos + sum + count, escapado de
// valores y los cubos de latencia por defecto.
#include "telemetry/metrics.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

TEST(MetricsRegistry, Counter_GetOrCreate_DevuelveLaMismaSerie) {
    nexus::MetricsRegistry reg;
    reg.counter("nexus_produce_total").inc();
    reg.counter("nexus_produce_total").inc(4);
    EXPECT_EQ(reg.counter("nexus_produce_total").value(), 5U);
}

TEST(MetricsRegistry, Counter_EtiquetasDistintas_SonSeriesDistintas) {
    nexus::MetricsRegistry reg;
    reg.counter("req", {{"topic", "a"}}).inc(2);
    reg.counter("req", {{"topic", "b"}}).inc(3);
    EXPECT_EQ(reg.counter("req", {{"topic", "a"}}).value(), 2U);
    EXPECT_EQ(reg.counter("req", {{"topic", "b"}}).value(), 3U);
}

TEST(MetricsRegistry, Counter_OrdenDeEtiquetas_NoImporta) {
    nexus::MetricsRegistry reg;
    reg.counter("m", {{"a", "1"}, {"b", "2"}}).inc();
    // Mismo conjunto de etiquetas en otro orden → misma serie (canonicalización).
    reg.counter("m", {{"b", "2"}, {"a", "1"}}).inc();
    EXPECT_EQ(reg.counter("m", {{"a", "1"}, {"b", "2"}}).value(), 2U);
}

TEST(MetricsRegistry, Gauge_SetIncDec) {
    nexus::MetricsRegistry reg;
    nexus::Gauge& g = reg.gauge("nexus_commit_index", {{"partition", "0"}});
    g.set(10);
    g.inc(5);
    g.dec(3);
    EXPECT_EQ(g.value(), 12);
}

TEST(MetricsRegistry, Histogram_CubosAcumulativosSumYCount) {
    nexus::MetricsRegistry reg;
    nexus::Histogram& h = reg.histogram("lat", {}, {1.0, 5.0, 10.0});
    h.observe(0.5);   // cubo le=1
    h.observe(3.0);   // cubo le=5
    h.observe(7.0);   // cubo le=10
    h.observe(50.0);  // +Inf
    EXPECT_EQ(h.count(), 4U);
    EXPECT_DOUBLE_EQ(h.sum(), 60.5);

    const std::string text = reg.render_prometheus();
    EXPECT_NE(text.find("# TYPE lat histogram"), std::string::npos);
    EXPECT_NE(text.find("lat_bucket{le=\"1\"} 1"), std::string::npos);
    EXPECT_NE(text.find("lat_bucket{le=\"5\"} 2"), std::string::npos);   // acumulado.
    EXPECT_NE(text.find("lat_bucket{le=\"10\"} 3"), std::string::npos);  // acumulado.
    EXPECT_NE(text.find("lat_bucket{le=\"+Inf\"} 4"), std::string::npos);
    EXPECT_NE(text.find("lat_count 4"), std::string::npos);
}

TEST(MetricsRegistry, Render_IncluyeHelpYType) {
    nexus::MetricsRegistry reg;
    reg.describe("nexus_up", "Si el nodo esta vivo.");
    reg.gauge("nexus_up").set(1);
    const std::string text = reg.render_prometheus();
    EXPECT_NE(text.find("# HELP nexus_up Si el nodo esta vivo."), std::string::npos);
    EXPECT_NE(text.find("# TYPE nexus_up gauge"), std::string::npos);
    EXPECT_NE(text.find("nexus_up 1"), std::string::npos);
}

TEST(MetricsRegistry, Render_EscapaValoresDeEtiqueta) {
    nexus::MetricsRegistry reg;
    reg.counter("m", {{"path", "a\"b\\c"}}).inc();
    const std::string text = reg.render_prometheus();
    EXPECT_NE(text.find("path=\"a\\\"b\\\\c\""), std::string::npos);
}

TEST(MetricsRegistry, Snapshot_CounterYGauge_CapturaValorYEtiquetas) {
    nexus::MetricsRegistry reg;
    reg.counter("nexus_requests_total", {{"protocol", "native"}}).inc(7);
    reg.gauge("nexus_commit_index", {{"partition", "0"}}).set(42);

    const nexus::MetricsSnapshot snap = reg.snapshot();

    const nexus::MetricSample* counter = nullptr;
    const nexus::MetricSample* gauge = nullptr;
    for (const auto& sample : snap.samples) {
        if (sample.name == "nexus_requests_total") {
            counter = &sample;
        } else if (sample.name == "nexus_commit_index") {
            gauge = &sample;
        }
    }
    ASSERT_NE(counter, nullptr);
    EXPECT_EQ(counter->type, nexus::MetricType::Counter);
    EXPECT_DOUBLE_EQ(counter->value, 7.0);
    ASSERT_EQ(counter->labels.size(), 1U);
    EXPECT_EQ(counter->labels[0].first, "protocol");
    EXPECT_EQ(counter->labels[0].second, "native");

    ASSERT_NE(gauge, nullptr);
    EXPECT_EQ(gauge->type, nexus::MetricType::Gauge);
    EXPECT_DOUBLE_EQ(gauge->value, 42.0);
}

TEST(MetricsRegistry, Snapshot_Histograma_CubosAcumulativosCountSum) {
    nexus::MetricsRegistry reg;
    nexus::Histogram& h = reg.histogram("lat", {}, {1.0, 5.0, 10.0});
    h.observe(0.5);   // cubo le=1
    h.observe(3.0);   // cubo le=5
    h.observe(7.0);   // cubo le=10
    h.observe(50.0);  // +Inf

    const nexus::MetricsSnapshot snap = reg.snapshot();
    const nexus::MetricSample* hist = nullptr;
    for (const auto& sample : snap.samples) {
        if (sample.name == "lat") {
            hist = &sample;
        }
    }
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->type, nexus::MetricType::Histogram);
    EXPECT_EQ(hist->count, 4U);  // total = cubo +Inf.
    EXPECT_DOUBLE_EQ(hist->sum, 60.5);
    ASSERT_EQ(hist->buckets.size(), 3U);  // solo cubos finitos (le=1,5,10).
    EXPECT_DOUBLE_EQ(hist->buckets[0].upper_bound, 1.0);
    EXPECT_EQ(hist->buckets[0].cumulative_count, 1U);
    EXPECT_EQ(hist->buckets[1].cumulative_count, 2U);  // acumulado.
    EXPECT_EQ(hist->buckets[2].cumulative_count, 3U);  // acumulado.
}

TEST(MetricsRegistry, MetricTypeName_NombresEstables) {
    EXPECT_EQ(nexus::metric_type_name(nexus::MetricType::Counter), "counter");
    EXPECT_EQ(nexus::metric_type_name(nexus::MetricType::Gauge), "gauge");
    EXPECT_EQ(nexus::metric_type_name(nexus::MetricType::Histogram), "histogram");
}

TEST(MetricsRegistry, DefaultLatencyBounds_AscendenteYNoVacio) {
    const auto bounds = nexus::MetricsRegistry::default_latency_bounds();
    ASSERT_FALSE(bounds.empty());
    for (std::size_t i = 1; i < bounds.size(); ++i) {
        EXPECT_LT(bounds[i - 1], bounds[i]);
    }
}

}  // namespace
