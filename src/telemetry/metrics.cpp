/// @file   telemetry/metrics.cpp
/// @brief  Implementación de MetricsRegistry + Histogram (exposición Prometheus, §7.6).
/// @ingroup telemetry

#include "telemetry/metrics.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <format>

namespace nexus {

namespace {

/// Escapa un valor de etiqueta según el formato de texto de Prometheus (`\`, `"`, salto de línea).
std::string escape_label_value(std::string_view value) {
    std::string out;
    out.reserve(value.size());
    for (const char c : value) {
        switch (c) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            default:
                out += c;
        }
    }
    return out;
}

/// Render del conjunto de etiquetas: `{k="v",...}` (vacío si no hay etiquetas).
std::string render_labels(const Labels& labels) {
    if (labels.empty()) {
        return {};
    }
    std::string out = "{";
    bool first = true;
    for (const auto& [key, value] : labels) {
        if (!first) {
            out += ',';
        }
        first = false;
        out += key;
        out += "=\"";
        out += escape_label_value(value);
        out += '"';
    }
    out += '}';
    return out;
}

/// Formatea un doble para Prometheus (`+Inf`/`-Inf` para infinitos; repr corto para el resto).
std::string format_double(double value) {
    if (std::isinf(value)) {
        return value < 0 ? "-Inf" : "+Inf";
    }
    if (std::isnan(value)) {
        return "NaN";
    }
    return std::format("{}", value);
}

}  // namespace

Histogram::Histogram(std::vector<double> bounds)
    : bounds_(std::move(bounds)), buckets_(bounds_.size() + 1) {}

void Histogram::observe(double value) noexcept {
    count_.fetch_add(1, std::memory_order_relaxed);

    // Acumula la suma en doble mediante CAS sobre los bits (portátil; sin atomic<double>).
    std::uint64_t expected = sum_bits_.load(std::memory_order_relaxed);
    for (;;) {
        const double next = std::bit_cast<double>(expected) + value;
        const auto desired = std::bit_cast<std::uint64_t>(next);
        if (sum_bits_.compare_exchange_weak(expected, desired, std::memory_order_relaxed)) {
            break;
        }
    }

    // Menor cubo con `bound >= value`; si ninguno, el cubo +Inf (índice bounds_.size()).
    std::size_t index = 0;
    while (index < bounds_.size() && value > bounds_[index]) {
        ++index;
    }
    buckets_[index].fetch_add(1, std::memory_order_relaxed);
}

double Histogram::sum() const noexcept {
    return std::bit_cast<double>(sum_bits_.load(std::memory_order_relaxed));
}

std::string MetricsRegistry::series_key(std::string_view name, const Labels& labels) {
    std::string key(name);
    for (const auto& [k, v] : labels) {
        key += '\x1f';  // separador de etiqueta (no aparece en nombres/valores legítimos).
        key += k;
        key += '\x1e';
        key += v;
    }
    return key;
}

Counter& MetricsRegistry::counter(std::string_view name, Labels labels) {
    std::ranges::sort(labels);
    const std::string key = series_key(name, labels);
    const std::scoped_lock lock{mutex_};
    auto it = counters_.find(key);
    if (it == counters_.end()) {
        it = counters_
                 .emplace(key, Series<Counter>{.name = std::string(name),
                                               .labels = std::move(labels),
                                               .metric = std::make_unique<Counter>()})
                 .first;
    }
    return *it->second.metric;
}

Gauge& MetricsRegistry::gauge(std::string_view name, Labels labels) {
    std::ranges::sort(labels);
    const std::string key = series_key(name, labels);
    const std::scoped_lock lock{mutex_};
    auto it = gauges_.find(key);
    if (it == gauges_.end()) {
        it = gauges_
                 .emplace(key, Series<Gauge>{.name = std::string(name),
                                             .labels = std::move(labels),
                                             .metric = std::make_unique<Gauge>()})
                 .first;
    }
    return *it->second.metric;
}

Histogram& MetricsRegistry::histogram(std::string_view name, Labels labels,
                                      std::vector<double> bounds) {
    std::ranges::sort(labels);
    const std::string key = series_key(name, labels);
    const std::scoped_lock lock{mutex_};
    auto it = histograms_.find(key);
    if (it == histograms_.end()) {
        it = histograms_
                 .emplace(
                     key,
                     Series<Histogram>{.name = std::string(name),
                                       .labels = std::move(labels),
                                       .metric = std::make_unique<Histogram>(std::move(bounds))})
                 .first;
    }
    return *it->second.metric;
}

void MetricsRegistry::describe(std::string_view name, std::string help) {
    const std::scoped_lock lock{mutex_};
    help_.insert_or_assign(std::string(name), std::move(help));
}

std::string MetricsRegistry::render_prometheus() const {
    const std::scoped_lock lock{mutex_};
    std::string out;

    const auto emit_help_type = [&](const std::string& name, const char* type) {
        if (const auto h = help_.find(name); h != help_.end()) {
            out += "# HELP " + name + ' ' + h->second + '\n';
        }
        out += "# TYPE " + name + ' ' + type + '\n';
    };

    std::string current;
    for (const auto& [key, series] : counters_) {
        if (series.name != current) {
            current = series.name;
            emit_help_type(current, "counter");
        }
        out += series.name + render_labels(series.labels) + ' ' +
               std::to_string(series.metric->value()) + '\n';
    }

    current.clear();
    for (const auto& [key, series] : gauges_) {
        if (series.name != current) {
            current = series.name;
            emit_help_type(current, "gauge");
        }
        out += series.name + render_labels(series.labels) + ' ' +
               std::to_string(series.metric->value()) + '\n';
    }

    current.clear();
    for (const auto& [key, series] : histograms_) {
        if (series.name != current) {
            current = series.name;
            emit_help_type(current, "histogram");
        }
        const Histogram& hist = *series.metric;
        std::uint64_t cumulative = 0;
        for (std::size_t i = 0; i < hist.bounds().size(); ++i) {
            cumulative += hist.bucket(i);
            Labels le = series.labels;
            le.emplace_back("le", format_double(hist.bounds()[i]));
            out += series.name + "_bucket" + render_labels(le) + ' ' + std::to_string(cumulative) +
                   '\n';
        }
        cumulative += hist.bucket(hist.bounds().size());
        Labels inf = series.labels;
        inf.emplace_back("le", "+Inf");
        out +=
            series.name + "_bucket" + render_labels(inf) + ' ' + std::to_string(cumulative) + '\n';
        out += series.name + "_sum" + render_labels(series.labels) + ' ' +
               format_double(hist.sum()) + '\n';
        out += series.name + "_count" + render_labels(series.labels) + ' ' +
               std::to_string(hist.count()) + '\n';
    }

    return out;
}

std::vector<double> MetricsRegistry::default_latency_bounds() {
    return {0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0};
}

}  // namespace nexus
