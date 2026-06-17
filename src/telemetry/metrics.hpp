/// @file   telemetry/metrics.hpp
/// @brief  MetricsRegistry: contadores/gauges/histogramas + exposición Prometheus (§7.6, ADR-0017).
/// @ingroup telemetry

#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace nexus {

/// @brief Conjunto de etiquetas (clave→valor) de una serie de métrica. Afinidad: INMUTABLE.
/// @details Se canonicalizan **ordenadas por clave** al registrar la serie, de modo que el orden de
///   declaración del llamante no crea series distintas.
using Labels = std::vector<std::pair<std::string, std::string>>;

/// @brief Contador monótono (solo sube). Afinidad: THREAD-SAFE (atómico, sin candado).
class Counter {
public:
    /// @brief Incrementa el contador en @p n (>= 0).
    void inc(std::uint64_t n = 1) noexcept { value_.fetch_add(n, std::memory_order_relaxed); }
    [[nodiscard]] std::uint64_t value() const noexcept {
        return value_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<std::uint64_t> value_{0};
};

/// @brief Medidor que sube y baja (p. ej. `commit_index`, *lag*, conexiones). Afinidad:
/// THREAD-SAFE.
class Gauge {
public:
    void set(std::int64_t v) noexcept { value_.store(v, std::memory_order_relaxed); }
    void inc(std::int64_t n = 1) noexcept { value_.fetch_add(n, std::memory_order_relaxed); }
    void dec(std::int64_t n = 1) noexcept { value_.fetch_sub(n, std::memory_order_relaxed); }
    [[nodiscard]] std::int64_t value() const noexcept {
        return value_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<std::int64_t> value_{0};
};

/// @brief Histograma de cubos acumulativos (estilo Prometheus). Afinidad: THREAD-SAFE.
/// @details Los `bounds` son los límites superiores (`le`) en orden ascendente; el cubo `+Inf` es
///   implícito (todas las observaciones). Cada observación cae en el menor cubo con `bound >= v`;
///   el render acumula. `sum` se mantiene en doble vía CAS sobre los bits (portátil; sin
///   `atomic<double>`).
class Histogram {
public:
    explicit Histogram(std::vector<double> bounds);

    /// @brief Registra una observación @p value.
    void observe(double value) noexcept;

    [[nodiscard]] const std::vector<double>& bounds() const noexcept { return bounds_; }
    /// Cuenta del cubo @p i (no acumulativa); `i == bounds().size()` es el cubo `+Inf`.
    [[nodiscard]] std::uint64_t bucket(std::size_t i) const noexcept {
        return buckets_[i].load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t count() const noexcept {
        return count_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] double sum() const noexcept;

private:
    std::vector<double> bounds_;
    std::vector<std::atomic<std::uint64_t>> buckets_;  ///< bounds_.size() + 1 (incluye +Inf).
    std::atomic<std::uint64_t> count_{0};
    std::atomic<std::uint64_t> sum_bits_{0};  ///< bits del doble acumulado (CAS).
};

/// @brief Registro central de métricas con exposición Prometheus (§7.6). Afinidad: THREAD-SAFE.
/// @details Crea/recupera series por `(nombre, etiquetas)` con `counter`/`gauge`/`histogram` y las
///   expone en el **formato de texto de Prometheus** con `render_prometheus()` (orden determinista:
///   familias y series ordenadas). El **mutex protege solo la estructura** (alta de series y
///   recorrido en el render); las actualizaciones de valor son **atómicas y sin candado**
///   (ADR-0017). `describe` asocia un texto `# HELP` a una familia (opcional).
/// @invariant Devolver una referencia a una serie es estable: las series viven en `unique_ptr`, así
///   que su dirección no cambia aunque se registren otras (la referencia sobrevive al `unlock`).
class MetricsRegistry {
public:
    MetricsRegistry() = default;
    MetricsRegistry(const MetricsRegistry&) = delete;
    MetricsRegistry& operator=(const MetricsRegistry&) = delete;
    MetricsRegistry(MetricsRegistry&&) = delete;
    MetricsRegistry& operator=(MetricsRegistry&&) = delete;
    ~MetricsRegistry() = default;

    /// @brief Recupera (o crea) el contador `(name, labels)`.
    [[nodiscard]] Counter& counter(std::string_view name, Labels labels = {});
    /// @brief Recupera (o crea) el gauge `(name, labels)`.
    [[nodiscard]] Gauge& gauge(std::string_view name, Labels labels = {});
    /// @brief Recupera (o crea) el histograma `(name, labels)` con los `bounds` dados (al crearlo).
    [[nodiscard]] Histogram& histogram(std::string_view name, Labels labels = {},
                                       std::vector<double> bounds = default_latency_bounds());

    /// @brief Asocia un texto de ayuda (`# HELP`) a la familia @p name (opcional, idempotente).
    void describe(std::string_view name, std::string help);

    /// @brief Render del registro en el formato de texto de Prometheus (para `/metrics`).
    [[nodiscard]] std::string render_prometheus() const;

    /// @brief Cubos de latencia por defecto (segundos), convención de Prometheus.
    [[nodiscard]] static std::vector<double> default_latency_bounds();

private:
    template <class Metric>
    struct Series {
        std::string name;
        Labels labels;  ///< canónicas (ordenadas por clave).
        std::unique_ptr<Metric> metric;
    };

    /// Clave única de serie: `name` + separador + etiquetas serializadas.
    [[nodiscard]] static std::string series_key(std::string_view name, const Labels& labels);

    mutable std::mutex mutex_;
    std::map<std::string, Series<Counter>> counters_;
    std::map<std::string, Series<Gauge>> gauges_;
    std::map<std::string, Series<Histogram>> histograms_;
    std::map<std::string, std::string, std::less<>> help_;  ///< familia → texto de ayuda.
};

}  // namespace nexus
