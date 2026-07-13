/// @file   server/metrics_json.cpp
/// @brief  Implementación de la serialización JSON del snapshot de métricas.
/// @ingroup server

#include "server/metrics_json.hpp"

#include <cstdint>

#include "ingress/json.hpp"

namespace nexus {

std::string render_metrics_snapshot_json(const MetricsSnapshot& snapshot) {
    JsonWriter writer;
    writer.begin_object();
    writer.key("metrics").begin_array();
    for (const MetricSample& sample : snapshot.samples) {
        writer.begin_object();
        writer.field("name", sample.name);
        writer.field("type", metric_type_name(sample.type));
        writer.key("labels").begin_object();
        for (const auto& [key, value] : sample.labels) {
            writer.field(key, value);
        }
        writer.end_object();
        switch (sample.type) {
            case MetricType::Counter:
            case MetricType::Gauge:
                writer.field("value", sample.value);
                break;
            case MetricType::Histogram:
                writer.field("count", static_cast<std::int64_t>(sample.count));
                writer.field("sum", sample.sum);
                writer.key("buckets").begin_array();
                for (const HistogramBucketSample& bucket : sample.buckets) {
                    writer.begin_object();
                    writer.field("le", bucket.upper_bound);
                    writer.field("cumulativeCount",
                                 static_cast<std::int64_t>(bucket.cumulative_count));
                    writer.end_object();
                }
                writer.end_array();
                break;
        }
        writer.end_object();
    }
    writer.end_array();
    writer.end_object();
    return writer.take();
}

}  // namespace nexus
