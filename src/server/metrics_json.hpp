/// @file   server/metrics_json.hpp
/// @brief  Serialización JSON del snapshot de métricas (REST admin `/api/v1/metrics/snapshot`,
/// SSE).
/// @ingroup server

#pragma once

#include <string>

#include "telemetry/metrics.hpp"

namespace nexus {

/// @brief Serializa un `MetricsSnapshot` al contrato JSON del REST admin.
/// @details Objeto `{ "metrics": [ ... ] }` con una entrada por serie: `name`, `type`
///   (`counter`/`gauge`/`histogram`) y `labels` (objeto clave→valor). Los contadores/gauges
///   añaden `value`; los histogramas, `count`, `sum` y `buckets` (cubos acumulativos finitos con
///   `le`/`cumulativeCount`; el cubo `+Inf` es el propio `count`). Reactor-local, sin E/S: lo usan
///   tanto el endpoint puntual (A1) como cada frame del stream SSE (C1).
[[nodiscard]] std::string render_metrics_snapshot_json(const MetricsSnapshot& snapshot);

}  // namespace nexus
