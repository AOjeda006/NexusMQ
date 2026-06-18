/// @file   ingress/health_monitor.cpp
/// @brief  Implementación de HealthMonitor: liveness y readiness con probes inyectados.
/// @ingroup ingress

#include "ingress/health_monitor.hpp"

#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "ingress/json.hpp"

namespace nexus {

namespace {

/// Construye una respuesta JSON con @p status y @p body.
HttpResponse json_response(int status, std::string body) {
    HttpResponse response;
    response.status = status;
    response.reason = std::string{http_reason(status)};
    response.set_header("Content-Type", "application/json");
    response.body = std::move(body);
    return response;
}

}  // namespace

void HealthMonitor::register_readiness(std::string name, Probe probe) {
    const std::scoped_lock lock{mutex_};
    probes_.emplace_back(std::move(name), std::move(probe));
}

void HealthMonitor::set_started(bool started) noexcept {
    const std::scoped_lock lock{mutex_};
    started_ = started;
}

void HealthMonitor::set_live(bool live) noexcept {
    const std::scoped_lock lock{mutex_};
    live_ = live;
}

HttpResponse HealthMonitor::liveness() const {
    bool live = true;
    {
        const std::scoped_lock lock{mutex_};
        live = live_;
    }
    JsonWriter writer;
    writer.begin_object();
    writer.field("status", live ? "ok" : "draining");
    writer.end_object();
    return json_response(live ? 200 : 503, writer.take());
}

HttpResponse HealthMonitor::readiness() const {
    // Copia los probes y el estado bajo el mutex; se ejecutan fuera para no retener el lock
    // (un probe podría ser lento o reentrante).
    std::vector<std::pair<std::string, Probe>> probes;
    bool started = false;
    {
        const std::scoped_lock lock{mutex_};
        probes = probes_;
        started = started_;
    }

    JsonWriter writer;
    writer.begin_object();
    writer.key("checks").begin_array();

    // Check sintético de arranque: hasta que el cableado llama a set_started(), no hay readiness.
    bool all_healthy = started;
    writer.begin_object();
    writer.field("name", std::string_view{"startup"});
    writer.field("healthy", started);
    writer.field("detail", started ? "" : "el arranque no ha terminado");
    writer.end_object();

    for (const auto& [name, probe] : probes) {
        const HealthCheckResult result = probe();
        all_healthy = all_healthy && result.healthy;
        writer.begin_object();
        writer.field("name", name);
        writer.field("healthy", result.healthy);
        writer.field("detail", result.detail);
        writer.end_object();
    }

    writer.end_array();
    writer.field("status", all_healthy ? "ready" : "not_ready");
    writer.end_object();
    return json_response(all_healthy ? 200 : 503, writer.take());
}

HealthMonitor::Probe disk_space_probe(std::filesystem::path path, std::uintmax_t min_free_bytes) {
    return [path = std::move(path), min_free_bytes]() -> HealthCheckResult {
        std::error_code ec;
        const std::filesystem::space_info space = std::filesystem::space(path, ec);
        if (ec) {
            return HealthCheckResult{
                .name = "disk", .healthy = false, .detail = "no se pudo consultar el espacio"};
        }
        const bool healthy = space.available >= min_free_bytes;
        return HealthCheckResult{.name = "disk",
                                 .healthy = healthy,
                                 .detail = healthy ? "" : "espacio libre por debajo del mínimo"};
    };
}

}  // namespace nexus
