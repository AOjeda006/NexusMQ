/// @file   server/admin_router.cpp
/// @brief  Implementación del enrutado del puerto de operación.
/// @ingroup server

#include "server/admin_router.hpp"

#include <chrono>
#include <string>
#include <utility>

namespace nexus {

namespace {

/// «Ahora» en segundos Unix según el reloj de pared del sistema.
std::int64_t system_now_seconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

/// Respuesta de texto plano con @p status y @p content_type.
HttpResponse text_response(int status, std::string content_type, std::string body) {
    HttpResponse response;
    response.status = status;
    response.reason = std::string{http_reason(status)};
    response.set_header("Content-Type", std::move(content_type));
    response.body = std::move(body);
    return response;
}

/// Solo GET (y HEAD) son válidos en los endpoints de operación de solo lectura.
bool is_read_method(HttpMethod method) noexcept {
    return method == HttpMethod::Get || method == HttpMethod::Head;
}

}  // namespace

AdminRouter::AdminRouter(RestGateway& rest, HealthMonitor& health, MetricsRegistry& metrics,
                         Clock clock)
    : rest_(rest), health_(health), metrics_(metrics), clock_(std::move(clock)) {}

HttpResponse AdminRouter::handle(const HttpRequest& request) const {
    const std::string_view path = request.path();
    if (path == "/metrics") {
        if (!is_read_method(request.method)) {
            return text_response(405, "text/plain; charset=utf-8", "method not allowed\n");
        }
        return text_response(200, "text/plain; version=0.0.4; charset=utf-8",
                             metrics_.render_prometheus());
    }
    if (path == "/healthz") {
        return health_.liveness();
    }
    if (path == "/readyz") {
        return health_.readiness();
    }
    const std::int64_t now = clock_ ? clock_() : system_now_seconds();
    return rest_.handle(request, now);
}

}  // namespace nexus
