/// @file   server/admin_router.hpp
/// @brief  AdminRouter: multiplexa el puerto de operación (REST + /metrics + /healthz + /readyz).
/// @ingroup server

#pragma once

#include <cstdint>
#include <functional>

#include "common/task.hpp"
#include "ingress/health_monitor.hpp"
#include "ingress/http.hpp"
#include "ingress/rest_gateway.hpp"
#include "telemetry/metrics.hpp"

namespace nexus {

/// @brief Enrutador del **puerto de operación**: un solo HTTP que sirve el REST admin, las métricas
///   Prometheus y los *health-checks* (§7.6, ADR-0006/0017). Afinidad: REACTOR-LOCAL.
/// @details Despacha por ruta: `/metrics` → exposición Prometheus (texto), `/healthz` → liveness,
///   `/readyz` → readiness (ambos sin autenticación: los consume el orquestador / el *scraper*); el
///   resto se delega al `RestGateway` (que aplica la autenticación Bearer JWT y enruta `/api/v1`).
///   El «ahora» del JWT lo aporta un reloj **inyectable** (`Clock`), para pruebas deterministas.
/// @invariant El `RestGateway`, el `HealthMonitor` y el `MetricsRegistry` referenciados viven más
///   que el enrutador.
class AdminRouter {
public:
    /// Reloj de pared en segundos Unix (para validar el `exp`/`nbf` del JWT).
    using Clock = std::function<std::int64_t()>;

    /// @param rest Pasarela REST admin (vive más que el enrutador).
    /// @param health Monitor de salud (liveness/readiness).
    /// @param metrics Registro de métricas (exposición Prometheus).
    /// @param clock Reloj Unix; si está vacío, usa `std::chrono::system_clock`.
    AdminRouter(RestGateway& rest, HealthMonitor& health, MetricsRegistry& metrics,
                Clock clock = {});

    /// @brief Atiende una petición HTTP ya parseada y devuelve la respuesta.
    /// @details Corrutina: health/metrics completan sin suspenderse; el REST admin se delega al
    ///   `RestGateway` (que puede propagar cambios de topic a varios núcleos, ADR-0026).
    [[nodiscard]] task<HttpResponse> handle(const HttpRequest& request) const;

private:
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
    RestGateway& rest_;
    HealthMonitor& health_;
    MetricsRegistry& metrics_;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
    Clock clock_;
};

}  // namespace nexus
