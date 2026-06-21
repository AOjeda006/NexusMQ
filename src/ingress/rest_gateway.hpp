/// @file   ingress/rest_gateway.hpp
/// @brief  RestGateway: enrutado del REST admin (`/api/v1`) sobre `AdminService` (§7.6,
/// ADR-0006/18).
/// @ingroup ingress

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "common/task.hpp"
#include "ingress/admin_service.hpp"
#include "ingress/http.hpp"
#include "ingress/jwt.hpp"
#include "ingress/pagination.hpp"

namespace nexus {

/// @brief Pasarela HTTP del REST admin: traduce HTTP↔`AdminService` (§7.6). REACTOR-LOCAL.
/// @details Enruta `/api/v1/...` al puerto `AdminService` (ADR-0018), autentica con **Bearer
///   JWT** (opcional), pagina las colecciones (`page`/`size`) y traduce los `Error` del núcleo
///   a **RFC 7807** (`application/problem+json`). Serializa los DTOs a JSON (sin exponer tipos
///   internos). *Ajuste del desglose:* `handle` es **síncrono** (el puerto es síncrono y
///   THREAD-SAFE); la corrutina `route` se reserva para E/S asíncrona. El instante del JWT se
///   **inyecta** (`now_unix_seconds`), para pruebas deterministas.
/// @invariant El `AdminService` y el verificador referenciados viven más que la pasarela.
class RestGateway {
public:
    /// @brief Configuración de la pasarela. Afinidad: INMUTABLE.
    struct Config {
        std::string api_prefix = "/api/v1";  ///< Prefijo de ruta del REST admin.
        PaginationLimits pagination{};       ///< Límites de `page`/`size`.
    };

    /// @param admin Puerto de administración (vive más que la pasarela).
    /// @param verifier Verificador JWT; `nullptr` desactiva la autenticación.
    RestGateway(AdminService& admin, const JwtVerifier* verifier);
    RestGateway(AdminService& admin, const JwtVerifier* verifier, Config config);

    /// @brief Atiende una petición HTTP ya parseada. @p now_unix_seconds: «ahora» para el JWT.
    /// @details Corrutina: las rutas que mutan topics (POST/DELETE) pueden propagar el cambio a
    ///   varios núcleos por paso de mensajes (ADR-0026); las de solo lectura completan sin
    ///   suspenderse.
    [[nodiscard]] task<HttpResponse> handle(const HttpRequest& request,
                                            std::int64_t now_unix_seconds) const;

private:
    [[nodiscard]] expected<Principal> authenticate(const HttpRequest& request,
                                                   std::int64_t now_unix_seconds) const;
    [[nodiscard]] task<HttpResponse> route_topics(const HttpRequest& request,
                                                  std::string_view resource) const;
    [[nodiscard]] HttpResponse route_groups(const HttpRequest& request) const;
    [[nodiscard]] HttpResponse list_topics(const HttpRequest& request) const;
    [[nodiscard]] task<HttpResponse> create_topic(const HttpRequest& request) const;
    [[nodiscard]] HttpResponse describe_topic(const HttpRequest& request,
                                              std::string_view name) const;
    [[nodiscard]] task<HttpResponse> delete_topic(std::string_view name) const;

    AdminService& admin_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const JwtVerifier* verifier_;
    Config config_;
};

}  // namespace nexus
