/// @file   cli/admin_client.hpp
/// @brief  AdminClient: puerto del cliente del REST admin (permite probar los comandos sin red).
/// @ingroup cli

#pragma once

#include <string>
#include <string_view>

#include "common/error.hpp"

namespace nexus::cli {

/// @brief Respuesta HTTP simplificada para el CLI (estado + cuerpo). Afinidad: INMUTABLE.
struct ClientResponse {
    int status = 0;    ///< Código de estado HTTP (p. ej. 200, 404).
    std::string body;  ///< Cuerpo de la respuesta (JSON o problem+json).
};

/// @brief Puerto del cliente de administración: las operaciones HTTP que usan los comandos.
/// @details Abstrae el transporte (HTTP real vs. doble de prueba) para que la lógica de los
///   subcomandos (`run_topic`, …) sea **testeable sin red** (inversión de dependencias). Devuelve
///   `expected` (ADR-0009): el `Error` señala fallos de transporte; los errores de aplicación
///   llegan como `ClientResponse` con `status >= 400`.
class AdminClient {
public:
    AdminClient() = default;
    AdminClient(const AdminClient&) = delete;
    AdminClient& operator=(const AdminClient&) = delete;
    AdminClient(AdminClient&&) = delete;
    AdminClient& operator=(AdminClient&&) = delete;
    virtual ~AdminClient() = default;

    /// @brief `GET @p path`.
    [[nodiscard]] virtual expected<ClientResponse> get(std::string_view path) = 0;
    /// @brief `POST @p path` con cuerpo JSON @p body.
    [[nodiscard]] virtual expected<ClientResponse> post(std::string_view path,
                                                        std::string_view body) = 0;
    /// @brief `DELETE @p path`.
    [[nodiscard]] virtual expected<ClientResponse> del(std::string_view path) = 0;
};

}  // namespace nexus::cli
