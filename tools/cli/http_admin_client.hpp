/// @file   cli/http_admin_client.hpp
/// @brief  HttpAdminClient: cliente HTTP/1.1 bloqueante del REST admin (plano de control).
/// @ingroup cli

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "cli/admin_client.hpp"
#include "common/error.hpp"

namespace nexus::cli {

/// @brief Implementación de `AdminClient` sobre HTTP/1.1 bloqueante (POSIX). Afinidad: el CLI es
///   monohilo; cada petición abre una conexión y la cierra (`Connection: close`).
/// @details Resuelve el destino con `getaddrinfo` (acepta IP o nombre), añade `Authorization:
/// Bearer`
///   si hay token y devuelve `IoError` ante fallos de conexión/transporte. No valida TLS (el REST
///   admin tras TLS llega en I17); de momento texto claro al puerto de operación.
class HttpAdminClient final : public AdminClient {
public:
    /// @brief Destino y credenciales del cliente. Afinidad: INMUTABLE.
    struct Options {
        std::string host = "127.0.0.1";  ///< Host del puerto de operación (IP o nombre).
        std::uint16_t port = 8080;       ///< Puerto de operación.
        std::string bearer_token;        ///< Token JWT (vacío = sin autenticación).
    };

    explicit HttpAdminClient(Options options);

    [[nodiscard]] expected<ClientResponse> get(std::string_view path) override;
    [[nodiscard]] expected<ClientResponse> post(std::string_view path,
                                                std::string_view body) override;
    [[nodiscard]] expected<ClientResponse> del(std::string_view path) override;

private:
    [[nodiscard]] expected<ClientResponse> request(std::string_view method, std::string_view path,
                                                   std::string_view body);

    Options options_;
};

}  // namespace nexus::cli
