/// @file   ingress/problem_detail.hpp
/// @brief  ProblemDetail (RFC 7807): formato de error del REST admin (§7.6, ADR-0009).
/// @ingroup ingress

#pragma once

#include <string>

#include "common/error.hpp"
#include "ingress/http.hpp"

namespace nexus {

/// @brief Detalle de problema RFC 7807 para errores del REST admin. Afinidad: REACTOR-LOCAL
/// (valor).
/// @details Es el modelo de error **de cara al cliente HTTP**, distinto de los `errorCode`
/// numéricos
///   del protocolo binario y del `Error` del núcleo (ADR-0009): el borde REST **traduce** el
///   `Error` interno a este formato. Se serializa como `application/problem+json`.
struct ProblemDetail {
    std::string type = "about:blank";  ///< URI del tipo de problema (RFC 7807).
    std::string title;                 ///< resumen legible y estable del tipo.
    int status = 500;                  ///< código HTTP.
    std::string detail;                ///< explicación específica de esta ocurrencia.
    std::string instance;              ///< URI de la ocurrencia concreta (opcional).

    /// @brief Cuerpo JSON RFC 7807.
    [[nodiscard]] std::string to_json() const;
    /// @brief Respuesta HTTP completa (status + `Content-Type: application/problem+json` + cuerpo).
    [[nodiscard]] HttpResponse to_response() const;
};

/// @brief Mapea un `ErrorCode` del núcleo al código HTTP del borde REST (ADR-0009).
[[nodiscard]] int http_status_for(ErrorCode code) noexcept;

/// @brief Construye un `ProblemDetail` desde un `Error` del núcleo (título por el código, `detail`
/// =
///   mensaje); @p instance es opcional (la ruta de la petición).
[[nodiscard]] ProblemDetail problem_from_error(const Error& error, std::string instance = {});

}  // namespace nexus
