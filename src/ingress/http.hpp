/// @file   ingress/http.hpp
/// @brief  Tipos y parser HTTP/1.1 del gateway REST de administración (§7.6, ADR-0006).
/// @ingroup ingress

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/error.hpp"

namespace nexus {

/// @brief Método HTTP. Afinidad: INMUTABLE.
enum class HttpMethod : std::uint8_t { Get, Head, Post, Put, Delete, Patch, Options, Unknown };

/// @brief Petición HTTP ya parseada. Afinidad: REACTOR-LOCAL (valor).
/// @details `target` es el *request-target* crudo (p. ej. `/api/v1/topics?page=1`); `path()` y
///   `query()` lo separan. Las cabeceras se consultan **sin distinguir mayúsculas** (`header`).
struct HttpRequest {
    HttpMethod method = HttpMethod::Unknown;
    std::string method_text;  ///< token original del método (útil si es `Unknown`).
    std::string target;
    std::string version;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;

    /// Parte de `target` anterior a `?`.
    [[nodiscard]] std::string_view path() const noexcept;
    /// Parte de `target` posterior a `?` (vacía si no hay).
    [[nodiscard]] std::string_view query() const noexcept;
    /// Valor de la cabecera @p name (sin distinguir mayúsculas), si existe.
    [[nodiscard]] std::optional<std::string_view> header(std::string_view name) const;
};

/// @brief Respuesta HTTP a serializar. Afinidad: REACTOR-LOCAL (valor).
struct HttpResponse {
    int status = 200;
    std::string reason = "OK";
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;

    /// Fija (o reemplaza) la cabecera @p name (sin distinguir mayúsculas).
    void set_header(std::string name, std::string value);

    /// @brief Serializa la respuesta HTTP/1.1; **añade `Content-Length`** acorde al cuerpo.
    [[nodiscard]] std::string serialize() const;
};

/// @brief Límites de parseo (anti-DoS): acotan el tamaño de la petición. Afinidad: INMUTABLE.
struct HttpParseLimits {
    std::size_t max_request_line = 8192;
    std::size_t max_headers = 100;
    std::size_t max_header_bytes = 16384;
    std::size_t max_body = std::size_t{1} << 20;  ///< 1 MiB.
};

/// @brief Frase de estado HTTP estándar para @p status (p. ej. 404 → "Not Found"; "" si
/// desconocido).
[[nodiscard]] std::string_view http_reason(int status) noexcept;

/// @brief Parsea un **mensaje HTTP/1.1 completo** (línea de petición + cabeceras + cuerpo).
/// @details Defensivo (`expected`): valida la línea de petición, la versión `HTTP/x.y`, el formato
/// de
///   las cabeceras y, si hay `Content-Length`, que el cuerpo lo cumpla; aplica los `limits`. Espera
///   un búfer **completo** (la capa de conexión acumula hasta el fin del cuerpo); el parseo en sí
///   no hace E/S.
/// @return La petición, o `InvalidArgument` si está malformada o excede algún límite.
[[nodiscard]] expected<HttpRequest> parse_request(std::string_view raw,
                                                  const HttpParseLimits& limits = {});

}  // namespace nexus
