/// @file   server/admin_http.hpp
/// @brief  Servicio HTTP/1.1 del puerto de operación sobre un Socket (lee, enruta, responde).
/// @ingroup server

#pragma once

#include "common/error.hpp"
#include "common/task.hpp"
#include "ingress/http.hpp"
#include "io/socket.hpp"

namespace nexus {

class Proactor;
class AdminRouter;

/// @brief Lee un **mensaje HTTP/1.1 completo** de @p sock (cabeceras + cuerpo por
/// `Content-Length`).
/// @details Acumula bytes hasta el fin de cabeceras (`\r\n\r\n`), determina el `Content-Length` y
/// lee
///   el cuerpo restante; aplica los @p limits (anti-DoS). Devuelve `ErrorCode::Shutdown` si el par
///   cierra **sin enviar nada** (cierre limpio), o `InvalidArgument` si el mensaje está truncado o
///   excede algún límite.
[[nodiscard]] task<expected<HttpRequest>> read_http_request(Proactor& proactor, const Socket& sock,
                                                            HttpParseLimits limits = {});

/// @brief Sirve **una** petición del puerto de operación y cierra (`Connection: close`).
///   Afinidad: REACTOR-LOCAL.
/// @details Lee la petición (`read_http_request`), la enruta por el `AdminRouter` (REST + /metrics
/// +
///   health) y envía la respuesta serializada. Un mensaje malformado se responde con `400`; un
///   cierre limpio sin datos termina sin responder. Toma posesión del @p sock (RAII).
task<void> serve_admin_connection(Proactor& proactor, Socket sock, const AdminRouter& router,
                                  HttpParseLimits limits = {});

}  // namespace nexus
