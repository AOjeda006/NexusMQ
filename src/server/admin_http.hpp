/// @file   server/admin_http.hpp
/// @brief  Servicio HTTP/1.1 del puerto de operación sobre un Socket (lee, enruta, responde).
/// @ingroup server

#pragma once

#include <atomic>

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

/// @brief Sirve el puerto de operación de una conexión: **buffered** (una petición y cierra) o
///   **streaming** (SSE) según la ruta. Afinidad: REACTOR-LOCAL.
/// @details Lee la petición (`read_http_request`); si es el stream SSE
/// (`AdminRouter::is_stream_request`)
///   la desvía al camino streaming (`serve_admin_sse_connection`); si no, la enruta por el
///   `AdminRouter` y envía **una** respuesta buffered (`Connection: close`). Un mensaje malformado
///   se responde con `400`; un cierre limpio sin datos termina sin responder. Toma posesión del @p
///   sock (RAII). @p draining se consulta en el camino SSE para cerrar la conexión al apagar
///   (ADR-0038).
task<void> serve_admin_connection(Proactor& proactor, Socket sock, const AdminRouter& router,
                                  const std::atomic<bool>& draining, HttpParseLimits limits = {});

/// @brief Sirve una conexión **SSE** (`text/event-stream`): cabeceras sin `Content-Length` +
///   conexión persistente, y luego un bucle que emite frames `data: {...}` con una cadencia hasta
///   que el par cierra, hay un error de envío o el servidor drena (@p draining). Afinidad:
///   REACTOR-LOCAL. Toma posesión del @p sock (RAII: al salir cierra la conexión).
task<void> serve_admin_sse_connection(Proactor& proactor, Socket sock, const AdminRouter& router,
                                      const std::atomic<bool>& draining);

}  // namespace nexus
