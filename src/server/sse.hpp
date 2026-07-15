/// @file   server/sse.hpp
/// @brief  Formato de eventos SSE (`text/event-stream`) del puerto de operación (ADR-0038).
/// @ingroup server

#pragma once

#include <string>
#include <string_view>

namespace nexus {

/// @brief Formatea un evento **Server-Sent Events** (W3C): `event:` opcional + una línea `data:`
/// por
///   cada línea del payload + una línea en blanco de fin.
/// @details Cada `\n` del @p data abre una nueva línea `data: ` (regla SSE); el evento termina en
/// una
///   línea vacía (`\n\n`). Con @p event vacío se omite la línea `event:` (evento por defecto
///   `message`). Función pura (sin E/S), unit-testable de forma determinista.
/// @param event Nombre del evento (`event:`), o vacío para el evento por defecto.
/// @param data Cuerpo del evento (típicamente JSON de una sola línea).
[[nodiscard]] std::string format_sse_event(std::string_view event, std::string_view data);

}  // namespace nexus
