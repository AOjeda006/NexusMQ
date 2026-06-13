/// @file   server/connection.hpp
/// @brief  serve_connection: bucle leer-trama → despachar → escribir-trama de una conexión.
/// @ingroup server

#pragma once

#include "common/task.hpp"

namespace nexus {

class Proactor;
class Socket;
class RequestRouter;

/// @brief Sirve una conexión cliente hasta que el par cierra o hay un error. Afinidad:
///   REACTOR-LOCAL.
/// @details Bucle de petición/respuesta: lee una trama (`FrameReader`), despacha su cuerpo por
///   `RequestRouter` y escribe la respuesta (`FrameWriter`) con la cabecera que **espeja**
///   `correlation_id`/`api_key`. Toma posesión del @p sock (lo cierra al terminar, RAII).
/// @note El desglose modelaba una clase `Connection`; aquí es una corrutina libre que posee el
///   socket en su *frame* (evita miembros auto-referenciados); ajuste anotado en la hoja de ruta.
task<void> serve_connection(Proactor& proactor, Socket sock, RequestRouter& router);

}  // namespace nexus
