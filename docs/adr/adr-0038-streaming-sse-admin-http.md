# ADR-0038: Modelo de streaming del servidor HTTP admin (SSE, `GET /api/v1/stream`)

- **Estado:** aceptado
- **Fecha:** 2026-07-13

## Contexto

La consola de administración quiere **tiempo real** (dashboards que se actualizan sin *polling*). El
servidor HTTP del puerto de operación es **one-shot**: `serve_admin_connection` lee **una** petición,
la enruta (`AdminRouter::handle` → `HttpResponse`) y envía la respuesta serializada, que **fuerza
`Content-Length`** y `Connection: close`. Ese contrato no admite una respuesta **indefinida**: SSE
(`text/event-stream`) mantiene la conexión abierta y va emitiendo eventos sin longitud conocida de
antemano.

Además, las corrutinas de conexión del pool son *fire-and-forget* (`reactor.spawn`), pensadas para
vidas cortas (una petición y cierre). Una conexión SSE es **larga**: hay que asegurar que **observa
la señal de parada** y no bloquea el apagado (SIGTERM).

## Decisión

Se introduce un **camino de respuesta streaming** hermano del buffered, sin tocar el contrato de
`handle`:

1. **Desvío temprano.** `serve_admin_connection`, tras parsear la petición, consulta
   `AdminRouter::is_stream_request` (GET a `/api/v1/stream`) y, si acierta, delega en
   `serve_admin_sse_connection` **antes** de caer en el modelo buffered. El resto de rutas siguen
   igual (una respuesta con `Content-Length` y cierre).

2. **Cabeceras SSE a mano.** El handler streaming escribe `HTTP/1.1 200 OK` +
   `Content-Type: text/event-stream` + `Cache-Control: no-cache` + `Connection: keep-alive` +
   `X-Accel-Buffering: no` (anti-buffering de proxies), **sin `Content-Length`** (no se puede usar
   `HttpResponse`, que lo fuerza).

3. **Bucle de emisión.** Luego repite: construye un frame `event: metrics` + `data: {snapshot}` con
   el **snapshot de métricas** (ADR-0017/A1, `render_metrics_snapshot_json`), lo envía
   (`co_await async_send`) y espera una **cadencia** (`co_await async_timer`, 1 s). Sale al primer
   error de envío (el cliente cerró el `EventSource`), al cancelarse el temporizador, o al detectar la
   señal de **drenaje**.

4. **Apagado limpio.** `Server::stop` activa un `atomic<bool> admin_draining_` (además de parar el
   reactor). El bucle SSE consulta esa señal en cada iteración y sale, cerrando la conexión (RAII del
   `Socket`). Las conexiones SSE largas se destruyen en el *teardown* del reactor igual que cualquier
   corrutina de conexión de vida larga (p. ej. `serve_connection` suspendida en `async_recv`): el
   `Proactor` cancela sus operaciones pendientes al cerrarse, sin *use-after-free*.

5. **Framing puro y testeable.** `format_sse_event(event, data)` es una función **pura** (una línea
   `data:` por línea del payload, evento terminado en línea en blanco), unit-testable de forma
   determinista; el **bucle** (con E/S y tiempo real) se valida por e2e.

## Consecuencias

- (+) La consola recibe métricas en **tiempo real** por `push` estándar (SSE), reconectable por el
  navegador (`EventSource`) sin lógica extra.
- (+) El modelo buffered queda **intacto**: el streaming es un camino hermano, no una reescritura del
  contrato de `handle`.
- (+) **Apagado limpio**: las conexiones SSE observan el drenaje y no cuelgan el `stop`/`join`;
  reutilizan el mismo *teardown* que las conexiones largas del plano de datos.
- (+) El framing es determinista y unit-testable (SSE es texto plano trivial de formatear).
- (−) El bucle en sí no es unit-testable de forma determinista (E/S + tiempo real): se cubre por e2e.
- (−) SSE es **unidireccional** (servidor→cliente): para comandos del cliente se sigue usando el REST
  puntual. Un canal bidireccional (WebSocket) sería otro ADR si hiciera falta.

## Alternativas consideradas

- **WebSockets:** bidireccional pero mucho más pesado (handshake de *upgrade*, framing binario,
  *ping/pong*); la consola solo necesita *push* de servidor→cliente. SSE es más simple y encaja en
  HTTP/1.1 plano. Descartado por sobredimensionado.
- **Long-polling:** el cliente repite peticiones; no es *push* real, multiplica conexiones y latencia.
  Descartado.
- **`Transfer-Encoding: chunked` sobre el modelo buffered:** seguiría siendo **una** respuesta
  conceptual (no un stream de eventos con reconexión) y no aporta la semántica de `EventSource`.
  Descartado a favor del camino streaming explícito con SSE.
- **Reescribir `handle` para devolver un tipo `StreamingResponse`:** contamina el contrato buffered
  (la mayoría de rutas no streaming) con un modo que solo usa una ruta. Se prefiere el **handler
  hermano** desviado temprano.
