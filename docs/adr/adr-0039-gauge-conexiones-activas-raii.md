# ADR-0039: Gauge de conexiones activas por plano con RAII (`nexus_broker_connections_active`)

- **Estado:** aceptado
- **Fecha:** 2026-07-17

## Contexto

Cualquier dashboard de un broker necesita saber **cuántas conexiones de cliente hay vivas** en cada
momento: es la señal de saturación más directa (fugas de conexión, clientes colgados, picos de
carga). NexusMQ ya emite las cuatro señales de oro por petición (`nexus_broker_requests_total` y
familia, ADR-0017), pero esas son **contadores de eventos puntuales** (una petición servida), no el
**nivel** de conexiones abiertas. En la prueba E2E con la consola web se detectó que no había ninguna
métrica de conexiones vivas.

Un contador de conexiones vivas es un **gauge** (sube al aceptar, baja al cerrar). El riesgo evidente
es la **fuga del contador**: si una conexión muere por excepción, error de E/S o cierre abrupto del
par y el decremento se hace "a mano" en la ruta de éxito, el gauge queda inflado para siempre y deja
de ser fiable. Además, el broker sirve **tres planos** de conexión heterogéneos, cada uno con su
propio bucle de aceptación (`server.cpp`):

- **native** — plano de datos binario nativo (`accept_loop` → `serve_connection`; en modo proxy,
  `proxy_accept_loop` → `serve_proxy_connection`).
- **kafka** — subconjunto Kafka (`kafka_accept_loop` → `serve_kafka_connection`).
- **admin** — plano de operación HTTP/REST + `/metrics` + SSE (`admin_accept_loop` →
  `serve_admin_connection`).

El plano inter-nodo de Raft (`cluster_accept_loop`) **no** se cuenta: no es tráfico de cliente sino
transporte de consenso, con su propia observabilidad (`nexus_raft_*`).

## Decisión

Se añade el gauge **`nexus_broker_connections_active{plane}`** (`plane ∈ {native, kafka, admin}`) y
se ata su recuento al **ciclo de vida de la corrutina de servicio** de cada conexión mediante RAII,
no a una pareja manual inc/dec:

1. **`GaugeGuard` (RAII) en `telemetry/metrics.hpp`.** Un tipo que **incrementa un `Gauge` al
   construirse y lo decrementa al destruirse**. Es *movible* (transfiere la responsabilidad; el
   origen queda inerte) y **no copiable** (evita el doble decremento). Con puntero nulo queda inerte
   (útil sin métricas cableadas). Al vivir el guard en el *frame* de la corrutina de servicio, el
   decremento ocurre **en cualquier salida** —cierre limpio, error o excepción—, de modo que el
   contador **no puede fugarse**.

2. **Envoltorio de servicio único.** `serve_counting_connection(Gauge& active, task<void> serve)`
   construye el `GaugeGuard` y luego `co_await`ea la corrutina de servicio real. Cada bucle de
   aceptación **envuelve** su `spawn` con este adaptador, pasando el gauge del plano que le
   corresponde. Así el conteo se añade **sin tocar las firmas** de `serve_connection` /
   `serve_kafka_connection` / `serve_admin_connection` (siguen siendo unit-testables sin métricas).

3. **Series estables, resueltas una vez.** En `Server::run` se hace `describe()` de la familia y se
   resuelven las tres series `{plane=...}` **una sola vez**; el `MetricsRegistry` devuelve
   referencias estables (viven en `unique_ptr` y sobreviven al alta de otras series) y el registro
   vive más que los reactores, así que cada bucle captura su referencia sin riesgo de invalidación.

4. **Thread-per-core sin contención.** Todas las conexiones se aceptan y sirven en el **núcleo 0**
   (los bucles de aceptación se lanzan en `main`), así que el inc/dec ocurre siempre en el mismo
   hilo; y aun cruzando hilos, el `Gauge` es atómico (`memory_order_relaxed`), coherente con el resto
   de la agregación de métricas (ADR-0017).

## Consecuencias

- (+) La consola dispone del **nivel de conexiones vivas por plano**, la señal de saturación que
  faltaba, en `/metrics` (con HELP) y en el snapshot estructurado (`/api/v1/metrics/snapshot`), sin
  serializador nuevo (ambos recorren el registro genéricamente).
- (+) **Imposible fugar el contador**: el decremento es un destructor, no una rama que se pueda
  saltar. Cubre el cierre abrupto, el error de E/S y la excepción por igual.
- (+) **No invasivo**: las corrutinas de servicio no cambian de firma; el conteo es un envoltorio
  hermano, igual de testeable que el resto.
- (+) La taxonomía `plane` permite al operador distinguir carga de datos (native/kafka) de carga de
  operación (admin) y detectar, p. ej., un scraper de `/metrics` demasiado agresivo.
- (−) El plano inter-nodo de Raft queda fuera de este gauge (por diseño): su salud se observa por las
  familias `nexus_raft_*`, no como "conexión de cliente".
- (−) El recuento es **por nodo**: en una topología multinodo (hito posterior) el dashboard debe
  agregar por `instance`, como es habitual en Prometheus.

## Alternativas consideradas

- **Pareja manual `gauge.inc()` / `gauge.dec()` en cada bucle de aceptación y cierre.** Frágil: un
  camino de error que no pase por el `dec` fuga el contador de forma permanente y silenciosa. Se
  descarta a favor de RAII, que es justamente la garantía que da C++ para esto.
- **Añadir un parámetro `Gauge*` a cada `serve_*`.** Contamina tres firmas (dos en cabeceras
  compartidas con los tests) y obliga a cada función a recordar decrementar en todas sus salidas.
  El envoltorio único concentra la responsabilidad en un solo punto.
- **Derivar las conexiones activas de contadores (`opened_total - closed_total`).** Requiere dos
  contadores y su resta en el consumidor; el nivel instantáneo es exactamente lo que modela un gauge.
  Además, un `closed_total` que se salte una salida reintroduce el problema de la fuga.
- **Un solo gauge sin la etiqueta `plane`.** Pierde la capacidad de distinguir carga de datos de
  carga de operación, que es una de las preguntas más útiles del dashboard. El coste de la etiqueta
  es nulo (tres series).
