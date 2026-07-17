# Catálogo de métricas (as-built)

> **Fuente de verdad de los *nombres* de métricas de NexusMQ.** El contrato REST
> ([`openapi.yaml`](./openapi.yaml)) fija la **forma** del snapshot (`MetricsSnapshot`), pero no los
> nombres de las series; este catálogo los fija. La **consola web y cualquier dashboard codifican
> contra estos nombres**. El código en `src/` (las llamadas `describe()`/`counter()`/`gauge()`/
> `histogram()` del `MetricsRegistry`, [ADR-0017](./adr/adr-0017-nexus-telemetry.md)) es la
> referencia última; este documento lo refleja.
>
> **Regla de mantenimiento:** si añades, renombras o cambias las etiquetas de una métrica, **actualiza
> este catálogo en el MISMO commit**. Así la consola tiene una única fuente y no se desincroniza.

## Cómo se exponen

Las mismas series se ofrecen por tres vías (sin autenticación, como el resto de observabilidad):

- **`GET /metrics`** — formato de texto de Prometheus (con `# HELP`/`# TYPE`).
- **`GET /api/v1/metrics/snapshot`** — snapshot JSON estructurado (`MetricsSnapshot`): una entrada
  por serie con `name`, `type`, `labels` y el valor (`value` para counter/gauge; `count`/`sum`/
  `buckets` para histogram).
- **`GET /api/v1/stream`** — el mismo snapshot emitido por SSE con una cadencia, para dashboards en
  tiempo real ([ADR-0038](./adr/adr-0038-streaming-sse-admin-http.md)).

Los histogramas siguen la convención de Prometheus: series derivadas `_bucket{le=...}` (cubos
acumulativos), `_sum` y `_count`; el cubo `+Inf` es el `_count`.

## Plano de datos del broker

Las cuatro señales de oro por tipo de petición, más el volumen de mensajes. Todas llevan las
etiquetas **`api`** (`produce` | `fetch`) y **`protocol`** (`native` para el protocolo binario
propio, `kafka` para el subconjunto Kafka, que comparte broker y particiones). Un operador agrega
con, p. ej., `sum by (protocol) (...)`.

| Métrica | Tipo | Etiquetas | Unidad | HELP |
|---|---|---|---|---|
| `nexus_broker_requests_total` | counter | `api`, `protocol` | peticiones | Peticiones del plano de datos del broker servidas, por tipo (api) y protocolo (protocol: native\|kafka). |
| `nexus_broker_request_errors_total` | counter | `api`, `protocol` | peticiones | Peticiones del plano de datos con error de wire, por tipo (api) y protocolo. |
| `nexus_broker_request_bytes_total` | counter | `api`, `protocol` | bytes | Bytes de payload del plano de datos (producido/servido), por tipo (api) y protocolo. |
| `nexus_broker_messages_total` | counter | `api`, `protocol` | records | Records (mensajes) del plano de datos producidos/servidos, por tipo (api) y protocolo; un request agrupa N records en un batch. |
| `nexus_broker_request_duration_seconds` | histogram | `api`, `protocol` | segundos | Latencia de servicio del plano de datos en segundos, por tipo (api) y protocolo. |

> `messages_total` es el nº de **records** (mensajes) y es **distinto** de `requests_total`: un único
> request de produce/fetch agrupa N records en un batch. Para la tasa de mensajes usa
> `rate(nexus_broker_messages_total[...])`; para la de peticiones, `rate(nexus_broker_requests_total[...])`.

## Conexiones del broker

| Métrica | Tipo | Etiquetas | Unidad | HELP |
|---|---|---|---|---|
| `nexus_broker_connections_active` | gauge | `plane` | conexiones | Conexiones de cliente activas por plano (plane: native\|kafka\|admin); RAII las cuenta durante su vida. |

La etiqueta **`plane`** distingue el plano de datos nativo (`native`), el subconjunto Kafka
(`kafka`) y el plano de operación HTTP/REST (`admin`). El conteo está atado por RAII al ciclo de
vida de la conexión, así que no se fuga ante cierres abruptos
([ADR-0039](./adr/adr-0039-gauge-conexiones-activas-raii.md)). El plano inter-nodo de Raft **no** se
cuenta aquí (su salud son las familias `nexus_raft_*`).

## Consenso (Raft) por partición

Presentes **solo en particiones replicadas** (`replication_factor ≥ 2`); en single-node/RF=1 no hay
réplicas de Raft y estas series no se emiten (el estado de topología en single-node se ve por
[`GET /api/v1/cluster`](./openapi.yaml), [ADR-0035](./adr/adr-0035-estado-cluster-raft-rest-admin.md)).
Todas llevan las etiquetas **`topic`** y **`partition`**; `nexus_raft_follower_lag` añade **`peer`**.

| Métrica | Tipo | Etiquetas | Unidad | HELP |
|---|---|---|---|---|
| `nexus_raft_commit_index` | gauge | `topic`, `partition` | índice | High-watermark de la réplica de Raft (entradas aplicadas). |
| `nexus_raft_term` | gauge | `topic`, `partition` | término | Término actual de la réplica de Raft. |
| `nexus_raft_leader` | gauge | `topic`, `partition` | booleano (0/1) | Rol de la réplica: 1 si es líder, 0 en otro caso. |
| `nexus_raft_log_last_index` | gauge | `topic`, `partition` | índice | Último índice del log local de la réplica. |
| `nexus_raft_uncommitted_entries` | gauge | `topic`, `partition` | entradas | Entradas escritas aún no confirmadas a quórum (last_log_index - commit). |
| `nexus_raft_follower_lag` | gauge | `topic`, `partition`, `peer` | entradas | Retraso de un seguidor: last_log_index - match_index (visto por el líder). |
| `nexus_raft_messages_sent_total` | counter | `topic`, `partition` | mensajes | Mensajes de Raft transportados por la réplica. |
| `nexus_raft_messages_received_total` | counter | `topic`, `partition` | mensajes | RPC de Raft entregados a la FSM de la réplica. |
| `nexus_raft_entries_replicated_total` | counter | `topic`, `partition` | entradas | Entradas enviadas en AppendEntries por el líder. |
| `nexus_raft_commit_latency_seconds` | histogram | `topic`, `partition` | segundos | Latencia de confirmación a quórum de una entrada (propose -> commit). |
