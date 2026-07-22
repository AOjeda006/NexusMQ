# 15. API REST de administración

> Plano de control HTTP para operar el broker: gestión de *topics* y grupos, salud y
> métricas. Este capítulo **explica y enlaza** el contrato; la fuente de verdad —rutas,
> esquemas y respuestas exactas— es [`docs/openapi.yaml`](../openapi.yaml).

## 15.1 Rationale y ubicación

La administración no viaja por el plano de datos binario, sino por una **API REST**
separada, expuesta en su propio puerto (`--admin-port`). La separación es deliberada
([ADR-0018](../adr/adr-0018-rest-admin-puerto-adaptador.md)): el contrato HTTP/JSON se
sirve por un **adaptador** (`AdminService` en `nexus-ingress`, `AdminApi` en
`nexus-server`) que traduce peticiones REST a operaciones del broker, sin acoplar el
núcleo al framework web. Es el plano de control —no el camino caliente—, así que aquí
**sí** rigen las excepciones y la traducción central de errores (ver
[capítulo 16](./16-modelo-errores-wire-codes.md)).

## 15.2 Recursos y operaciones

Bajo `/api/v1`, recursos en plural y verbos HTTP (sin acciones en la URL):

| Método y ruta | Propósito |
| ------------- | --------- |
| `GET /api/v1/topics` | Lista los *topics* (paginado). |
| `POST /api/v1/topics` | Crea un *topic*; responde `201` con cabecera `Location`. |
| `GET /api/v1/topics/{name}` | Describe un *topic* (resumen + particiones + `config`). |
| `PATCH /api/v1/topics/{name}` | Ajusta en caliente la config de retención (`retentionMs`/`retentionBytes`) y la publica cross-core ([ADR-0037](../adr/adr-0037-config-topic-mutable-cross-core.md)); `segmentBytes` es de solo-creación → `400`. |
| `DELETE /api/v1/topics/{name}` | Borra un *topic* **y sus datos en disco** (el `.log`/`.index` de cada partición). |
| `GET /api/v1/groups` | Lista los grupos de consumidores (paginado). |
| `GET /api/v1/groups/{id}` | Describe un grupo: miembros, suscripción, offsets confirmados y *lag* por partición. |
| `GET /api/v1/cluster` | Estado del clúster y de Raft por partición (nodos, rol, *leader*, *commit index*, progreso de *followers*) agregado cross-core ([ADR-0035](../adr/adr-0035-estado-cluster-raft-rest-admin.md)); las particiones **no replicadas** se sintetizan como líder estático ([ADR-0040](../adr/adr-0040-topologia-raft-single-node.md)). |

Fuera de `/api/v1` y **sin autenticación**, el plano de salud/observabilidad:

| Ruta | Propósito |
| ---- | --------- |
| `GET /healthz` | *Liveness*: ¿el proceso está vivo? |
| `GET /readyz` | *Readiness*: ¿puede recibir tráfico? (disco/Raft/*lag*). |
| `GET /metrics` | Métricas en formato de exposición de Prometheus. |
| `GET /api/v1/metrics/snapshot` | Snapshot **estructurado** de métricas (JSON) para la consola; mismo recorrido que `/metrics`, servido por el `AdminRouter` sin autenticar. |
| `GET /api/v1/stream` | Stream **SSE** (`text/event-stream`) que emite el snapshot de métricas por *push* con una cadencia ([ADR-0038](../adr/adr-0038-streaming-sse-admin-http.md)). |

`/healthz` y `/readyz` solo aceptan **`GET`** (y `HEAD`); cualquier otro método responde
**`405 Method Not Allowed`**, para que una *probe* mal configurada (p. ej. un `POST`) falle de forma
explícita en vez de tratarse como una consulta de salud.

La distinción *liveness*/*readiness* permite que un orquestador reinicie procesos
"vivos pero no listos" sin sacarlos de servicio prematuramente (ver
[capítulo 12, Observabilidad](./12-observabilidad.md)).

## 15.3 Autenticación

Las rutas `/api/v1/*` se autentican con **Bearer JWT** (HS256), firmado con el secreto
del nodo. La autenticación **solo se exige si el nodo arrancó con `--jwt-secret`**; en
ese caso, una petición sin un token válido recibe `401`. El plano de salud/métricas
queda siempre sin autenticar para no entorpecer a las sondas y al *scraper*. El detalle
de TLS y la postura de seguridad están en el [capítulo 27](./27-seguridad.md).

## 15.4 Paginación y errores

- **Colecciones paginadas:** `GET /api/v1/topics` y `GET /api/v1/groups` paginan en
  lugar de devolver listas sin límite.
- **Errores RFC 7807:** toda respuesta de error usa el esquema `ProblemDetail`
  (`application/problem+json`), con una **política central** de traducción. Esto da al
  cliente un formato de error uniforme y predecible (códigos `400`/`401`/`404`/`409`/…).
- **Validación de nombre de *topic*:** `POST /api/v1/topics` con un nombre inválido
  (vacío, con espacios o separadores de ruta, `.`/`..`, o de más de 249 caracteres)
  responde `400`. La regla vive en `TopicManager::validate_topic_name` (**fuente única**),
  de modo que el protocolo binario nativo aplica exactamente la misma validación —traducida
  a su código de wire— y ninguna superficie acepta lo que otra rechaza.

## 15.5 Superficie para la consola web

Sobre el puerto de operación se apoya una **consola de administración** (repositorio aparte). Para
alimentarla sin acoplar el núcleo, la API añade lecturas ricas y un canal en tiempo real:

- **Snapshot de métricas** (`GET /api/v1/metrics/snapshot`): el mismo recorrido que la exposición
  Prometheus, pero en JSON estructurado (nombre, tipo, *labels*, valor/percentiles). Lo sirve el
  `AdminRouter` —que ya tiene el `MetricsRegistry`— y queda **sin autenticar**, como `/metrics`.
- **Streaming SSE** (`GET /api/v1/stream`): un camino de respuesta hermano del *buffered* (una
  petición, una respuesta con `Content-Length`). Se desvía **antes** de enrutar por el modelo normal;
  emite frames `event: metrics` con el snapshot y una cadencia, y observa la señal de **drenaje** para
  cerrar limpio en el apagado ([ADR-0038](../adr/adr-0038-streaming-sse-admin-http.md)). Es
  unidireccional (servidor→cliente); los comandos siguen yendo por el REST puntual.
- **Describe de grupo y estado de clúster** (`GET /api/v1/groups/{id}`, `GET /api/v1/cluster`):
  agregaciones **cross-core** (`call_on` al núcleo dueño de cada grupo/partición) que exponen *lag*,
  offsets y el estado de Raft por partición sin filtrar los tipos internos (se copian a DTOs). El
  recorrido de `/cluster` unifica los dos casos para que la vista de topología nunca salga vacía: las
  particiones **replicadas** (RF≥2) se leen de su portador de Raft y las **no replicadas** (RF=1, la
  única topología hoy arrancable) se sintetizan como **líder estático** de este nodo —`role: leader`,
  `term: 0`, `commitIndex == lastLogIndex ==` *high-watermark* del log local, sin *followers*— con
  datos coherentes con el `describe` del *topic*
  ([ADR-0040](../adr/adr-0040-topologia-raft-single-node.md)).
- **Config mutable** (`PATCH /api/v1/topics/{name}`): la retención se ajusta en caliente y se publica a
  todos los núcleos; el barrido periódico por núcleo la aplica en el siguiente ciclo
  ([ADR-0036](../adr/adr-0036-aplicacion-retencion-runtime.md), [ADR-0037](../adr/adr-0037-config-topic-mutable-cross-core.md)).

## 15.6 Documentación como contrato

`openapi.yaml` no es solo documentación: es un **artefacto de tooling** (Swagger UI,
generación de clientes, *contract testing*). Por eso se mantiene como fichero
independiente y este capítulo no reproduce sus esquemas: consúltalos en
[`docs/openapi.yaml`](../openapi.yaml). El flujo de autenticación y petición se ilustra
en el [diagrama 18](../diagramas/18-flujo-rest-jwt.md).
