# 12. Observabilidad

> Cómo se ve por dentro un broker en marcha: métricas, logs y salud. La observabilidad es de
> grado producción y se concentra en el target **`nexus-telemetry`**
> ([ADR-0017](../adr/adr-0017-nexus-telemetry.md)). El flujo se ilustra en el
> [diagrama 23](../diagramas/23-pipeline-observabilidad.md).

## 12.1 Un target dedicado bajo el broker

La telemetría vive en su propia librería, `nexus-telemetry`, por debajo del broker en el grafo
de dependencias ([ADR-0017](../adr/adr-0017-nexus-telemetry.md)): así el núcleo emite métricas
y logs sin depender del detalle de exposición, y se evita un ciclo de dependencias. Agrupa tres
piezas: `metrics`, `logging` y `tracing`.

## 12.2 Métricas (Prometheus)

El nodo expone `GET /metrics` en formato de exposición de Prometheus, sin autenticación (para
no entorpecer al *scraper*). Las métricas cubren las **cuatro señales de oro** —latencia,
tráfico, errores, saturación— y los recursos por el método **USE** (utilización, saturación,
errores). Magnitudes típicas de un broker: throughput de *produce*/*fetch*, latencias por
percentil, *lag* de consumidores, tamaño y *commit* del log, estado de Raft (líder/seguidor,
`commitIndex`), créditos de *backpressure*. La latencia se reporta por **percentiles**
(p50/p99/p999), no por medias (ver [capítulo 23](./23-rendimiento-y-benchmarks.md)).

## 12.3 Logs estructurados

Los logs son **estructurados (JSON)**, con **correlation IDs** que permiten seguir una
petición a través de los planos y los reactores. Se evita el *logging* síncrono caro en el
camino caliente. El idioma de los mensajes de cara al operador es español; los identificadores,
en inglés.

## 12.4 Tracing

`tracing` propaga *correlation IDs* a través del *pipeline* (recepción → decodificación →
*append* → replicación → *ack*) y entre reactores, de modo que una operación distribuida pueda
reconstruirse. Como toda observabilidad, tiene coste: se mide su *overhead* (*observer effect*)
y no se deja activado a ciegas bajo carga.

## 12.5 Salud: liveness y readiness

El plano de salud distingue dos preguntas distintas:

- **`GET /healthz` (*liveness*):** ¿el proceso está vivo? Si falla, el orquestador reinicia.
- **`GET /readyz` (*readiness*):** ¿puede recibir tráfico? Comprueba disco, estado de Raft y
  *lag*; si falla, el orquestador lo saca de balanceo **sin** reiniciarlo.

Esta distinción evita reiniciar procesos "vivos pero no listos" (p. ej. poniéndose al día tras
un *failover*). El `HEALTHCHECK` de la imagen y las *probes* de Kubernetes se cablean a
`/readyz` (ver [capítulo 25](./25-despliegue.md)). El despliegue de observabilidad
(Prometheus + Grafana *self-hosted*) se describe en el
[capítulo 26](./26-configuracion-y-operacion.md).
