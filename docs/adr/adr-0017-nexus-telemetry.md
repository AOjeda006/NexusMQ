# ADR-0017: Target `nexus-telemetry` para observabilidad (métricas/logs) bajo el broker

- **Estado:** aceptado
- **Fecha:** 2026-06-17

## Contexto

Este ADR **refina el desglose** (no cambia ninguna decisión de arquitectura previa): el desglose detallado (§4.9) ubicaba `metrics.{hpp,cpp}` dentro de `nexus-server`, que es el **ejecutable** del broker. La decisión mueve la observabilidad a una **biblioteca** propia para respetar el grafo de dependencias.

La `MetricsRegistry` es **THREAD-SAFE** y la **alimentan** capas del plano de datos (broker/reactor: tasa de produce/fetch, *lag*, `commit_index`/term de Raft, latencias) mientras que la **exponen** el gateway REST (`nexus-ingress`, que tiene un `MetricsRegistry&`) y el servidor (`/metrics`). Si viviera en `nexus-server` (un *exe*, no enlazable como dependencia) ninguna biblioteca inferior podría registrar métricas; y colocarla en `nexus-ingress` crearía una dependencia ascendente `broker → ingress` (ciclo de capas). Lo mismo aplica al logger JSON estructurado (lo usa todo el árbol). `nexus-common` se reserva para vocabulario mínimo (tipos, errores, bytes); una registry con mutex, familias y render Prometheus no encaja ahí.

## Decisión

Se crea el target **`nexus-telemetry`** (`src/telemetry/`), que **depende solo de `nexus-common`** y se sitúa **bajo** broker/ingress/server en el grafo. Aloja `MetricsRegistry` (contadores/gauges/histogramas + exposición Prometheus) y, más adelante, el logger JSON estructurado. La registry es THREAD-SAFE con un **mutex que protege solo la estructura** (alta de series y recorrido en `render`); las **actualizaciones de valor** (`inc`/`set`/`observe`) son **atómicas y sin candado** (lock-free en el camino caliente). Cualquier capa (broker, reactor, ingress, server) enlaza `nexus-telemetry` para registrar o exponer.

## Consecuencias

- (+) Grafo de dependencias **acíclico**: el plano de datos registra sin depender de capas superiores.
- (+) Observabilidad **testeable en aislamiento** (sin red ni servidor).
- (+) `nexus-common` se mantiene mínimo.
- (+) Registro de valores **lock-free**; solo el alta de una serie nueva toma el mutex.
- (−) Un target más que mantener.
- (−) El *sharding* por núcleo de los contadores (cero contención total) queda como optimización futura medida; hoy basta con atómicos.

## Alternativas consideradas

- **`metrics` en `nexus-server` (desglose literal):** imposible que las bibliotecas inferiores registren métricas (un *exe* no es dependencia); descartado.
- **`metrics` en `nexus-ingress`:** crea el ciclo de capas `broker → ingress`; descartado.
- **`metrics` en `nexus-common`:** mete infraestructura con estado (mutex, familias, render) en la capa de vocabulario mínimo; descartado por cohesión.
