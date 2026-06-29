# 17. Mapa de módulos

> La traducción del diseño a *targets* de build: qué librerías existen, cómo dependen entre sí
> y qué se entregó en cada fase. Es la vista de implementación de la
> [vista de conjunto](./05-vista-de-conjunto.md).

## 17.1 Targets

Un único árbol CMake produce **15 librerías** `nexus-*`, los ejecutables y las herramientas:

- **Librerías:** `nexus-common`, `nexus-io`, `nexus-wire`, `nexus-protocol`, `nexus-storage`,
  `nexus-reactor`, `nexus-consensus`, `nexus-cluster`, `nexus-broker`, `nexus-kafka`,
  `nexus-ingress`, `nexus-telemetry`, `nexus-server`, `nexus-client`, `nexus-ffi`.
- **Ejecutables:** `nexusd` (el servidor), y las *tools* `nexus-cli`, `nexus-bench`,
  `nexus-loadgen`, más `wincheck` (arnés Windows-only).

`nexus-wire` es una librería **INTERFACE** (solo cabeceras: el framing sobre conexión);
`nexus-ffi` se compila como **SHARED** (su superficie es una ABI C estable, ver
[capítulo 20](./20-herramientas-y-bindings.md)).

## 17.2 Grafo de dependencias y regla de capas

Las dependencias **apuntan hacia el núcleo**; ningún *target* depende de otro de su misma capa
o superior. La regla la fuerza CMake (`target_link_libraries` con `PUBLIC`/`PRIVATE`). De abajo
arriba: `nexus-common` (sin dependencias internas) → `io`/`wire` → `protocol`/`storage` →
`reactor` → `consensus`/`cluster` → `broker`/`kafka` → `ingress`/`telemetry` →
`server`/`client`; `nexusd` depende de `nexus-server`. El grafo completo —incluidas las aristas
exactas— está en el [diagrama 3](../diagramas/03-grafo-dependencias.md).

Esta disciplina mantiene el núcleo (storage, broker, consenso) **independiente del detalle de
I/O y de protocolo**: cuando una capa inferior necesitaría a una superior, se invierte la
dependencia con un **puerto** en el consumidor y un **adaptador** arriba (DIP), como en
`nexus-telemetry` ([ADR-0017](../adr/adr-0017-nexus-telemetry.md)) y en la REST admin
([ADR-0018](../adr/adr-0018-rest-admin-puerto-adaptador.md)).

## 17.3 Mapa fase → targets

Cada fase entregó un subconjunto demoable (ver
[diagrama 4](../diagramas/04-mapa-fase-targets.md) y la
[historia de desarrollo](./29-historia-de-desarrollo.md)):

| Fase | Targets principales | Entregable |
| ---- | ------------------- | ---------- |
| **1** | `nexus-common`, `nexus-storage`, `nexus-bench` | Motor de log monopartición + benchmarks (I/O bloqueante). |
| **1b** | `nexus-io`, `nexus-wire`, `nexus-protocol`, `nexus-reactor`, `nexus-broker`, `nexus-client` | Broker de un nodo *thread-per-core*; *produce*/*fetch* con cliente nativo. |
| **2** | `nexus-consensus`, `nexus-cluster` (+ `broker`) | Raft por partición, *failover*, grupos de consumidores. |
| **3** | `nexus-ingress`, `nexus-telemetry`, `nexus-server`, `nexus-cli` | Ingress (TLS, REST admin), CLI, métricas. |
| **4** | `nexus-kafka`, `nexus-ffi`, `nexus-loadgen`, `wincheck` (+ IOCP en `nexus-io`) | Subset Kafka, binding Python, loadgen, port Windows. |

El detalle de cada librería está en el [catálogo por subsistema](./18-catalogo-por-subsistema.md).
