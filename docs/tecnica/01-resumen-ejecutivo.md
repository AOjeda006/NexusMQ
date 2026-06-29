# 1. Resumen ejecutivo

> Qué es NexusMQ, qué demuestra, cómo es su arquitectura en un vistazo y en qué estado
> se encuentra. Una página para situarse antes de entrar en el detalle.

## Qué es

NexusMQ es un **broker de mensajería distribuido de alto rendimiento** escrito en **C++23**.
Adopta el modelo de **log particionado** estilo Kafka: los productores **añaden** registros al
final de un log *append-only* y los consumidores **leen** desde el *offset* que elijan y avanzan
a su ritmo —el broker no borra al entregar, retiene por tiempo o tamaño—. Un *topic* es un flujo
lógico dividido en **particiones**; cada partición es una secuencia ordenada e inmutable de
registros, cada uno identificado por un *offset* monótono.

No pretende competir con Kafka en producción. Su propósito es **demostrar**, en una base de código
propia y comprensible, las técnicas que hacen viable un sistema así. La arquitectura es la
**frontera actual** del diseño de brokers (la que usa Redpanda), no la de hace una década.

## Qué demuestra

El proyecto persigue **profundidad dentro de una sola tesis arquitectónica**, no amplitud. Lo que
busca evidenciar es:

- **Profundidad técnica en un sistema coherente** — correctitud distribuida (consenso y
  replicación), latencia (camino caliente sin contención) y funcionamiento *end-to-end* (de la
  conexión del cliente al *fsync* en disco).
- **Criterio de ingeniería** — cada técnica avanzada se introduce **donde un *profiling* lo
  justifica** ("medido, no *checklist*"), y cada decisión de arquitectura queda registrada y
  argumentada como un [ADR](../adr/).

## Arquitectura en un vistazo

- **Shared-nothing thread-per-core.** Un **reactor por núcleo físico**, *pinned* a su CPU, dueño
  exclusivo de su anillo io_uring, su *allocator* y su subconjunto de particiones. **No hay estado
  mutable compartido** entre reactores: se comunican por **paso de mensajes** (colas SPSC
  *cross-core*). Elimina la clase de bug más cara —las carreras sobre estado compartido—.
  Ver [ADR-0005](../adr/adr-0005-concurrencia-shared-nothing-thread-per-core.md) y el diagrama
  [05 — topología thread-per-core](../diagramas/05-topologia-thread-per-core.md).
- **Raft por partición.** Cada partición es su propio **grupo Raft**: su log replicado *es* el log
  de la partición (el WAL). El *high-watermark* es el `commitIndex` de Raft, y el modelo de `acks`
  (`0`/`1`/`quorum`) se asienta sobre su *commit*. Postura **CP** (consistencia sobre
  disponibilidad; PACELC PC/EC). Ver
  [ADR-0003](../adr/adr-0003-replicacion-raft-por-particion.md) y
  [ADR-0007](../adr/adr-0007-consistencia-cp-pacelc.md).
- **I/O por *completions* (proactor).** Abstracción común a **io_uring** en Linux —usado
  **directamente sobre el uapi del kernel**, sin `liburing`
  ([ADR-0012](../adr/adr-0012-io_uring-directo-uapi.md))— e **IOCP** en Windows. Sobre el proactor
  se asientan las **corutinas de C++23**. Ver
  [ADR-0002](../adr/adr-0002-modelo-io-asincrona-proactor.md) y el diagrama
  [07 — secuencia proactor](../diagramas/07-secuencia-proactor.md).
- **Protocolo binario propio** (framing + *multiplexing* por *correlation ID* + versionado) con
  **gateway REST** para administración y un **subconjunto Kafka-compatible** ya implementado
  (interop con `kcat`). Ver [ADR-0004](../adr/adr-0004-protocolo-binario-propio-gateway-rest.md) y
  [ADR-0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md).
- **Capa de *ingress* en dos modos:** cliente nativo directo al líder (primario) + proxy/REST
  (*opt-in*). Ver [ADR-0006](../adr/adr-0006-ingress-dos-modos.md).

La solución son **15 librerías `nexus-*`** (common, io, wire, protocol, storage, reactor,
consensus, cluster, broker, kafka, ingress, telemetry, server, client, ffi) más los ejecutables
(`nexusd`, `nexus-cli`, `nexus-bench`, `nexus-loadgen`) y herramientas de soporte. La vista de
conjunto está en el diagrama [02 — contenedores C4](../diagramas/02-contenedores-c4.md) y el grafo
de dependencias en [03 — grafo de dependencias](../diagramas/03-grafo-dependencias.md).

## Estado

NexusMQ está construido **por fases**, cada una autocontenida y demoable. Las **fases 1 a 4 están
implementadas**:

| Fase | Nombre | Entregable |
| ---- | ------ | ---------- |
| **1** | Storage engine | Motor de log monopartición con cifras de rendimiento. I/O bloqueante, sin reactor. |
| **1b** | Reactor + broker monolítico | Broker de un nodo *thread-per-core*: publicar y consumir con cliente nativo. |
| **2** | Distribución | Cluster tolerante a fallos (Raft por partición, *failover*). |
| **3** | Ingress + operación | Plataforma operable y observable (TLS, REST admin, CLI, métricas Prometheus). |
| **4** | *Stretch* | Productor idempotente, DLQ, compactación, LZ4/Zstd, *direct I/O*, subconjunto Kafka, IOCP (Windows). |

El objetivo primario es **Linux x86-64**; el árbol completo compila y pasa la suite con
GCC/libstdc++ y Clang/libc++, y el port a **Windows** está **verificado en runtime con MSVC**
(ADR-0023/0028). El mapa fase→targets está en el diagrama
[04 — mapa fase-targets](../diagramas/04-mapa-fase-targets.md).

## Dónde están las cifras

Este documento **no fabrica números de rendimiento**. La metodología de medición —percentiles
p50/p99/p999, generación de carga *open-loop* para evitar *coordinated omission*— y todos los
resultados reproducibles viven en el contrato as-built [`benchmarks.md`](../benchmarks.md),
explicado en el capítulo [Rendimiento y benchmarks](./23-rendimiento-y-benchmarks.md).
