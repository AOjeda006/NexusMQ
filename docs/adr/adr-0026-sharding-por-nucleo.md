# ADR-0026: Sharding por núcleo del plano de datos — partición→núcleo y coordinación de grupos por hash

- **Estado:** aceptado
- **Fecha:** 2026-06-21

## Contexto

ADR-0025 (punto 4) decidió mover el `Server` a un `ReactorPool` y volver asíncrono el `RequestRouter`, pero dejó a **alto nivel** *cómo* se reparte el estado entre núcleos. Tras D3.4a (`dispatch` asíncrono) y D3.4b (`Server` sobre `ReactorPool` con el plano de datos **confinado al núcleo 0**, sin *data races*), toca decidir el **sharding** concreto.

El modelo es *shared-nothing thread-per-core* (ADR-0005): ningún estado mutable se comparte entre hilos; el único canal entre núcleos es el paso de mensajes (`PartitionRouter`/`call_on`, ya existentes y probados). Hay tres estados que ubicar:

- (a) el **log/estado de cada partición** (`Partition`/`ReplicatedPartition`, hoy todos en un `TopicManager` único),
- (b) los **metadatos de topics** (qué topics existen y con cuántas particiones),
- (c) la **coordinación de grupos de consumidores** y sus **offsets confirmados** (`GroupCoordinator`/`OffsetManager`, hoy una instancia REACTOR-LOCAL por router).

## Decisión

Tres reglas de ubicación, coherentes con shared-nothing:

1. **Partición → núcleo dueño = `partition % N`** (ya lo fija `PartitionRouter`). El **estado de cada partición** (su log, su pila Raft) vive **solo** en el reactor dueño. El `TopicManager` se **fragmenta**: cada núcleo abre e instancia únicamente las particiones que le tocan; las demás no existen en su memoria. Las operaciones de partición (Produce/Fetch) se enrutan al núcleo dueño con `call_on` (cross-core local y síncrono entre reactores; ADR-0025).

2. **Metadatos de topics = inmutables tras crear, replicados por valor a cada núcleo.** El conjunto `{nombre topic → nº particiones}` es plano de control que cambia raras veces (Create/DeleteTopic). Cada núcleo guarda **su propia copia** (INMUTABLE entre cambios), de modo que `Metadata` y la validación ("¿existe la partición P?") se responden **localmente** sin cross-core. Un cambio de metadatos se propaga publicando la nueva copia a cada núcleo vía `submit_to` (control, no *hot path*). No hay un `TopicManager` compartido con lock: se elige réplica inmutable por núcleo frente a estado compartido sincronizado.

3. **Grupo de consumidores → núcleo coordinador = `hash(group_id) % N`** (estilo Kafka *group coordinator*). La membresía del grupo (`GroupCoordinator`) y sus **offsets confirmados** (`OffsetManager`) viven en **un solo** núcleo —el coordinador del grupo—, no replicados. Las operaciones de grupo (JoinGroup/SyncGroup/Heartbeat/OffsetCommit/OffsetFetch) se enrutan a ese núcleo con `call_on`. Así cada grupo tiene un **dueño único** (linealizable sin locks) y la carga de grupos se reparte entre núcleos. El listado de grupos del puerto admin agrega con `call_on` sobre todos los núcleos.

### Función de hash de ubicación

`hash(group_id)` usa una función estable y documentada (FNV-1a sobre los bytes del id), parte del **contrato interno** de ubicación; no viaja por wire.

## Consecuencias

- (+) Cero estado mutable compartido entre hilos: el modelo sigue siendo shared-nothing puro (ADR-0005); la corrección no depende de locks sino del confinamiento por reactor.
- (+) Produce/Fetch y las operaciones de grupo escalan con los núcleos; cada partición y cada grupo tienen un dueño único que serializa sus operaciones sin contención global.
- (+) `Metadata`/validación son locales (sin cross-core en el *hot path* de cada petición).
- (−) Una petición que toca una partición o un grupo de **otro** núcleo paga un salto cross-core (`call_on`: encolar en el buzón del dueño más reanudar); es el coste inherente del sharding y se mide en D3.4.
- (−) Los metadatos de topics se **duplican** por núcleo: aceptable por ser pequeños e inmutables entre cambios, pero la propagación de un Create/DeleteTopic debe alcanzar a todos los núcleos antes de servir el nuevo estado.
- (−) El `RequestRouter` pasa a tener **una instancia por núcleo** y a depender del `PartitionRouter`/`call_on`; su modelo de test cambia (de `sync_wait` sin reactor a conducir un reactor real o un doble).

## Alternativas consideradas

- **`TopicManager` único compartido con lock (RW-mutex):** rompe shared-nothing, introduce contención en el *hot path* y *cache ping-pong* en el lock; contradice ADR-0005 y la normativa de concurrencia ("la mejor región crítica es la que no existe"). Descartada.
- **Coordinador de grupos global en el núcleo 0:** concentra toda la carga de grupos en un núcleo (punto caliente) y serializa grupos independientes. Se reparte por hash del `group_id`. Descartada.
- **Replicar también offsets/membresía de grupo a todos los núcleos:** exigiría sincronizar escrituras entre núcleos (consenso o locks) para mantener un único valor confirmado por grupo; rompe el dueño único. Se confina cada grupo a su coordinador. Descartada.
- **Sharding de particiones por *hash(topic,partition)* en vez de `partition % N`:** repartiría mejor topics con pocas particiones, pero rompe la igualdad con `PartitionRouter` (ya probado) y el mapeo directo partición→Raft→núcleo. Se mantiene `partition % N`. Descartada.
