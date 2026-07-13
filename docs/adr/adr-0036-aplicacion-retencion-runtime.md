# ADR-0036: Aplicación de la retención en runtime (barrido periódico por núcleo)

- **Estado:** aceptado
- **Fecha:** 2026-07-13

## Contexto

`PartitionLog::enforce_retention(policy)` existía —reclama los segmentos **sellados** más antiguos por
tamaño o tiempo, nunca el activo, respetando el prefijo frío del tier— pero **no tenía ningún
llamante**: `retentionMs`/`retentionBytes` se almacenaban en la config del topic y se aplicaban al
`LogConfig`, pero **nunca se hacían efectivos**. La retención era, en la práctica, inerte: un log
crecía sin límite aunque el topic declarara una política.

Faltaba, pues, **quién** dispara `enforce_retention`, **cuándo** y **sobre qué particiones**, sin
romper el modelo *shared-nothing thread-per-core* (cada `PartitionLog` es REACTOR-LOCAL de su núcleo
dueño, `partition % N`, [ADR-0026](adr-0026-sharding-por-nucleo.md)) ni los invariantes del `RaftLog`
de las particiones replicadas.

## Decisión

Se cablea la aplicación de retención como una **tarea de mantenimiento periódica por núcleo**,
reactor-local:

1. **Bucle por núcleo.** `Server::start_retention_maintenance` registra en **cada** reactor un
   temporizador periódico (`Reactor::every`, misma vía que los *ticks* de Raft, ADR-0025) con cadencia
   `kRetentionInterval` (30 s). El callback corre en el hilo del núcleo dueño y llama a
   `TopicManager::enforce_retention_all` de **ese** núcleo. Núcleo 0 inline; núcleos 1..N-1 registran
   su temporizador en su propio hilo por el buzón (los temporizadores son reactor-locales, no
   thread-safe).

2. **Lee la config actual.** `enforce_retention_all` recorre los topics del núcleo y deriva una
   `RetentionPolicy` de la config **vigente** de cada topic (`topic->meta().config`) en cada barrido;
   así un `PATCH` de config ([ADR-0037](adr-0037-config-topic-mutable-cross-core.md)) surte efecto en
   el siguiente ciclo sin reiniciar. Los topics sin política (`retention_ms < 0` **y**
   `retention_bytes < 0`) se omiten.

3. **Solo particiones no replicadas.** La retención por política se aplica a las particiones locales
   `Partition` (mono-nodo). Las **réplicas** (`ReplicatedPartition`) reclaman su prefijo por la
   **compactación de Raft** sobre el `commit_index` ([ADR-0024](adr-0024-compactacion-raft-snapshot.md)):
   borrar segmentos bajo el `RaftLog` por retención directa rompería sus invariantes (índices y base
   de snapshot). Se modela con un método virtual `PartitionBase::enforce_retention` **no-op por
   defecto**, sobreescrito solo en `Partition`.

4. **Reclama por segmentos sellados enteros**, nunca el activo (garantía preexistente de
   `enforce_retention`), y nunca por debajo del prefijo frío del tier.

5. **Reloj inyectable.** La edad de un segmento se mide contra el *mtime* de su `.log`.
   `enforce_retention(policy, now)` recibe el instante de referencia (por defecto el reloj de
   fichero), de modo que las pruebas de la retención por tiempo son **deterministas** (se inyecta un
   `now` futuro) sin dormir ni manipular *mtimes*.

## Consecuencias

- (+) La retención declarada por un topic **se aplica de verdad**: los logs dejan de crecer sin
  límite.
- (+) Respeta el confinamiento por núcleo: cada `TopicManager` lo barre su propio reactor; sin
  cross-core, sin locks nuevos en el *hot path*.
- (+) **Determinista y testeable**: el reloj se inyecta; el barrido es una llamada síncrona
  (`enforce_retention_all(now)`) que las pruebas conducen directamente.
- (+) Interopera con `PATCH` de config (lee la config vigente cada ciclo) y con el tier (no toca el
  prefijo frío).
- (−) La reclamación no es instantánea: hay hasta `kRetentionInterval` de retraso entre superar la
  política y reclamar (aceptable para mantenimiento de fondo).
- (−) Las particiones **replicadas** no reclaman por retención de tiempo/tamaño en este corte (solo
  por compactación de Raft): unificar ambas políticas es trabajo futuro.

## Alternativas consideradas

- **Reclamar en el camino de `append` (al rotar segmento):** mete trabajo de mantenimiento en el
  *hot path* de escritura y ata la cadencia al tráfico (un topic ocioso nunca reclamaría); descartado.
- **Un hilo de mantenimiento dedicado (global):** rompe el modelo shared-nothing (tocaría
  `PartitionLog` de otros núcleos con sincronización); descartado a favor del temporizador por
  reactor.
- **Aplicar retención también a las réplicas:** requeriría coordinar con la base de snapshot del
  `RaftLog` para no dejar el consenso inconsistente; se deja para cuando la compactación y la
  retención se unifiquen. Por ahora, no-op en `ReplicatedPartition`.
- **Medir la edad con el `MonoTime` del reactor (steady_clock):** no compara con `last_write_time`
  (reloj de fichero) del segmento; se usa el reloj de fichero, inyectable para tests.
