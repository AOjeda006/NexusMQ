# 10. Replicación y consenso

> Cómo NexusMQ tolera fallos sin perder datos *committed*: **Raft por partición**, con el log
> de Raft como WAL. Es la Fase 2 del proyecto y el corazón de su *correctitud demostrable*.
> Decisiones: [ADR-0003](../adr/adr-0003-replicacion-raft-por-particion.md),
> [0014](../adr/adr-0014-modelo-log-raft.md), [0015](../adr/adr-0015-raftnode-fsm-sin-io.md),
> [0016](../adr/adr-0016-replicated-partition.md),
> [0024](../adr/adr-0024-compactacion-raft-snapshot.md),
> [0025](../adr/adr-0025-activacion-raft-multireactor-transporte.md).

## 10.1 Un grupo Raft por partición

Cada partición es **su propio grupo Raft**: un único mecanismo de ordenación, replicación y
elección. Frente al ISR clásico de Kafka (consenso solo para metadatos + primario-backup para
datos), Raft por partición es **conceptualmente uniforme**, **formalmente especificado** y
**testeable de forma determinista**, y compone con shared-nothing (un grupo anclado a un
núcleo). El precio es el **quórum en cada escritura** (latencia de mayoría) en el camino
caliente, asumido por la postura CP. Los estados de un nodo
(`follower → candidate → leader`) están en el [diagrama 10](../diagramas/10-estados-raft.md).

## 10.2 El log de Raft es el WAL

No hay un WAL separado del log replicado: **el log de Raft *es* el log de la partición**. De
ahí dos consecuencias clave:

- **High-watermark = `commitIndex`.** El offset visible a los consumidores es el `commitIndex`
  del grupo Raft; no existe un mecanismo de ISR aparte. El modelo de `acks` se asienta sobre
  el *commit*: `acks=0` responde sin esperar (*at-most-once*); `acks=1`, tras el *append*
  local del líder; `acks=quorum` (por defecto), tras el *commit* de Raft. Ver el flujo de
  replicación en el [diagrama 11](../diagramas/11-replicacion-commit.md).
- **Dos espacios de coordenadas** ([ADR-0014](../adr/adr-0014-modelo-log-raft.md)): el
  **índice de entrada Raft** (ordinal de entrada; **una entrada = un `RecordBatch`**) es
  distinto del **offset por record** (dentro del batch). `RaftLog` envuelve el `PartitionLog`
  y persiste `term`/offsets por entrada en un **sidecar** de tamaño fijo, dejando el
  `RecordBatch` y `nexus-storage` **sin cambios**. La correspondencia se ilustra en el
  [diagrama 13](../diagramas/13-espacios-coordenadas.md).

## 10.3 RaftNode: una máquina de estados sin E/S

El núcleo de Raft, `RaftNode`, es una **máquina de estados síncrona sin E/S**
([ADR-0015](../adr/adr-0015-raftnode-fsm-sin-io.md)): recibe **entradas** (`tick(now)`,
`on_append_entries`, `on_request_vote`, `on_propose`…) con el reloj **inyectado** y produce
una **cola de mensajes de salida** drenable; no hace ni red ni disco por sí mismo. Esta
separación núcleo/E-S es lo que habilita la **simulación determinista** (reloj y red
virtuales): se reproducen *timing*, elecciones de líder y particiones de forma **repetible**,
cumpliendo FIRST sin *flakiness* (ver [capítulo 21](./21-estrategia-de-pruebas.md)). El estado
persistente (`currentTerm`, `votedFor`, `log[]`) se guarda **antes** de enviar nada por la red.

## 10.4 ReplicatedPartition y el transporte inter-nodo

`ReplicatedPartition` es un **tipo paralelo** a `Partition`
([ADR-0016](../adr/adr-0016-replicated-partition.md)) que compone la pila Raft sobre el log
de la partición. En producción, los RPC de Raft viajan por un **plano inter-nodo separado**
del plano de cliente ([ADR-0025](../adr/adr-0025-activacion-raft-multireactor-transporte.md)):

- Un **sobre de wire** (`topic | partition | from | to | type | payload`, longitud-prefijo)
  rutea cada `RaftMessage` a la réplica `(topic, partition)` destino, reutilizando el
  `encode`/`decode` por RPC.
- Un **portador por partición** (`RaftCarrier`), afinado al reactor dueño vía
  `PartitionRouter`, conduce la FSM (`tick`/`take_messages`/`on_*`), **persiste el estado
  antes de enviar** y dispara la compactación por política.
- El `Server` se monta sobre `ReactorPool` y el `RequestRouter` enruta async al reactor dueño.

Los dos planos (cliente vs Raft, en puertos separados) se ven en el
[diagrama 15](../diagramas/15-planos-red.md).

## 10.5 Failover sin pérdida de datos committed

Al expirar el *election timeout* sin *heartbeat* del líder, un *follower* pasa a *candidate*,
incrementa el `term` y solicita votos (`RequestVote`, con *pre-vote* opcional); si obtiene
mayoría, se promueve a líder. **Solo puede ganar quien tenga el log al menos tan actualizado
como la mayoría**, así que **no se pierden datos *committed***. Los clientes redescubren el
líder vía *metadata* (un líder obsoleto responde `NOT_LEADER_FOR_PARTITION`, detectado por el
`leaderEpoch`). El proceso está en el [diagrama 12](../diagramas/12-failover-eleccion-lider.md).

## 10.6 Compactación por snapshot

Para que el log replicado no crezca sin límite,
[ADR-0024](../adr/adr-0024-compactacion-raft-snapshot.md) añade una **base de snapshot**
`(last_included_index, last_included_term, last_included_offset)` persistida en un sidecar
dedicado (`raft-snapshot`, registro fijo con CRC32C). `compact_to(index)` descarta el prefijo
ya aplicado (exacto en el índice) y recorta el prefijo **físico** del `PartitionLog` por
**segmentos sellados enteros** (*best-effort*). Un seguidor muy rezagado se pone al día con
**`InstallSnapshot`**, adoptando la base del líder
([diagrama 14](../diagramas/14-snapshot-installsnapshot.md)).

## 10.7 Postura de consistencia

NexusMQ es **CP / PACELC PC-EC**
([ADR-0007](../adr/adr-0007-consistencia-cp-pacelc.md)): ante una partición de red, una
partición de datos sin quórum **deja de aceptar escrituras** (no diverge); en operación
normal, las escrituras esperan al quórum (`acks=quorum`), priorizando consistencia sobre
latencia. La lectura por defecto es desde el **líder** hasta el *high-watermark*; las lecturas
*stale* desde *followers* son *opt-in* y documentadas.
