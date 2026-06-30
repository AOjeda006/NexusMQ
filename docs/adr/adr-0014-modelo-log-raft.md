# ADR-0014: Modelo del log de Raft (índice = ordinal de entrada; término en sidecar)

- **Estado:** aceptado
- **Fecha:** 2026-06-14

## Contexto

Este ADR **refina el diseño detallado** (no cambia ADR-0003): concreta *cómo* `RaftLog` es «una vista `(term, index)` sobre el `PartitionLog`» cuando el formato de `RecordBatch` (§5.4) no lleva campo de término y los offsets del log son **por record** (un batch abarca varios offsets).

ADR-0003 fija que «el log de Raft **es** el log de la partición». Al implementarlo surgen dos fricciones:

1. El `RecordBatch` (la unidad de replicación, §5.4) **no tiene** campo de término, y añadírselo cambiaría el formato en disco y su CRC (toca todo `nexus-storage`).
2. El offset de partición es **por record** (un batch de N records ocupa N offsets), mientras que Raft razona con un **índice por entrada**. Igualar «índice = offset» obligaría a entradas de un solo record (mata el *batching*, contrario al §5.4) o a partir batches al truncar.

## Decisión

Una **entrada de Raft ↔ un `RecordBatch`**. El **índice de Raft es el ordinal de la entrada** (1-based, +1 por entrada, contiguo), **espacio distinto** del offset de partición (por record). `RaftLog` envuelve un `PartitionLog` (que almacena los bytes de cada entrada y asigna sus offsets) y **posee el mapeo** `índice → (term, base_offset, last_offset, type)`.

El **término** (y el resto de metadatos por entrada) se persiste en un **sidecar** de registros de tamaño fijo (`raft-meta`, 25 B/entrada: `term:i64 | base_offset:i64 | last_offset:i64 | type:u8`), append-only y truncable; así el log de partición conserva `RecordBatch` **intactos** (el *fetch* del consumidor los lee sin desenvolver) y la recuperación del término no exige tocar el formato del batch. El *high-watermark* visible (espacio de offsets) = `last_offset` de la entrada en `commit_index`. La resolución de conflictos (`truncate_from(index)`) → `PartitionLog::truncate_to(base_offset(index))` (frontera de batch, ADR/C3) + truncado del sidecar.

## Consecuencias

- (+) `RecordBatch` y `nexus-storage` quedan **sin cambios** (formato y CRC estables); el *fetch* sirve batches tal cual.
- (+) El algoritmo de Raft opera sobre índices contiguos +1 (formulación estándar del *paper*; sin aritmética de offsets).
- (+) El sidecar de tamaño fijo hace `term_at`/`last_term`/recuperación O(1)/lineal triviales.
- (−) Dos espacios de coordenadas (índice de entrada vs offset de record) que la capa de partición reconcilia (el *high-watermark* traduce de uno a otro).
- (−) Un fichero sidecar por partición además del `.log`/`.index`.
- (−) El mapeo se persiste por duplicado parcial (`base`/`last` son derivables del log escaneándolo), a cambio de un `open` simple sin re-escanear el log.

## Alternativas consideradas

- **Añadir `leader_epoch`/term a la cabecera del `RecordBatch`** (estilo Kafka v2): fiel a «el log es el log», pero cambia el formato en disco y el CRC, e impacta todo `nexus-storage` y sus tests; descartado para no reabrir un formato ya estabilizado en Fase 1.
- **Índice = offset+1 con entradas de un solo record:** iguala ambos espacios, pero elimina el *record batch* como unidad (contradice §5.4 y el modelo Kafka); descartado.
- **Anidar el `RecordBatch` del cliente dentro de un marco Raft** (term/type + payload como records del batch del log): término durable sin sidecar, pero cambia la semántica del offset a *por batch* (rompe el modelo por record de la Fase 1b) y obliga al *fetch* a desenvolver; descartado.
