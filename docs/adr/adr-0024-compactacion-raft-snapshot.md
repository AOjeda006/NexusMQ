# ADR-0024: Compactación del log de Raft por **snapshot** (base de snapshot en `RaftLog`)

- **Estado:** aceptado
- **Fecha:** 2026-06-20

## Contexto

Bajo Raft por partición (ADR-0003), el **log replicado es el WAL** (glosario): crece sin techo aunque sus entradas más antiguas ya estén *committed* y aplicadas. Sin compactación, (1) el log de cada partición crece indefinidamente y (2) un seguidor que se reincorpora muy rezagado obligaría al líder a re-replicar desde el índice 1. El paper de Raft resuelve ambos con **snapshots** (§7): se toma una imagen del estado hasta `last_included_index`, se descarta el prefijo del log cubierto por ella y un seguidor demasiado atrás se pone al día con `InstallSnapshot` en vez de `AppendEntries`. El RPC `InstallSnapshot` y el tipo `Snapshot` ya existían (ADR-0014/Fase 2), pero `RaftLog` era **denso desde el índice 1** (`índice i ↔ entries_[i-1]`), sin forma de descartar un prefijo ni de recuperar `term_at(last_included_index)` tras hacerlo.

## Decisión

Dotar a `RaftLog` de una **base de snapshot** `(last_included_index, last_included_term, last_included_offset)` y compactar contra ella en tres planos coherentes:

1. **Modelo lógico (exacto).** `RaftLog` recuerda `snapshot_index_/snapshot_term_`; el índice `i` vive en `entries_[i − snapshot_index_ − 1]`, `last_index() = snapshot_index_ + entries_.size()`, `term_at(snapshot_index_) = snapshot_term_` y `term_at(i < snapshot_index_)` es `Compacted`. `compact_to(index)` descarta el prefijo aplicado **exactamente** en `index` (el llamante garantiza `index ≤ commit_index`).
2. **Persistencia.** La base de snapshot se guarda en un **sidecar dedicado** `raft-snapshot` (un único registro de tamaño fijo con **CRC32C**, mismo patrón que `RaftStateStore` de D1), separado del sidecar de metadatos por entrada (`raft-meta`). Al abrir, `RaftLog::open` lee la base y reconstruye el espacio de índices; la comprobación de coherencia con el `PartitionLog` se generaliza para admitir un `log_start_offset` ya recortado.
3. **Reclamación física (best-effort).** `PartitionLog::truncate_prefix_to(offset)` borra los segmentos **sellados enteros** cuyo rango queda por debajo de `offset` (nunca el activo, nunca a media trama), avanzando `log_start_offset`. La compactación lógica es exacta en el índice; la física libera disco por segmentos completos (puede quedar algo por encima de `log_start_offset` hasta que el segmento rote). Es la pieza simétrica de `enforce_retention`, pero recortando a un **offset preciso**.

La **puesta al día de un seguidor** muy rezagado (cuyo `next_index ≤ snapshot_index_` del líder) se hace con `InstallSnapshot`: el seguidor adopta la base de snapshot del líder y reanuda la replicación normal desde ahí. Coherente con ADR-0015 (FSM sin E/S): instalar un snapshot es una transición síncrona que reposiciona la base del log; el portador hace la E/S.

**Alcance / FSM del broker.** En NexusMQ la "máquina de estados" aplicada **es el propio log de particiones** (los `RecordBatch`); el `state` del `Snapshot` no es una materialización aparte, sino la **base `(index, term, offset)`** que mantiene consistente el espacio de offsets entre réplicas. La visibilidad de los **datos** antiguos para consumidores la gobierna la **retención** (ADR de storage), ortogonal a la compactación del WAL de Raft.

## Consecuencias

- (+) El log de Raft deja de crecer sin techo: una réplica acota su tamaño compactando el prefijo aplicado, y la base sobrevive a reinicios.
- (+) Reaprovecha el patrón de durabilidad de D1 (registro fijo + CRC32C + `fsync`) y la maquinaria de borrado de segmentos del `PartitionLog`.
- (+) El RPC `InstallSnapshot` ya existente encuentra por fin un modelo donde encajar.
- (−) La reclamación física es por segmentos enteros (no exacta al byte): aceptable y estándar.
- (−) La puesta al día por `InstallSnapshot` en el seguidor exige reposicionar el `PartitionLog` a una base vacía: se implementa de forma incremental (D2b) y la compactación **no se dispara automáticamente** hasta cablearla con seguridad en el servidor vivo (D3), de modo que el hueco queda latente, no activo.

## Alternativas consideradas

- **No compactar (retención del WAL como único freno):** la retención borra datos antiguos, pero no resuelve el espacio de índices de Raft ni la puesta al día de un seguidor rezagado por debajo del punto de borrado. Insuficiente. Descartada.
- **Snapshot como materialización separada del estado (fichero de imagen):** clásico del paper, pero en un broker el estado aplicado es el propio log; duplicarlo en una imagen aparte sería redundante y costoso. Se adopta la base `(index, term, offset)` + datos en el `PartitionLog`. Descartada.
- **Recorte físico exacto al byte (partir segmentos):** complicaría el `PartitionLog` (segmentos inmutables una vez sellados) por una ganancia marginal. Se recorta por segmentos enteros. Descartada.
