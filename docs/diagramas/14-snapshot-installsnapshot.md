# Diagrama 14: Compactación por snapshot e `InstallSnapshot`

El log replicado es el WAL y crece sin techo; la compactación por *snapshot* (ADR-0024) fija una **base** `(last_included_index, last_included_term, last_included_offset)`, descarta el prefijo aplicado y permite poner al día a un seguidor demasiado rezagado con `InstallSnapshot` en vez de re-replicar desde el índice 1 (§7 del paper). En NexusMQ el "estado" del snapshot **es la propia base** (índice/término/offset): los datos viven en el `PartitionLog`.

```mermaid
sequenceDiagram
    participant Pol as Politica (RaftCarrier)
    participant L as Lider (RaftLog)
    participant F as Seguidor rezagado

    Note over Pol: commit_index - snapshot_index >=<br/>applied_entries_threshold
    Pol->>L: compact_to(commit_index)
    Note over L: Fija base (last_included_index, term, last_offset).<br/>Descarta prefijo del sidecar (exacto en el indice).<br/>truncate_prefix_to (best-effort por segmentos).<br/>Persiste base en sidecar raft-snapshot (CRC32C + fsync)

    Note over L,F: El seguidor va tan atras que su next_index <= snapshot_index del lider

    L->>F: InstallSnapshot(term, leader_id, Snapshot{last_included_index/term/offset})
    Note over F: on_install_snapshot: rechaza lider obsoleto,<br/>con term >= propio reconoce al lider
    F->>F: RaftLog::install_snapshot(index, term, last_offset)
    Note over F: Adopta la base: si ya tiene (index, term) compacta hasta ella,<br/>si no, descarta el log y reabre PartitionLog en last_offset+1.<br/>Avanza commit_index hasta el indice incluido. Persiste (fsync)
    F-->>L: InstallSnapshotReply(term)

    Note over L: on_install_snapshot_reply: fija match_index/next_index<br/>del seguidor en el indice del snapshot
    L->>F: AppendEntries (reanuda replicacion normal desde la base)
```

> Coherente con ADR-0015 (FSM sin E/S): instalar un snapshot es una transición síncrona que reposiciona la base del log; la E/S durable la hace el `RaftLog`/portador, no el `RaftNode`. La compactación es *best-effort*: un fallo de E/S deja el log intacto y se reintenta en el próximo `tick`.
