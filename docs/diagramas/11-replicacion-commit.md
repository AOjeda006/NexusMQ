# Diagrama 11: Replicación y confirmación a quórum (`acks=quorum`)

Camino de escritura con Raft por partición: el productor envía al líder, este añade la entrada al log, la replica con `AppendEntries`, y cuando una mayoría confirma avanza `commit_index` (= *high-watermark*) y responde (§7.8, ADR-0003/0014). Una entrada de Raft es un `RecordBatch`; `ReplicatedPartition::produce` propone y la escritura es durable cuando `high_watermark()` la supera.

```mermaid
sequenceDiagram
    participant P as Productor
    participant L as Lider (RaftNode)
    participant F1 as Seguidor1
    participant F2 as Seguidor2

    P->>L: Produce(topic, partition, batch, acks=quorum)
    Note over L: produce(): valida idempotencia (5.9)<br/>y propone la entrada
    L->>L: RaftLog::append(entry) -> Index (term actual)

    par Replicacion a los peers
        L->>F1: AppendEntries(prev_log_index/term, entries, leader_commit)
        L->>F2: AppendEntries(prev_log_index/term, entries, leader_commit)
    end

    Note over F1,F2: log matching: comprueba (prev_log_index, prev_log_term),<br/>trunca conflicto y anexa
    F1-->>L: AppendEntriesReply(success=true)
    F2-->>L: AppendEntriesReply(success=true)

    Note over L: on_append_entries_reply: actualiza match_index/next_index.<br/>advance_commit_index() -> mayoria del termino actual
    L->>L: commit_index avanza (= high-watermark)

    L-->>P: Ack(baseOffset)  // offset del commit_index, visible a consumidores
```

> El `commit_index` **es** la *high-watermark*: el offset de partición del `last_offset` de la entrada en `commit_index` (ADR-0014). `acks=0` responde sin esperar; `acks=1`, tras el *append* local del líder; `acks=quorum` (por defecto), tras el *commit* de Raft. Solo se cuenta en el quórum a los miembros votantes (los `learner` replican pero no cuentan).
