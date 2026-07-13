# Diagrama 25: Transacciones y 2PC logueado (exactly-once nativo)

El *exactly-once* multi-partición (ADR-0033 / ADR-0034) se apoya en un `TransactionCoordinator`: una **máquina de estados sin E/S** (mismo patrón que `RaftNode` y `GroupCoordinator`) que conduce un *two-phase commit* **logueado y recuperable, no bloqueante**. El coordinador **registra la decisión** (`PrepareCommit`/`PrepareAbort`) en su propio grupo Raft **antes** de escribir ningún marcador; el portador transporta las **órdenes de marcador** a cada partición participante y las acusa. Este diagrama detalla la FSM de una transacción, el flujo feliz del 2PC, la recuperación ante *failover* del coordinador y la visibilidad `read_committed` por LSO.

> Fuentes: `src/broker/transaction_coordinator.{hpp,cpp}` (`TransactionCoordinator`, `TransactionState`, `MarkerWrite`, `ProducerIdentity`), `src/broker/partition_txn_index.{hpp,cpp}` (`PartitionTxnIndex`, `IsolationLevel`, `AbortedTxn`), `src/common/control_record.{hpp,cpp}` (`EndTxnMarker`, `build_control_batch`). Diseño: [capítulo 10 §10.8](../tecnica/10-replicacion-y-consenso.md), [capítulo 9 §9.8](../tecnica/09-almacenamiento.md), [ADR-0033](../adr/adr-0033-exactly-once-nativo-transacciones.md), [ADR-0034](../adr/adr-0034-2pc-logueado-recuperable.md). Contrato de wire: [`../protocol.md`](../protocol.md#transacciones-y-marcadores-de-control-exactly-once-nativo).

## 1. FSM de una transacción en el coordinador

Una transacción vive el ciclo `Ongoing` → `Prepare*` (decisión registrada) → `Complete*` (todos los marcadores acusados). El registro de la decisión **antes** de escribir marcadores es lo que hace el 2PC recuperable: un coordinador que arranca y ve un estado `Prepare*` sabe que debe **re-emitir** los marcadores que falten (`resume_pending`), no dejar a los participantes bloqueados esperando.

```mermaid
stateDiagram-v2
    [*] --> Ongoing: begin(now, pid, epoch)
    Ongoing --> Ongoing: add_partitions (fusiona, sin duplicar)
    Ongoing --> PrepareCommit: commit()  ·  decisión registrada
    Ongoing --> PrepareAbort: abort()  ·  decisión registrada
    Ongoing --> PrepareAbort: tick() timeout / init_producer_id zombie

    PrepareCommit --> PrepareCommit: on_marker_written (quedan unacked)
    PrepareAbort --> PrepareAbort: on_marker_written (quedan unacked)

    PrepareCommit --> CompleteCommit: unacked_partitions vacío
    PrepareAbort --> CompleteAbort: unacked_partitions vacío

    CompleteCommit --> [*]
    CompleteAbort --> [*]
```

- **Sin participantes**, `commit`/`abort` concluye de inmediato (`Complete*`) sin emitir marcadores.
- **Fencing por época:** una época **inferior** a la autoritativa (`current_epoch_`) es `Fenced`; una **superior** (o un estado `Complete*`) inicia una transacción nueva, expulsando a la anterior. La época autoritativa **sobrevive** a que la transacción concluya, de modo que un zombi en `CompleteAbort` con época vieja no puede re-abrir.
- **`tick(now)`** aborta las transacciones `Ongoing` que superan el timeout (`kDefaultTxnTimeout`, 60 s), liberando el LSO.

## 2. Flujo feliz del 2PC (init → begin → produce → commit)

El productor obtiene su identidad con `init_producer_id`, abre la transacción, declara participantes y publica batches transaccionales; al confirmar, el coordinador registra `PrepareCommit` y **luego** encola un `MarkerWrite` por participante, que el portador escribe como **batch de control** y acusa.

```mermaid
sequenceDiagram
    participant P as Productor
    participant TC as TransactionCoordinator (FSM + Raft propio)
    participant CA as Portador (drena take_pending_markers)
    participant PA as Partición A
    participant PB as Partición B

    P->>TC: init_producer_id(now, "txn-id")
    TC-->>P: ProducerIdentity {producer_id, epoch}
    P->>TC: begin(now, pid, epoch)
    P->>TC: add_partitions(now, pid, epoch, {A, B})
    P->>PA: Produce(batch transaccional, pid, epoch)  [attrs.transactional]
    P->>PB: Produce(batch transaccional, pid, epoch)

    P->>TC: commit(now, pid, epoch)
    Note over TC: Ongoing → PrepareCommit<br/>decisión REGISTRADA en Raft propio<br/>antes de emitir marcadores
    TC-->>CA: take_pending_markers() → [MarkerWrite(A), MarkerWrite(B)]
    CA->>PA: escribe batch de control Commit (coordinator_epoch)
    CA->>PB: escribe batch de control Commit (coordinator_epoch)
    CA->>TC: on_marker_written(pid, epoch, A)
    CA->>TC: on_marker_written(pid, epoch, B)
    Note over TC: unacked vacío → CompleteCommit
```

- El batch de datos arrastra `producer_id` + época en la cabecera, reutilizando la **idempotencia** de `ProducerSession` (dedup por secuencia + fencing por época).
- El batch de control lleva `EndTxnMarker {type, coordinator_epoch, version}`; el `coordinator_epoch` **sella** el marcador para el fencing en el failover.
- **ABORT** es simétrico: `PrepareAbort` → marcadores `Abort` → `CompleteAbort`; los batches de datos de la transacción nunca se hacen visibles.

## 3. Recuperación ante *failover* del coordinador (2PC recuperable)

Si el coordinador cae **después** de registrar la decisión pero **antes** de que todos los marcadores se acusen, el nuevo líder reconstruye su estado desde el grupo Raft, adopta una **época mayor** y **re-conduce** la decisión ya registrada. No hay espera bloqueante: los marcadores re-emitidos llevan la nueva época y los antiguos quedan fencing-eados.

```mermaid
sequenceDiagram
    participant TC1 as Coordinador (época N) †
    participant R as Grupo Raft del coordinador
    participant TC2 as Coordinador (época N+1)
    participant PA as Partición A

    TC1->>R: registra PrepareCommit(pid) + participantes
    TC1--xPA: (cae antes de acusar todos los marcadores)
    Note over TC1,R: la decisión ya está DURABLE en Raft

    R-->>TC2: reconstruye estado (ve PrepareCommit a medias)
    TC2->>TC2: set_coordinator_epoch(N+1)
    TC2->>TC2: resume_pending() → re-encola marcadores faltantes
    TC2->>PA: batch de control Commit (coordinator_epoch = N+1)
    Note over PA: descarta cualquier marcador con época ≤ N<br/>(fencing del coordinador obsoleto)
    TC2->>TC2: on_marker_written → CompleteCommit
```

- La partición **descarta** (idempotente) marcadores con `coordinator_epoch` obsoleto: un marcador rezagado del líder viejo no puede deshacer ni duplicar la decisión del nuevo.
- Coherente con la postura **CP**: cerrar transacciones nuevas exige quórum en el grupo del coordinador; si no hay quórum, no se decide (no se diverge).

## 4. Visibilidad `read_committed` y LSO por partición

En lectura, `PartitionTxnIndex` mantiene el conjunto de transacciones abiertas y de rangos abortados. El **LSO** (*last stable offset*) es el mínimo *first-offset* de transacción abierta, acotado por el *high-watermark*; `read_committed` entrega solo hasta el LSO y **filtra** los records abortados y los propios marcadores de control.

```mermaid
graph LR
    subgraph log["Log de la partición (offsets crecientes →)"]
        D0["#0 batch txn X<br/>(commit)"]
        D1["#1 batch txn Y<br/>(abort)"]
        M0["#2 control Commit X"]
        M1["#3 control Abort Y"]
        D2["#4 batch txn Z<br/>(abierta)"]
        HWM["#5 high-watermark"]
    end
    D0 --> D1 --> M0 --> M1 --> D2 --> HWM

    LSO["LSO = first-offset de Z (#4)<br/>= min(txn abierta, HWM)"]:::lso
    LSO -. "read_committed corta aquí" .-> D2

    RU["read_uncommitted<br/>entrega hasta HWM"]:::ru
    RC["read_committed<br/>entrega X, oculta Y (abortada),<br/>oculta marcadores, corta en LSO"]:::rc

    classDef lso fill:#fde68a,stroke:#b45309,color:#000
    classDef ru fill:#e5e7eb,stroke:#6b7280,color:#000
    classDef rc fill:#bbf7d0,stroke:#15803d,color:#000
```

- **Atomicidad:** un `Commit` hace visible **toda** la data de la transacción; un `Abort`, **ninguna**; una transacción **abierta** retiene el LSO (nada suyo ni posterior es visible en `read_committed` hasta que cierre).
- `PartitionTxnIndex` se mantiene incrementalmente (`on_data`/`on_marker`/`evict_below`); el algoritmo `filter_committed` procesa los marcadores en línea para activar/desactivar cada rango abortado por `producer_id`. Estas invariantes están cubiertas por la **simulación determinista** (`tests/sim/transaction_sim.hpp`), incluido un caos de 300 transacciones con *failover*.
