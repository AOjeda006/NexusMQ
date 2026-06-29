# Diagrama 15: Planos de red (cliente vs inter-nodo Raft)

NexusMQ separa el **plano de cliente** (produce/fetch contra el líder, sobre el framing de `nexus-wire`) del **plano inter-nodo de Raft** (`AppendEntries`/`RequestVote`/`InstallSnapshot`), que viaja por su **propio puerto y conexiones persistentes** (ADR-0025). El plano inter-nodo usa el sobre `RaftEnvelope` (`topic | partition | from | to | type:u8 | payload`) con prefijo de longitud sobre TCP, sin `FrameHeader`/`ApiKey`, para mantener ambos protocolos ortogonales.

```mermaid
graph LR
    C["Cliente (productor / consumidor)"]

    subgraph N1["Nodo 1 (lider de la particion)"]
        L1["Puerto cliente<br/>(nexus-wire: Produce/Fetch)"]
        RP1["ReplicatedPartition + RaftNode"]
        R1["Puerto inter-nodo Raft<br/>(RaftEnvelopeReader/Writer)"]
        L1 --> RP1
        RP1 <--> R1
    end

    subgraph N2["Nodo 2 (seguidor)"]
        R2["Puerto inter-nodo Raft"]
        RP2["ReplicatedPartition + RaftNode"]
        R2 --> RP2
    end

    subgraph N3["Nodo 3 (seguidor)"]
        R3["Puerto inter-nodo Raft"]
        RP3["ReplicatedPartition + RaftNode"]
        R3 --> RP3
    end

    C -->|"Plano cliente (produce/fetch al lider)"| L1

    R1 ==>|"AppendEntries / RequestVote / InstallSnapshot<br/>(RaftTransport, conexiones persistentes)"| R2
    R1 ==>|"sobre: topic|partition|from|to|type|payload"| R3
    R2 -.->|"replies (fire-and-forget, reply diferido)"| R1
    R3 -.->|"replies"| R1
```

> El `RaftTransport` (sumidero `RaftMessageSink`) mantiene una conexión TCP persistente por peer, resolviendo `NodeId` -> `PeerAddress` (host + **puerto inter-nodo**) vía `PeerDirectory`. Es *best-effort*: Raft reenvía en el próximo `tick`. A diferencia del `call_on` cross-core (local y síncrono), un RPC a un nodo remoto **no vuelve** al completarse: su respuesta llega después como **otro** sobre (notificación asíncrona, ADR-0025). El `RaftCarrier` vive en el reactor dueño de la partición (`core = partition % cores`), respetando *shared-nothing*.
