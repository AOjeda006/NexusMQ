# Diagrama 10: Estados y transiciones de una réplica Raft

Ciclo de vida del rol de un `RaftNode` por partición (`Follower` ⇄ `PreCandidate` ⇄ `Candidate` ⇄ `Leader`), con la fase de *pre-vote* (§9.6) y la transferencia ordenada de liderazgo (§3.10) tal como las implementa `src/consensus/raft_node.hpp` (ADR-0003/0015).

```mermaid
stateDiagram-v2
    [*] --> Follower : arranque (RaftNode)

    Follower --> PreCandidate : election timeout sin contacto del lider
    note right of PreCandidate
        Sondea RequestVote con pre_vote=true.
        No sube el termino ni vota (anti-disrupcion 9.6)
    end note

    PreCandidate --> Candidate : mayoria de pre-votos concedidos
    PreCandidate --> Follower : termino mayor observado / sin mayoria

    Candidate --> Candidate : nuevo election timeout (+term, reintenta)
    note right of Candidate
        advance_term: +1 al termino, se vota a si mismo,
        difunde RequestVote (pre_vote=false)
    end note

    Candidate --> Leader : mayoria de votos del termino (has_majority)
    Candidate --> Follower : termino mayor observado / AppendEntries de lider valido

    Leader --> Leader : heartbeat_interval (AppendEntries vacio a los peers)
    note right of Leader
        Sirve produce/fetch, propone entradas y
        avanza commit_index por mayoria del termino actual
    end note

    Leader --> Follower : termino mayor observado (cede liderazgo)

    Leader --> Follower : transfer_leadership: envia TimeoutNow al destino al dia
    Follower --> Candidate : on_timeout_now (eleccion real inmediata, sin pre-vote)
```

> Regla transversal (cualquier rol): observar un `term` mayor en cualquier RPC degrada el nodo a `Follower` y adopta ese término (`become_follower`). Un `learner` (§4.2.1) nunca se postula: replica el log sin votar ni contar para el quórum.
