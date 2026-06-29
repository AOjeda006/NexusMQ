# Diagrama 12: Failover y elección de líder

Cuando el líder cae, un seguidor agota su *election timeout* sin recibir *heartbeats*, se postula incrementando el término, difunde `RequestVote` y, al reunir mayoría, se convierte en líder y reanuda la replicación (§5.2, ADR-0003/0015). El diagrama omite la fase de *pre-vote* previa (§9.6) para centrarse en el *failover*.

```mermaid
sequenceDiagram
    participant L as Lider (caido)
    participant F1 as Seguidor1
    participant F2 as Seguidor2

    Note over L: El lider deja de emitir AppendEntries (heartbeat)

    Note over F1: tick(now): vence election_deadline<br/>sin contacto del lider
    F1->>F1: become_candidate: advance_term (+1),<br/>se vota a si mismo (record_vote)

    par RequestVote a los votantes
        F1->>F2: RequestVote(term, last_log_index, last_log_term)
        F1-xL: RequestVote (sin respuesta: caido)
    end

    Note over F2: on_request_vote: term >= propio y<br/>log del candidato al dia (log_is_up_to_date)
    F2-->>F1: RequestVoteReply(vote_granted=true)

    Note over F1: on_request_vote_reply: votes_granted alcanza mayoria<br/>(has_majority) -> become_leader
    F1->>F1: reset_leader_progress (next_index/match_index)

    F1->>F2: AppendEntries (heartbeat del nuevo lider, nuevo term)
    Note over F2: Reconoce al nuevo lider, rearma election timer
```

> El estado persistente (término y voto) se guarda con `fsync` **antes** de responder al RPC (regla §5 de Raft): lo gobierna `persistent_state_dirty()` y lo ejecuta el `RaftCarrier` antes de transportar. Ningún dato confirmado con `acks=quorum` se pierde mientras sobreviva la mayoría del grupo: el nuevo líder tiene, por la restricción de elección, todas las entradas *committed*.
