# ADR-0016: `ReplicatedPartition` como tipo paralelo a `Partition` (composición de la pila Raft)

- **Estado:** aceptado
- **Fecha:** 2026-06-15

## Contexto

Este ADR **refina el desglose** (no cambia ADR-0003/0015): el desglose detallado preveía **mutar** `Partition` para intercalar `RaftNode` y convertir `produce`/`fetch` en `task<expected<…>>`. La decisión mantiene `Partition` intacta y añade un tipo nuevo, sin alterar el algoritmo ni la frontera de E/S de ADR-0015.

En C9 hay que dar a la partición respaldo Raft (`acks=quorum`, *high-watermark* = `commit_index`, ADR-0003). La `Partition` de la Fase 1b funciona con *ack* local y sirve al broker e2e que ya pasa. Dos fricciones al mutarla in situ:

1. La pila de consenso es **autorreferencial** —`PartitionLog` ← `RaftLog`(ref) ← `RaftNode`(ref)—, así que un tipo con esos miembros por valor no sería **movible** sin invalidar referencias internas.
2. `RaftNode` es una **FSM sin E/S** (ADR-0015) que un portador externo debe conducir (`tick`/`on_*`/`take_messages`), contrato que aún no existe en el broker (llega con el *routing* multi-reactor, C11).

Mutar `Partition` ahora mezclaría ambos modelos (ack local vs quorum) en un tipo en uso y arriesgaría el camino e2e verde.

## Decisión

Se añade **`ReplicatedPartition`** (`src/broker/replicated_partition.{hpp,cpp}`) como **tipo paralelo** a `Partition`, sin tocar esta última. Compone la pila por **`unique_ptr`** (`PartitionLog` + `RaftLog` + `RaftNode`): las direcciones de heap son estables, por lo que las referencias internas siguen válidas y el objeto es **movible** (move por defecto, copia borrada).

`produce` (solo líder; `Unsupported` si no) aplica la idempotencia por productor (§5.9, reutilizada de `Partition`) y **propone** la entrada al `RaftNode`, traduciendo el índice de Raft asignado a su último offset de partición vía `RaftLog::offsets_at`; `high_watermark()` = offset (exclusivo) de la entrada en `commit_index` (0 ⇒ `log_start_offset`). La FSM **no se conduce sola**: `raft()` expone la superficie para que el portador (reactor o arnés) la dirija; en C9 se valida con **enrutado directo/simulado** (test con reloj y red virtuales que comprueba que una escritura no es visible hasta el quorum). El **cambio en caliente** del broker a `ReplicatedPartition` (con transporte real) se difiere a **C11**.

## Consecuencias

- (+) `Partition` y el broker e2e quedan **intactos** (sin riesgo en el camino verde); la unidad de partición replicada queda lista y probada aislada.
- (+) Tipo **movible** pese a la pila autorreferencial (los `unique_ptr` fijan las direcciones).
- (+) Reutiliza idempotencia y `PartitionLog`/`RaftLog` sin duplicar lógica.
- (+) `produce` síncrono (la FSM no hace E/S, ADR-0015); cuando llegue el reactor, el portador drena `take_messages()` y espera a `commit_index`.
- (−) **Dos** tipos de partición conviviendo hasta C11 (deuda temporal explícita).
- (−) El cliente de `ReplicatedPartition` debe conducir `raft()` (contrato externo, como en ADR-0015).

## Alternativas consideradas

- **Mutar `Partition` (desglose literal):** un solo tipo, pero rompe su movilidad por la pila autorreferencial, mezcla ack local/quorum en un tipo en uso y arriesga el e2e verde antes de tener el portador (C11); descartado.
- **`RaftNode` por valor dentro de la partición:** evita una indirección, pero el tipo deja de ser movible (referencias internas colgantes al mover) y complica almacenarlo en contenedores del broker; descartado a favor de los `unique_ptr`.
- **Fusionar ambos tras C11** (un tipo con modo local/quorum): posible evolución futura; hoy se prefiere separar para no acoplar la Fase 1b a la 2.
