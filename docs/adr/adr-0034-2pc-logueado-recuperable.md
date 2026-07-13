# ADR-0034: 2PC logueado y recuperable (reconciliación con la prohibición del 2PC bloqueante)

- **Estado:** aceptado
- **Fecha:** 2026-07-12

> Complementa a [ADR-0033](adr-0033-exactly-once-nativo-transacciones.md): detalla **cómo** se ejecuta el *two-phase commit* de una transacción multi-partición sin caer en el **2PC en memoria bloqueante** que la normativa de la biblioteca (`fundamentos/datos-distribuidos`) desaconseja.

## Contexto

El *two-phase commit* clásico tiene un defecto conocido: si el **coordinador cae** entre la fase de preparación y la de commit, los participantes quedan **bloqueados** esperando la decisión, indefinidamente. Por eso la normativa del proyecto prohíbe el «2PC en memoria bloqueante». Pero el commit atómico multi-partición de [ADR-0033](adr-0033-exactly-once-nativo-transacciones.md) **es** un 2PC (preparar la decisión → escribir marcadores en cada participante). Hay que reconciliar ambas cosas: dar atomicidad multi-partición **sin** que un fallo del coordinador blosquee a los participantes.

## Decisión

Un 2PC **logueado y recuperable**, no bloqueante, con cuatro reglas:

1. **La decisión se registra antes de actuar.** El coordinador pasa la transacción a `PrepareCommit`/`PrepareAbort` —y **persiste** esa transición en su **propio log de Raft**— **antes** de escribir ningún marcador en las particiones. La decisión es durable y está replicada por quórum en cuanto se toma.

2. **La recuperación re-conduce, no espera.** Un coordinador que arranca (o un nuevo líder tras un failover) **reconstruye** su estado desde el log de Raft. Si encuentra una transacción en `Prepare*`, **re-emite** los marcadores que falten (`resume_pending`): el 2PC se **reanuda**, no se queda esperando. Los participantes nunca dependen de que un coordinador concreto siga vivo; dependen de que el **grupo Raft** del coordinador tenga quórum (postura CP, [ADR-0007](adr-0007-consistencia-cp-pacelc.md)).

3. **Fencing del coordinador por época.** Cada marcador se sella con la `coordinator_epoch` (la época de liderazgo del coordinador que lo emite; sube en cada failover). Una partición **descarta** un marcador cuya época sea inferior a la última vista para ese productor: dos coordinadores solapados (viejo líder rezagado + nuevo líder) no pueden escribir decisiones contradictorias.

4. **Los marcadores son idempotentes; el timeout desbloquea al productor colgado.** Re-emitir un marcador ya aplicado es un *no-op* (la transacción ya está cerrada en esa partición). Una transacción que queda `Ongoing` demasiado tiempo (productor que abre y no cierra) se **aborta** por `tick` de timeout, liberando el LSO de sus participantes.

## Consecuencias

- (+) **No bloqueante:** la caída del coordinador no cuelga a los participantes; el nuevo líder re-conduce la decisión ya registrada. El límite es la disponibilidad del **quórum** del grupo Raft del coordinador, no la de un proceso.
- (+) **Atómico y recuperable:** la decisión (commit/abort) es durable antes de tener efectos visibles; el sistema converge al mismo resultado tras cualquier fallo a mitad, como valida la simulación determinista (failover a mitad del commit → se resuelve).
- (+) Reutiliza la infraestructura de Raft del proyecto (el coordinador es «otro grupo Raft»), sin un gestor de transacciones externo.
- (−) La escritura de marcadores es **at-least-once** (se re-emite ante la duda); la idempotencia en la partición lo absorbe.
- (−) Añade una dependencia de disponibilidad: sin quórum en el grupo del coordinador, no se pueden **cerrar** transacciones nuevas (coherente con la postura CP; las escrituras no transaccionales no se ven afectadas).

## Alternativas consideradas

- **2PC en memoria bloqueante:** el modelo prohibido; un fallo del coordinador bloquea a los participantes. Descartado por la normativa y por corrección.
- **3PC (three-phase commit):** elimina el bloqueo añadiendo una fase, pero a costa de más *round-trips* y de suposiciones de temporización; el 2PC **logueado sobre Raft** logra el mismo no-bloqueo apoyándose en el consenso que el proyecto ya tiene.
- **Gestor de transacciones externo (XA/servicio aparte):** un subsistema nuevo y una dependencia operativa; el coordinador como grupo Raft propio es coherente con la arquitectura *shared-nothing* existente.
- **Decidir primero y loguear después:** invertiría el orden y abriría una ventana en la que un fallo dejaría marcadores escritos sin decisión registrada (irrecuperable). Loguear **antes** de actuar es lo que da la recuperabilidad.
