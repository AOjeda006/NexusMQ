# ADR-0033: Exactly-once multi-partición nativo (transacciones, coordinador y `read_committed`)

- **Estado:** aceptado
- **Fecha:** 2026-07-12

> **Feature nueva, opcional y con degradación limpia** (patrón de [ADR-0031](adr-0031-cifrado-en-reposo-aes-gcm.md) y [ADR-0032](adr-0032-tiered-storage-puerto-y-tier-local.md)): sin usar la API transaccional, el broker se comporta byte a byte como hasta ahora. Cierra el punto «exactly-once transaccional entre particiones» del [capítulo 30](../tecnica/30-limitaciones-y-trabajo-futuro.md) (trabajo futuro). El 2PC **recuperable** que lo sostiene se detalla en [ADR-0034](adr-0034-2pc-logueado-recuperable.md).

## Contexto

El **productor idempotente** (`ProducerSession`, §5.9) ya da *effectively-once* **por partición**: `producer_id` + época de *fencing* + dedup por secuencia descartan reintentos. Falta la pieza que hace visible **atómicamente** un conjunto de escrituras que abarca **varias particiones**: o se ven todas (commit) o ninguna (abort), sin estados intermedios observables. Es el modelo de transacciones de Kafka. El reto: darlo **solo en la superficie nativa** (no en el subset Kafka, [ADR-0030](adr-0030-particion-mono-protocolo.md)), reutilizando la idempotencia existente, sin acoplarlo al resto del motor y con **terminología honesta**: es *effectively-once* end-to-end (deduplicación + visibilidad atómica), **no** una entrega *exactly-once* literal (imposible en el caso general con fallos de red).

## Decisión

Un modelo transaccional **opt-in** con cuatro piezas coordinadas, todas deterministas y probadas por una simulación:

1. **Control records (marcadores COMMIT/ABORT).** Dos *flags* en `attrs` del `RecordBatch` —**transaccional** y **control**— disjuntos de los bits de códec. Un batch de **control** no lleva datos de usuario sino un único **marcador** `EndTxnMarker` (`type` COMMIT/ABORT, `coordinator_epoch`, `version`), con codec de clave/valor versionado y decodificador defensivo (`common/control_record`). El marcador delimita el fin de la transacción en el log de cada partición participante.

2. **Coordinador de transacciones como FSM sin E/S, con su propio Raft.** `TransactionCoordinator` (`broker/transaction_coordinator`) es una máquina de estados síncrona **sin E/S** (patrón `RaftNode`/`GroupCoordinator`, [ADR-0015](adr-0015-raftnode-fsm-sin-io.md)): consume `init_producer_id`/`begin`/`add_partitions`/`commit`/`abort`/`on_marker_written`/`tick` con el `now` inyectado y produce **órdenes de marcador** en una cola que el portador transporta. Su estado (metadatos de cada transacción: estado, participantes, época) es **replicable por su propio grupo Raft**. `init_producer_id` (InitProducerId/`initTransactions`) asigna la **identidad** del productor (`producer_id` + época) y rota la época al reiniciar.

3. **LSO por partición + nivel de aislamiento.** `PartitionTxnIndex` mantiene, por partición, el **Last Stable Offset** = `min(high_watermark, primer offset de la transacción abierta más antigua)` y el registro de las transacciones **abortadas**. Un consumidor `read_committed` no ve más allá del LSO (los datos posteriores podrían pertenecer a una transacción sin decidir) y **filtra** los records de transacciones abortadas y los marcadores de control (`filter_committed`); `read_uncommitted` ve todo hasta el *high-watermark*, como hasta ahora.

4. **Fencing por época en las dos direcciones.** El **productor** arrastra su época; una encarnación anterior (zombi) queda expulsada (`Fenced`) por `init_producer_id`, que además **aborta** su transacción en curso para no bloquear el LSO. El **coordinador** sella cada marcador con su `coordinator_epoch`; una partición **descarta** los marcadores de un coordinador obsoleto (fencing en el failover). Reutiliza la misma semántica de época que `ProducerSession` (§5.9).

La API nativa —`initTransactions`/`beginTransaction`/`addPartitionsToTxn`/`commit`/`abort`— se asienta sobre estas piezas. El camino **no transaccional** (sin los *flags* en `attrs`) es byte-idéntico al de hoy.

## Consecuencias

- (+) **Visibilidad atómica multi-partición:** un commit hace visibles todos los records de la transacción a los consumidores `read_committed`; un abort, ninguno. Verificado por una **simulación determinista** (300 transacciones con commit/abort aleatorios, entregas parciales y failover del coordinador: al final se ve exactamente el conjunto confirmado).
- (+) **Degradación limpia:** sin API transaccional, el prefijo transaccional está vacío, el LSO == *high-watermark* y todos los caminos nuevos son *no-ops*.
- (+) Reutiliza la idempotencia (`ProducerSession`) y el patrón FSM-sin-E/S; el índice, la retención y Raft de partición no cambian. Interopera con el cifrado (ADR-0031) y el tiering (ADR-0032): los marcadores son records normales del log.
- (−) **Solo superficie nativa:** el subset Kafka no expone transacciones (coherente con ADR-0030). Documentado como límite.
- (−) Es *effectively-once* **honesto**, no *exactly-once* literal: la deduplicación y la visibilidad atómica evitan duplicados y estados parciales, pero la entrega end-to-end exacta no es alcanzable ante fallos arbitrarios; la terminología lo refleja.
- (−) El LSO puede **retenerse** mientras una transacción siga abierta: un productor que abre y no cierra frena a los `read_committed` de esa partición hasta el `tick` de timeout (abort del servidor).

## Alternativas consideradas

- **Exactly-once literal de entrega:** imposible en el caso general (el problema de los dos generales); se elige *effectively-once* honesto (dedup + atomicidad de visibilidad), que es lo que ofrecen los sistemas reales.
- **Marcador por record en vez de por batch/transacción:** multiplicaría el overhead y rompería la unidad de escritura (el batch, §9.3). El marcador por transacción, en un batch de control propio, es el modelo de Kafka y encaja con el log existente.
- **Filtrado solo en el cliente (sin LSO en el servidor):** dejaría ver datos no confirmados hasta que el cliente los descartara; el LSO en el servidor es la frontera correcta de visibilidad y evita exponer estados intermedios.
- **Transacciones también en el subset Kafka:** fuera de alcance (ADR-0030, partición mono-protocolo); la superficie nativa es suficiente para demostrar el modelo.
- **Coordinador con estado en memoria no replicado:** un fallo perdería transacciones en vuelo; el coordinador con **Raft propio** y 2PC **logueado** (ADR-0034) lo hace recuperable.
