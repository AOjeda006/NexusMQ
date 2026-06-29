# ADR-0025: Activación en producción del stack Raft + multi-reactor con transporte inter-nodo real

- **Estado:** aceptado
- **Fecha:** 2026-06-21

## Contexto

Las fases previas dejaron listas, pero **sin enchufar al servidor vivo**, varias piezas: el log y la FSM de Raft (ADR-0014/0015), la `ReplicatedPartition` (ADR-0016), el *routing* cross-core (`PartitionRouter`/`call_on`), el `ReactorPool` *thread-per-core* (ADR-0005) y la persistencia (D1) y compactación (D2) del estado de Raft.

El binario `nexusd` corre hoy con **un único `Reactor`** y `Partition` simples (*ack* local); los RPC de Raft solo circulan por la **red virtual** de los tests (simulación determinista); nadie persiste el estado de Raft ni dispara la compactación en producción. D3 cierra esa deuda diferida por *fasing*: **activar** el stack en `nexusd` con **transporte real** entre nodos. No reescribe nada; es integración más inyección de un transporte de red.

## Decisión

Cuatro piezas coherentes, implementadas de forma incremental (D3.1..D3.7), siempre verde:

1. **Plano inter-nodo separado del plano de cliente.** Los RPC de Raft viajan por su **propio plano** (su listener, sus conexiones persistentes entre nodos), distinto del plano de datos del cliente (ADR-0004/0013). Un **sobre de wire de Raft** rutea cada `RaftMessage` y el sobre identifica la **réplica de partición destino** `(topic, partition)` para entregar el RPC al `RaftNode` correcto del nodo receptor.

2. **Portador por partición, afinado al reactor dueño.** Cada réplica la conduce un **portador** (`RaftCarrier`) que vive en el reactor dueño de la partición (`PartitionRouter`: `core = partition % cores`): un `tick` periódico **local** avanza la FSM, drena `take_messages()` y entrega cada mensaje al transporte; los RPC entrantes se enrutan al reactor dueño (`call_on`) y se aplican con `on_*`. El camino del `tick` es local al reactor de la partición (sin cross-core), respetando shared-nothing: no hay estado de Raft compartido entre núcleos.

3. **Persistencia y compactación en el lazo del portador.** Al abrir la réplica se carga `RaftStateStore::load()` → `restore_persistent_state` (D1); **antes** de transportar `take_messages()`, si `persistent_state_dirty()`, se hace `save()` con `fsync` (regla §5 de Raft: persistir término/voto antes de responder al RPC). La compactación (`compact_to`, D2) se dispara por **política** (umbral de entradas aplicadas) desde el portador, ya con seguridad.

4. **Multi-reactor en el `Server`.** El `Server` pasa de un `Reactor` único a un `ReactorPool` (uno por núcleo, *pinned*); el listener acepta en el reactor 0 y el `RequestRouter` enruta cada petición a la partición por su reactor dueño (`call_on`), volviéndose **asíncrono** (`task<expected<...>>`). `ReplicatedPartition` (ADR-0016) sustituye a `Partition` cuando `replication_factor > 1`; con factor 1 (mono-nodo, desarrollo) sigue `Partition`.

### Sobre de wire de Raft

El formato del sobre es `topic | partition | from | to | type:u8 | payload`, con prefijo de longitud para *streaming* sobre TCP. El campo `type` es el discriminante del `variant` del RPC y el `payload` reutiliza el `encode`/`decode` por RPC ya existente (ADR-0014). De este modo el contrato de wire de Raft reaprovecha los codecs de los RPC sin duplicarlos.

## Consecuencias

- (+) D1/D2 dejan de ser mecanismo latente: el consenso se replica de verdad por la red y sobrevive a reinicios; la compactación acota el log en el servidor vivo.
- (+) El plano inter-nodo separado evita mezclar el contrato de cliente con el de réplica: evolucionan por separado (ADR-0013) y tienen SLAs distintos.
- (+) El portador por reactor respeta shared-nothing (ADR-0005).
- (+) La red virtual determinista se **conserva** para probar la lógica de la FSM; el transporte real se prueba aparte (sockets de loopback más inyección de caos).
- (−) Aparece un segundo listener/puerto (inter-nodo) y direccionamiento de peers por configuración.
- (−) El `RequestRouter` cambia de firma a asíncrono y el camino de despacho se refactoriza; se hace incremental para no romper el árbol.
- (−) El transporte real introduce timeouts/reintentos/reordenamiento que la red virtual no tenía: se asumen como parte del plano de red (`fundamentos/redes/`).

## Alternativas consideradas

- **Reutilizar el plano de cliente para los RPC de Raft (un puerto, `ApiKey`s nuevas):** acopla el contrato de cliente al de réplica, mezcla flujos con SLAs distintos y complica el versionado. Se separa el plano. Descartada.
- **Portador global único (un hilo conduce todas las particiones):** concentra el trabajo, rompe shared-nothing y serializa el consenso de particiones independientes. Se hace por partición/reactor. Descartada.
- **Persistir el estado de Raft en cada `tick` (no solo si *dirty*):** `fsync` innecesarios en el camino caliente. Se persiste solo ante cambio (`dirty`), antes de enviar. Descartada.
- **`co_await` cross-core para el *reply* del RPC (estilo `call_on`):** un RPC a un nodo remoto **no vuelve** al completarse; su respuesta entra después como **otro** RPC (notificación asíncrona). El transporte inter-nodo es *fire-and-forget* con *reply* diferido, no petición/respuesta acoplada (a diferencia del `call_on` cross-core, que sí es local y síncrono entre reactores). Descartada para el plano de red.
