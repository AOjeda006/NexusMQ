# 18. Catálogo por subsistema

> Una ficha por librería `nexus-*`: su responsabilidad, sus tipos clave, su **afinidad** de
> concurrencia y sus invariantes principales. El nivel es "tipos clave + invariantes", no
> clase-a-clase: el detalle exhaustivo (cada campo y firma) vive en el código fuente de cada
> subsistema (`src/`). La afinidad sigue la anotación del proyecto:
> **REACTOR-LOCAL** (de un reactor, no *thread-safe*) · **INMUTABLE** (solo lectura tras
> construir) · **CROSS-CORE** (solo se comunica por paso de mensajes) · **THREAD-SAFE**.

## nexus-common
**Responsabilidad:** tipos y utilidades transversales sin dependencias internas.
**Tipos clave:** `Bytes`/`span` (vistas no propietarias), `Error` y `expected<T>`, `Record`
y `RecordCodec`, `crc32c`, `varint`/zigzag, `base64`, `sha256`, `fnv1a`, `compression`
(LZ4/Zstd opcional), `task<T>` (corutinas), `move_only_function`.
**Afinidad:** mayormente **INMUTABLE**/valor puro; `task<T>` es REACTOR-LOCAL en uso.
**Invariantes:** `crc32c` coincide con el hardware (SSE4.2) y el *fallback* software; el
codec de `Record` es *round-trip* (`decode(encode(x)) == x`).

## nexus-io
**Responsabilidad:** I/O asíncrona por *completions* (el proactor) y recursos del SO.
**Tipos clave:** `Proactor` (puerto), `IoUringBackend`, `IocpBackend`, `File`, `Socket`,
`Listener`, `AlignedBuffer`, `BlockReader`, `awaitable`; tipos portables `NativeHandle`/`IoResult`.
**Afinidad:** **REACTOR-LOCAL** (un anillo io_uring por núcleo; sin compartición).
**Invariantes:** todo recurso del SO es RAII (se libera en el destructor); toda *completion*
comprueba su resultado (puede fallar como una syscall síncrona).

## nexus-wire
**Responsabilidad:** *framing* sobre conexión (separar el framing de la codificación pura).
**Tipos clave:** `FrameReader`/`FrameWriter` (cabecera INTERFACE, [ADR-0013](../adr/adr-0013-capa-nexus-wire.md)).
**Afinidad:** **REACTOR-LOCAL** (por conexión).
**Invariantes:** `nexus-protocol` queda **puro** (sin I/O); el framing no interpreta el cuerpo.

## nexus-protocol
**Responsabilidad:** protocolo binario nativo: codificación, frames, versionado y códigos de error.
**Tipos clave:** `FrameHeader` (`length:u32, apiKey:u16, apiVersion:u16, correlationId:u32,
flags:u16`), `Codec`, `messages`, `versioning`, `error_code` (`i16`).
**Afinidad:** **INMUTABLE**/sin estado (codec puro).
**Invariantes:** *round-trip* de serialización; los códigos de wire son **contrato** y se
traducen en el borde (ver [capítulo 16](./16-modelo-errores-wire-codes.md)).

## nexus-storage
**Responsabilidad:** motor de log append-only (Fase 1).
**Tipos clave:** `PartitionLog`, `Segment`, `Index` (`IndexEntry`), `LogConfig`, `Retention`,
`LogCompactor`, `FetchResult`.
**Afinidad:** **REACTOR-LOCAL** (pertenece al reactor dueño de la partición).
**Invariantes:** `logEndOffset` es monótono; la retención nunca borra el segmento activo;
cada `RecordBatch` está protegido por CRC32C verificado al leer/recuperar.

## nexus-reactor
**Responsabilidad:** modelo de ejecución *thread-per-core* y coordinación entre núcleos.
**Tipos clave:** `Reactor`, `ReactorPool`, `SpscQueue`, `MpmcQueue`, `CrossCore`/`CrossCoreCall`,
`PartitionRouter`, `Scheduler`, `Allocator` (por núcleo), `CacheLine`, `PeriodicTimers`.
**Afinidad:** `Reactor` **REACTOR-LOCAL**; las colas y `CrossCore`/`PartitionRouter` son el
puente **CROSS-CORE**.
**Invariantes:** un reactor por núcleo, *pinned*; nada bloqueante en el reactor; *head*/*tail*
de cada cola en líneas de caché distintas (`alignas`); SPSC con un único productor/consumidor.

## nexus-consensus
**Responsabilidad:** Raft como máquina de estados sin E/S.
**Tipos clave:** `RaftNode` (FSM), `RaftLog`, `RaftState`/`RaftStateStore`, `RaftRpc`,
`RaftWire`, `RaftCarrier`, `*MessageSink` (deferred/null).
**Afinidad:** **REACTOR-LOCAL** (la FSM la conduce el portador del reactor dueño).
**Invariantes:** la FSM no hace I/O (entradas→cola de salidas, `now` inyectado,
[ADR-0015](../adr/adr-0015-raftnode-fsm-sin-io.md)); el estado persistente se guarda **antes**
de enviar; el índice de entrada Raft es espacio distinto del offset por record
([ADR-0014](../adr/adr-0014-modelo-log-raft.md)).

## nexus-cluster
**Responsabilidad:** transporte inter-nodo de los RPC de Raft (plano separado).
**Tipos clave:** `RaftTransport`, `RaftLink`, `RaftReceiver`, `PeerDirectory`.
**Afinidad:** **CROSS-CORE**/borde de red (rutea por `(topic, partition)` al reactor dueño).
**Invariantes:** sobre de wire `topic|partition|from|to|type|payload` con longitud-prefijo;
plano separado del de cliente ([ADR-0025](../adr/adr-0025-activacion-raft-multireactor-transporte.md)).

## nexus-broker
**Responsabilidad:** dominio del broker: topics, particiones, grupos, offsets, enrutado.
**Tipos clave:** `Topic`, `Partition`, `PartitionBase`, `ReplicatedPartition`, `TopicCatalog`,
`TopicManager`, `TopicCluster`, `ConsumerGroup`, `GroupCatalog`, `GroupCoordinator`,
`OffsetManager`, `ProducerSession` (idempotencia), `CreditWindow` (backpressure), `RequestRouter`.
**Afinidad:** **REACTOR-LOCAL** por partición/grupo (cada uno tiene un único dueño;
ver [sharding](./07-concurrencia.md)).
**Invariantes:** una partición es la unidad de serialización (estilo *actor*); `leaderEpoch`
descarta líderes obsoletos; secuencia idempotente por `producerId` (duplicado/hueco).

## nexus-kafka
**Responsabilidad:** subconjunto Kafka-compatible (interop `kcat`).
**Tipos clave:** `Codec`, `messages`, `produce`, `fetch`, `metadata`, `list_offsets`,
`record_batch` (opaco), `gateway`, `error_code` (Kafka).
**Afinidad:** adaptador **asíncrono cross-core** sobre el broker vivo
([ADR-0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md)).
**Invariantes:** codec por versión (clásico vs flexible); el `RecordBatch` se almacena **opaco**;
los códigos se mapean a los de Kafka en el borde (ver [capítulo 14](./14-subconjunto-kafka.md)).

## nexus-ingress
**Responsabilidad:** capa de entrada (dos modos) y plano de control HTTP.
**Tipos clave:** `Proxy`, `UpstreamPool`, `Tls`, `RestGateway`, `AdminService`, `CircuitBreaker`,
`LoadBalancer`, `HealthMonitor`/`HealthCheck`, `RateLimiter`, `ProblemDetail`, `Jwt`, `Pagination`,
`Http`/`Json`, `ConnectionState`.
**Afinidad:** `UpstreamPool` **REACTOR-LOCAL** (pool por reactor); el resto, por conexión.
**Invariantes:** el modo nativo es el primario; TLS opcional con degradación a claro
([ADR-0019](../adr/adr-0019-tls-opcional-openssl-bios.md)); validación en el borde (*fail-fast*).

## nexus-telemetry
**Responsabilidad:** observabilidad: métricas, logs y *tracing*.
**Tipos clave:** `metrics` (exposición Prometheus), `logging` (JSON estructurado), `tracing`.
**Afinidad:** **THREAD-SAFE** en la agregación/exposición de métricas.
**Invariantes:** sin *logging* síncrono caro en el camino caliente; las métricas cubren las
cuatro señales de oro (ver [capítulo 12](./12-observabilidad.md)).

## nexus-server
**Responsabilidad:** *composition root* de `nexusd`; ensambla todo y enruta peticiones.
**Tipos clave:** `Server`, `Connection`, `AdminApi`/`AdminHttp`/`AdminRouter`, `KafkaAdapter`,
`KafkaConnection`, `main`.
**Afinidad:** monta el `ReactorPool`; el `RequestRouter` enruta async al reactor dueño.
**Invariantes:** el cableado de dependencias vive aquí, no en el dominio (ver
[capítulo 19](./19-arranque-y-composition-root.md)).

## nexus-client
**Responsabilidad:** librería cliente nativa en C++.
**Tipos clave:** `Client`, `Producer`, `Consumer`, `DeadLetter`, `Endpoint`.
**Afinidad:** usada por aplicaciones externas; el *smart-client* va directo al líder vía *metadata*.
**Invariantes:** reacciona a `NOT_LEADER_FOR_PARTITION` redescubriendo el líder; respeta los
créditos de *backpressure*.

## nexus-ffi
**Responsabilidad:** frontera ABI C estable para *bindings* (Python vía `ctypes`).
**Tipos clave:** `nexus_ffi.h` (API C), `nexus_ffi.cpp` (implementación).
**Afinidad:** **frontera**; traduce entre la ABI C y el cliente C++.
**Invariantes:** ABI C estable (no C++), por lo que el binding no necesita `python3-dev` ni
recompilación ([ADR-0020](../adr/adr-0020-binding-python-abi-c.md)).
