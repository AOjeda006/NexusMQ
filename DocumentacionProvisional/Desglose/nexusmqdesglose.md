================================================================================
DESGLOSE DE LA SOLUCIÓN — NexusMQ (C++20)
================================================================================
**Archivo:** `nexusmq-desglose.md`
**Versión:** 0.1.0 (esqueleto de diseño derivado del anteproyecto v0.4.0)
**Fecha:** 2026-06-10
**Naturaleza:** Proyección de diseño (pre-implementación). Describe la estructura
**prevista** de la solución —no código ya escrito—. Cada clase/método es una
intención de diseño coherente con el anteproyecto; se concretará por fases.

================================================================================
0. CONVENCIONES DEL DESGLOSE
================================================================================
- **Una sola solución de Visual Studio = un único árbol CMake.** Los "proyectos"
  son *targets* CMake (`add_library`/`add_executable`). No hay `.sln` salvo que se
  genere con el *generator* de VS; con *Open Folder* + CMakePresets no hace falta.
- **Visibilidad:** `+` público · `#` protegido · `-` privado.
- **Afinidad de concurrencia** (anotada por tipo, base del diseño *shared-nothing*):
    [REACTOR-LOCAL] pertenece a un reactor; NO es thread-safe; cross-core solo por mensaje.
    [INMUTABLE]     solo lectura tras construir; compartible entre hilos sin lock.
    [CROSS-CORE]    diseñado para comunicación entre reactores (SPSC, alignas).
    [THREAD-SAFE]   seguro para uso concurrente (atomics/lock interno).
- **Naming** (conforme a la normativa C++): tipos `PascalCase`; funciones/variables/
  miembros `snake_case`; constantes `kPascalCase`; **sin** prefijo `I` en interfaces
  (clases abstractas con nombre normal); miembros privados con sufijo `_`.
- **RAII:** todo recurso (fd, socket, ring io_uring, lock, arena) lo posee un objeto
  que lo libera en su destructor.
- **Modelo de errores (ADR-0009):** núcleo/hot-path devuelve `expected<T>` (alias de
  `std::expected<T, Error>`); excepciones solo en plano de control; códigos de wire
  en el protocolo. Las corrutinas devuelven `task<expected<T>>`.
- **Ficheros:** cabecera `.hpp` (declaración + Doxygen) + `.cpp` (definición);
  `#pragma once`; identificadores en inglés, doc-comments en español.

================================================================================
1. ESTRUCTURA DE LA SOLUCIÓN (targets y áreas)
================================================================================
1 solución → 4 áreas → ~14 targets:

ÁREA            TARGET (tipo)              FASE   RESPONSABILIDAD
-------------   ------------------------   ----   ------------------------------------
Núcleo (libs)   nexus-common      (lib)    1      Tipos, bytes, CRC32C, error, config, log, reloj.
                nexus-io          (lib)    1/1b   Abstracción proactor (io_uring/IOCP), File, Socket.
                nexus-reactor     (lib)    1b     Reactor thread-per-core, scheduler de corrutinas, SPSC, allocator.
                nexus-storage     (lib)    1      Record/Batch, Segment, índice, PartitionLog, retención, recuperación.
                nexus-protocol    (lib)    1b     Framing, codec, mensajes, códigos de error, compresión, créditos.
                nexus-consensus   (lib)    2      Raft por partición (estado, RPC, log, elección).
                nexus-broker      (lib)    1b/2   Topics, particiones, grupos, offsets, idempotencia, backpressure.
                nexus-ingress     (lib)    3      TLS, rate-limit, circuit-breaker, balanceo, REST gateway.
Ejecutables     nexus-server      (exe)    1b+    Daemon del broker (orquesta reactores + ingress + admin).
                nexus-cli         (exe)    3      CLI de administración (nexus-cli).
                nexus-bench       (exe)    1      Generador de carga open-loop + histograma de latencia.
Cliente         nexus-client      (lib)    1b     Librería cliente C++ (smart-client). [+ binding Python, stretch]
Pruebas         nexus-tests       (tests)  1+     unit/property/integration/crash/chaos/sim/fuzz.

================================================================================
2. GRAFO DE DEPENDENCIAS ENTRE TARGETS (de abajo hacia arriba)
================================================================================
nexus-common                         (sin dependencias internas)
  ├── nexus-io          → common
  ├── nexus-protocol    → common
  ├── nexus-reactor     → common, io
  ├── nexus-storage     → common, io
  ├── nexus-consensus   → common, storage, protocol
  ├── nexus-broker      → common, io, reactor, storage, protocol, consensus
  ├── nexus-ingress     → common, io, protocol
  ├── nexus-client      → common, io, protocol
  ├── nexus-server (exe) → common, io, reactor, storage, protocol, consensus, broker, ingress
  ├── nexus-cli    (exe) → common, protocol, client
  ├── nexus-bench  (exe) → common, protocol, client
  └── nexus-tests        → (todos)

Regla de capas (forzada en CMake): un target NO puede depender de otro de su
misma capa o superior. `storage` nunca depende de `ingress`; `protocol` nunca de
`broker`; etc. La portabilidad Windows vive SOLO en `nexus-io` (adaptador IOCP).

================================================================================
3. ÁRBOL DE CARPETAS Y ARCHIVOS (alto nivel)
================================================================================
nexusmq/
├── CMakeLists.txt                 # raíz: define targets, capas, opciones
├── CMakePresets.json              # presets: linux-gcc, linux-clang, windows-msvc
├── vcpkg.json                     # deps (liburing solo en linux, etc.)
├── .clang-format · .clang-tidy · .dockerignore
├── docs/
│   ├── anteproyecto.md
│   ├── protocol.md                # spec del protocolo binario (Anexo A)
│   ├── openapi.yaml               # contrato del REST admin (RFC 7807)
│   └── adr/                       # adr-NNNN-*.md (extraídos de §9)
├── src/
│   ├── common/      → nexus-common
│   ├── io/          → nexus-io
│   ├── reactor/     → nexus-reactor
│   ├── storage/     → nexus-storage
│   ├── protocol/    → nexus-protocol
│   ├── consensus/   → nexus-consensus
│   ├── broker/      → nexus-broker
│   ├── ingress/     → nexus-ingress
│   └── server/      → nexus-server (exe)
├── client/cpp/      → nexus-client (+ bindings/python/)
├── tools/
│   ├── cli/         → nexus-cli (exe)
│   └── bench/       → nexus-bench (exe)
├── tests/           → nexus-tests
└── deploy/
    ├── docker/      # Dockerfile (multi-stage→distroless), docker-compose.yml
    └── k8s/         # Deployment, Service, probes

================================================================================
4. DESGLOSE POR MÓDULO/TARGET
================================================================================

--------------------------------------------------------------------------------
4.1 nexus-common  (src/common/)  — Fase 1
--------------------------------------------------------------------------------
Tipos transversales, bytes, checksum, error, configuración, logging, reloj.

types.hpp
  - Aliases de ancho fijo: `Offset=std::int64_t`, `PartitionId=std::int32_t`,
    `NodeId=std::int32_t`, `Term=std::int64_t`, `Index=std::int64_t`,
    `Crc=std::uint32_t`, `Epoch=std::int32_t`.
  - `enum class Codec : std::uint8_t { None, Lz4, Zstd }`.
  - Helpers little-endian: `+ load_le<T>(ByteSpan) -> T`, `+ store_le<T>(T, MutByteSpan)`.

bytes.hpp / .cpp
  **Buffer** — [REACTOR-LOCAL] búfer propietario (RAII) con capacidad/longitud.
    - `- data_: std::unique_ptr<std::byte[]>`, `- size_`, `- cap_`.
    - `+ reserve(size_t)`, `+ append(ByteSpan)`, `+ as_span() -> ByteSpan`, `+ clear()`.
  **ByteSpan / MutByteSpan** — [INMUTABLE] alias de `std::span<const std::byte>` /
    `std::span<std::byte>` (vista no propietaria, zero-copy).

crc32c.hpp / .cpp
  - `+ crc32c(ByteSpan, Crc seed=0) -> Crc` — SSE4.2 (`_mm_crc32_u64`) con detección
    de CPU en runtime y **fallback** software por tabla.
  - `- crc32c_hw(...)`, `- crc32c_sw(...)` (selección por `cpu_supports_sse42()`).

error.hpp
  **Error** — [INMUTABLE] código + contexto.
    - `- code_: ErrorCode`, `- context_: std::string`.
    - `+ code() const`, `+ message() const`, `+ with_context(std::string) const -> Error`.
  - `enum class ErrorCode` — espejo interno de los códigos de wire (§7.2.2) +
    internos (`IoError`, `Corrupt`, `OutOfSpace`, `Shutdown`...).
  - `template<class T> using expected = std::expected<T, Error>;`
  - Macro/helper `NEXUS_TRY(expr)` para propagación monádica.

config.hpp / .cpp
  **Config** — [INMUTABLE tras carga] catálogo de configuración (§7.10).
    - `- values_: std::unordered_map<std::string, std::string>`.
    - `+ load(path) -> expected<Config>` (fichero) `+ override_from_env()` (12-factor).
    - `+ get<T>(key, default) const -> T` (tipado: bytes, duración, enum).
    - Accesores tipados: `+ broker_id()`, `+ data_dir()`, `+ segment_bytes()`,
      `+ retention_ms()`, `+ default_acks()`, `+ fsync_policy()`,
      `+ raft_election_timeout()`, `+ num_reactors()`, `+ max_connections()`, …

logging.hpp / .cpp
  **Logger** — [THREAD-SAFE] log estructurado JSON con niveles.
    - `+ log(Level, std::string_view msg, fields...)`, `+ with(correlation_id)`.
    - Niveles `enum class Level { Trace, Debug, Info, Warn, Error }`.

clock.hpp
  **MonotonicClock** — [THREAD-SAFE] envoltorio de `std::chrono::steady_clock`
    (timeouts, heartbeats, elección Raft). `+ now() -> MonoTime`.
  **WallClock** — `std::chrono::system_clock` (timestamps de record, retención).
    NO se usa para ordenar (el orden lo da el offset). `+ now() -> WallTime`.

--------------------------------------------------------------------------------
4.2 nexus-io  (src/io/)  — Fase 1 (bloqueante) → 1b (proactor)
--------------------------------------------------------------------------------
Abstracción de E/S por *completions* (proactor). Único lugar con código por plataforma.

proactor.hpp
  **Proactor** — clase abstracta (puerto). [REACTOR-LOCAL]
    - `+ virtual submit_read(fd, MutByteSpan, Completion) = 0`.
    - `+ virtual submit_write(fd, ByteSpan, Completion) = 0`.
    - `+ virtual submit_fsync(fd, Completion) = 0`.
    - `+ virtual submit_accept(fd, Completion) = 0`.
    - `+ virtual submit_timer(MonoTime, Completion) = 0`.
    - `+ virtual run_completions(max) -> int = 0`  (drena la CQ).
  - `using Completion = std::function<void(std::int32_t result)>` (o callback inline).

io_uring_backend.hpp / .cpp   [Linux]
  **IoUringBackend : Proactor** — [REACTOR-LOCAL] un anillo por reactor.
    - `- ring_: io_uring` (RAII: `io_uring_queue_init`/`_exit`).
    - `- registered_buffers_`, `- fixed_files_` (registered buffers/fixed files).
    - Implementa los `submit_*` encolando SQEs; `run_completions` procesa CQEs.

iocp_backend.hpp / .cpp   [Windows, Fase 4]
  **IocpBackend : Proactor** — equivalente sobre *completion ports*.

awaitable.hpp
  *Awaitables* que suspenden la corrutina y reanudan en la completion (§3.3):
  **ReadAwaitable / WriteAwaitable / FsyncAwaitable / AcceptAwaitable / TimerAwaitable**
    - `+ await_ready() -> bool` (false: siempre suspende).
    - `+ await_suspend(coroutine_handle<>)` (registra la op en el proactor).
    - `+ await_resume() -> expected<ssize_t>` (entrega el resultado).
  Gotcha documentado: el búfer debe vivir hasta la completion.

file.hpp / .cpp
  **File** — [REACTOR-LOCAL] RAII sobre un descriptor de fichero.
    - `- fd_: int` (cierra en destructor).
    - Fase 1 (bloqueante): `+ pread(MutByteSpan, off) -> expected<size_t>`,
      `+ pwrite(ByteSpan, off) -> expected<size_t>`, `+ fsync() -> expected<void>`.
    - Fase 1b (async): `+ async_pread(...) -> task<expected<size_t>>`,
      `+ async_pwrite(...)`, `+ async_fsync() -> task<expected<void>>` (vía Proactor).
    - `+ static open(path, flags) -> expected<File>`.

socket.hpp / .cpp
  **Socket** — [REACTOR-LOCAL] RAII sobre socket TCP. `+ async_read/_write`, `+ close()`.
  **Listener** — RAII sobre socket de escucha. `+ async_accept() -> task<expected<Socket>>`.

--------------------------------------------------------------------------------
4.3 nexus-reactor  (src/reactor/)  — Fase 1b
--------------------------------------------------------------------------------
El reactor thread-per-core propio: corazón del modelo shared-nothing (ADR-0005).

reactor.hpp / .cpp
  **Reactor** — [REACTOR-LOCAL] dueño de su proactor, scheduler, allocator y particiones.
    - `- core_id_: int`, `- proactor_: std::unique_ptr<Proactor>`,
      `- scheduler_: CoroScheduler`, `- alloc_: ArenaAllocator`,
      `- mailbox_: CrossCoreMailbox`, `- partitions_: std::vector<Partition*>`.
    - `+ run()` — bucle: completions + scheduler + mailbox (+ busy-poll opcional medido).
    - `+ spawn(task<void>)` — programa una corrutina en este reactor.
    - `+ submit_to(core_id, std::move_only_function<void()>)` — envía trabajo a OTRO reactor (cross-core).
    - `+ stop()` — apagado limpio (drena en curso).
    - `- poll_once()`, `- drain_mailbox()`.

scheduler.hpp / .cpp
  **CoroScheduler** — [REACTOR-LOCAL] cola de `coroutine_handle<>` listas.
    - `- ready_: std::deque<std::coroutine_handle<>>`.
    - `+ schedule(handle)`, `+ run_ready() -> int`.

task.hpp
  **task<T>** — tipo de retorno de corrutina (promise propio, lazy, `co_await`-able).

spsc_queue.hpp
  **SpscQueue<T>** — [CROSS-CORE] cola lock-free single-producer/single-consumer.
    - `- head_` y `- tail_` **en líneas de caché distintas**
      (`alignas(std::hardware_destructive_interference_size)`) para evitar *false sharing*.
    - `+ try_push(T) -> bool`, `+ try_pop() -> std::optional<T>` (memory_order acquire/release).

mpmc_queue.hpp
  **MpmcQueue<T>** — [THREAD-SAFE] cola lock-free multi-prod/multi-cons.
    - CAS con **contador de versión** o *hazard pointers* para evitar **ABA**.
    - Validada con ThreadSanitizer + estrés aleatorio (CI).

cross_core.hpp / .cpp
  **CrossCoreMailbox** — [CROSS-CORE] N×N buzones SPSC entre reactores + `eventfd` de despierte.
    - `+ post(from_core, to_core, msg)`, `+ drain(handler)`.

allocator.hpp / .cpp
  **ArenaAllocator** — [REACTOR-LOCAL] arena por núcleo (RAII; libera el bloque al destruir).
    - `- arena_: std::pmr::monotonic_buffer_resource` (o pool); NUMA-aware.
    - `+ resource() -> std::pmr::memory_resource*`, `+ reset()`.
    - `placement new`/gestión cruda **confinada** aquí; la app no ve memoria cruda.

reactor_pool.hpp / .cpp
  **ReactorPool** — [THREAD-SAFE] crea N reactores, uno por núcleo, *pinned*.
    - `- reactors_: std::vector<std::unique_ptr<Reactor>>`,
      `- threads_: std::vector<std::jthread>`.
    - `+ start(num_reactors)` (afinidad por `pthread_setaffinity`),
      `+ shutdown()` (señaliza `stop()` a todos y une),
      `+ reactor_for(PartitionId) -> Reactor&` (asignación partición→núcleo).

--------------------------------------------------------------------------------
4.4 nexus-storage  (src/storage/)  — Fase 1
--------------------------------------------------------------------------------
Motor de log append-only: el núcleo de aprendizaje. I/O bloqueante (F1) → io_uring (F1b).

record.hpp / .cpp
  **RecordBatch** — [INMUTABLE en disco] unidad de escritura/replicación (§5.4).
    - Campos: `base_offset:Offset`, `length:i32`, `crc:Crc`, `attrs:u16`,
      `producer_id:i64`, `producer_epoch:i16`, `base_sequence:i32`, `record_count:i32`.
    - `+ encode(Buffer&) const`, `+ static decode(ByteSpan) -> expected<RecordBatch>`,
      `+ verify_crc() const -> bool`, `+ last_offset() const -> Offset`.
  **Record** — `length_delta:varint`, `attrs:i8`, `timestamp_delta:varint`,
      `offset_delta:varint`, `key:bytes?`, `value:bytes?`, `headers[]`.

record_batch_builder.hpp / .cpp
  **RecordBatchBuilder** — construye un batch (varint/zigzag, deltas), calcula CRC.
    - `+ add(key, value, headers)`, `+ build(base_offset) -> RecordBatch`.

index.hpp / .cpp
  **IndexEntry** — [INMUTABLE] `relative_offset:u32`, `file_position:u32`.
  **SparseIndex** — [REACTOR-LOCAL] índice disperso de un segmento (`.index`).
    - `- entries_: std::vector<IndexEntry>` (mmap del `.index`).
    - `+ append(Offset, file_pos)`, `+ floor(Offset) -> IndexEntry` (búsqueda binaria),
      `+ load(path) -> expected<SparseIndex>`.

segment.hpp / .cpp
  **Segment** — [REACTOR-LOCAL] un tramo del log (`.log` + `.index`).
    - `- base_offset_:Offset`, `- log_:File`, `- index_:SparseIndex`,
      `- size_bytes_`, `- max_timestamp_`, `- state_:{Active,Closed}`.
    - `+ append(const RecordBatch&) -> expected<Offset>`,
      `+ read(Offset, max_bytes) -> expected<FetchResult>`,
      `+ seal()` (cierra y finaliza índice), `+ recover() -> expected<Offset>`.

partition_log.hpp / .cpp
  **PartitionLog** — [REACTOR-LOCAL] secuencia de segmentos de una partición.
    - `- segments_: std::vector<std::unique_ptr<Segment>>`, `- active_:Segment*`,
      `- log_start_offset_`, `- log_end_offset_`, `- recovery_point_`, `- cfg_:const LogConfig&`.
    - `+ append(const RecordBatch&) -> expected<Offset>` (rota si supera segment.bytes).
    - `+ read(Offset, max_bytes) -> expected<FetchResult>` (seek por índice; §7.11 #3).
    - `+ recover() -> expected<void>` (valida CRC + **trunca cola torn**; §7.11 #2).
    - `+ fsync(FsyncPolicy) -> task<expected<void>>` (asíncrono bajo el reactor).
    - `+ enforce_retention(const RetentionPolicy&)`.
    - `- roll_segment()`, `- segment_for(Offset) -> Segment*`.

retention.hpp / .cpp
  **RetentionPolicy** — [INMUTABLE] por tiempo/tamaño + compactación por clave.
    - `+ eligible(const Segment&, now) -> bool`.
  **LogCompactor** — compactación por clave (retiene último valor por clave).

recovery.hpp / .cpp
  - `+ recover_partition(dir, cfg) -> expected<PartitionLog>` — orquesta la recuperación.

--------------------------------------------------------------------------------
4.5 nexus-protocol  (src/protocol/)  — Fase 1b
--------------------------------------------------------------------------------
Protocolo binario propio: framing, mensajes, códigos de error, compresión, créditos.

frame.hpp / .cpp
  **FrameHeader** — [INMUTABLE] `length:u32 | api_key:u16 | api_version:u16 |
      correlation_id:u32 | flags:u16` (§7.2).
    - `+ encode(Buffer&) const`, `+ static decode(ByteSpan) -> expected<FrameHeader>`.
  **FrameReader / FrameWriter** — leen/escriben frames con longitud-prefijo sobre un Socket.
    - `+ read_frame() -> task<expected<Frame>>`, `+ write_frame(...) -> task<expected<void>>`.

codec.hpp
  - Helpers de serialización: `+ put_varint/get_varint`, `+ zigzag`, `+ put_le/get_le`,
    `+ put_string/get_bytes` (todos con chequeo de límites — decodificador defensivo).

messages.hpp / .cpp
  Structs request/response por operación (§7.2.1), con `encode`/`decode`:
  - **ApiVersionsRequest/Response**, **MetadataRequest/Response**,
    **ProduceRequest/Response** (`acks`, `batch` / `base_offset, error_code, throttle_ms`),
    **FetchRequest/Response** (`fetch_offset, max_bytes, min_bytes, max_wait_ms` /
      `batches, high_watermark, log_start_offset, error_code`),
    **OffsetCommit/Fetch**, **JoinGroup/SyncGroup/Heartbeat/LeaveGroup**,
    **CreateTopic/DeleteTopic**.

error_code.hpp
  - `enum class WireError : std::int16_t { None=0, NotLeaderForPartition,
      LeaderNotAvailable, UnknownTopicOrPartition, OffsetOutOfRange, NotEnoughReplicas,
      RequestTimedOut, CorruptMessage, MessageTooLarge, OutOfOrderSequence,
      DuplicateSequence, Throttled, RebalanceInProgress, UnsupportedVersion,
      Unauthorized, InvalidRequest }` (§7.2.2).
  - `+ is_retryable(WireError) -> bool`.

versioning.hpp
  - `+ negotiate(client_versions, supported) -> ApiVersionRange` (negociación).

compression.hpp / .cpp
  **Compressor** — [THREAD-SAFE] none/LZ4/Zstd por batch.
    - `+ compress(Codec, ByteSpan) -> Buffer`,
      `+ decompress(Codec, ByteSpan, max_out) -> expected<Buffer>`  ← límite anti
      *decompression bomb* (ratio + tamaño descomprimido).

credits.hpp
  **CreditWindow** — [REACTOR-LOCAL] control de flujo por créditos (§6.3/§7.11 #4).
    - `- credits_:i32`. `+ take(cost) -> task<void>` (se frena si 0), `+ grant(n)`.

--------------------------------------------------------------------------------
4.6 nexus-consensus  (src/consensus/)  — Fase 2
--------------------------------------------------------------------------------
Raft por partición: un grupo Raft por partición; el log de Raft ES el log de la partición.

raft_state.hpp
  **RaftPersistentState** — [REACTOR-LOCAL] (en disco): `current_term:Term`,
      `voted_for:std::optional<NodeId>`, log (delegado a RaftLog).
  **RaftVolatileState** — `commit_index:Index`, `last_applied:Index`;
      en líder: `next_index[peer]`, `match_index[peer]`.
  **RaftLogEntry** — `term:Term`, `index:Index`, `type:{Data,Config}`,
      `payload` (RecordBatch o cambio de configuración).
  **Snapshot** — `last_included_index`, `last_included_term`, `state`.

raft_rpc.hpp
  - **RequestVoteArgs/Reply**, **AppendEntriesArgs/Reply**, **InstallSnapshotArgs/Reply**
    (con `encode`/`decode` sobre el codec del protocolo).

raft_log.hpp / .cpp
  **RaftLog** — [REACTOR-LOCAL] adaptador sobre PartitionLog para vista (term,index).
    - `+ append(entries) -> expected<Index>`, `+ truncate_from(Index)`,
      `+ term_at(Index) -> expected<Term>`, `+ last_index()`, `+ entries_from(Index)`.

raft_node.hpp / .cpp
  **RaftNode** — [REACTOR-LOCAL] máquina de estados de una réplica de partición.
    - `- role_:{Follower,Candidate,Leader}`, `- persistent_:RaftPersistentState`,
      `- volatile_:RaftVolatileState`, `- log_:RaftLog`, `- peers_:std::vector<NodeId>`,
      `- election_timer_`, `- heartbeat_timer_` (reloj monotónico), `- leader_epoch_:Epoch`.
    - `+ propose(RecordBatch) -> task<expected<Index>>` (solo líder; §7.11 #1).
    - `+ on_append_entries(AppendEntriesArgs) -> AppendEntriesReply` (§7.11 #5).
    - `+ on_request_vote(RequestVoteArgs) -> RequestVoteReply`.
    - `+ tick(MonoTime)` — vence election/heartbeat; con **pre-vote**.
    - `+ commit_index() const`, `+ is_leader() const`, `+ leader_epoch() const`.
    - `- become_follower/_candidate/_leader()`, `- advance_commit_index()`,
      `- replicate_to(peer)`.

election.hpp / .cpp
  - Soporte de **pre-vote**, **leadership transfer**, réplicas **learner** (no votan).

--------------------------------------------------------------------------------
4.7 nexus-broker  (src/broker/)  — Fase 1b–2
--------------------------------------------------------------------------------
El núcleo del broker: topics, particiones, grupos, offsets, idempotencia, backpressure.

topic.hpp / .cpp
  **TopicMetadata** — [INMUTABLE] `name`, `partition_count`, `replication_factor`,
      `config{retention,segment_bytes,compaction,compression}`, `created_at`.
  **Topic** — [REACTOR-LOCAL por partición] mapa `partition_id → Partition*`.

partition.hpp / .cpp
  **Partition** — [REACTOR-LOCAL] une PartitionLog + RaftNode; unidad de serialización.
    - `- log_:PartitionLog`, `- raft_:RaftNode`, `- state_:PartitionState`,
      `- producers_: std::unordered_map<i64, ProducerSession>`, `- credits_:CreditWindow`.
    - `+ produce(const RecordBatch&, Acks) -> task<expected<Offset>>` (hot path; §7.11 #1):
        valida CRC/tamaño → idempotencia → raft.propose → según acks (0/1/quorum).
    - `+ fetch(Offset, max_bytes) -> task<expected<FetchResult>>` (hasta high-watermark).
    - `+ high_watermark() const`, `+ is_leader() const`, `+ leader_epoch() const`.
    - `- check_sequence(producer_id, base_seq) -> {Accept,Duplicate,Gap}`.

partition_state.hpp
  **PartitionState** — [INMUTABLE/observable] `topic`, `partition_id`, `leader_epoch`,
      `leader_node_id`, `replica_node_ids[]`, `high_watermark`, `log_start_offset`,
      `log_end_offset`, `owner_reactor_id`.

produce_handler.hpp / fetch_handler.hpp / .cpp
  - Corrutinas que traducen el mensaje de wire ↔ `Partition::produce/fetch`,
    enrutando al reactor dueño de la partición (cross-core si toca).

consumer_group.hpp / .cpp
  **ConsumerGroup** — [REACTOR-LOCAL] `group_id`, `generation_id`,
      `state:{Empty,PreparingRebalance,Stable,Dead}`, `members[]`, `leader_member_id`.
    - `+ join(member) `, `+ sync(assignment)`, `+ heartbeat(member)`, `+ leave(member)`,
      `- rebalance()`.

offset_manager.hpp / .cpp
  **OffsetManager** — almacena `OffsetCommit{group,topic,partition,offset,metadata}`.
    - `+ commit(...) -> expected<void>`, `+ fetch(group,topic,partition) -> expected<Offset>`.

producer_session.hpp
  **ProducerSession** — [REACTOR-LOCAL] idempotencia: `producer_id`, `epoch`,
      `last_sequence[partition]`. Descarta duplicados por reintento (§5.9).

topic_manager.hpp / .cpp
  **TopicManager** — [THREAD-SAFE / plano de control] crea/borra topics, asigna
      particiones a núcleos, publica metadata.
    - `+ create_topic(spec) -> expected<TopicMetadata>`, `+ delete_topic(name)`,
      `+ describe(name) -> expected<TopicDescription>`, `+ metadata() -> Metadata`,
      `+ reassign_partitions(plan) -> expected<void>`.

dlq.hpp / backpressure.hpp
  **DeadLetterQueue** — destino de mensajes no procesables tras N intentos.
  **BackpressureController** — políticas de crédito por conexión/topic.

--------------------------------------------------------------------------------
4.8 nexus-ingress  (src/ingress/)  — Fase 3
--------------------------------------------------------------------------------
Capa de ingress en dos modos (ADR-0006) + gateway REST de administración.

tls.hpp / .cpp
  **TlsContext** — [THREAD-SAFE] RAII sobre `SSL_CTX` (OpenSSL); TLS 1.3, valida certs.
  **TlsConnection** — [REACTOR-LOCAL] RAII sobre `SSL`; handshake, read/write; **mTLS** intra-cluster.

rate_limiter.hpp / .cpp
  **TokenBucket** — [REACTOR-LOCAL] por cliente/topic. `+ allow(cost) -> bool`, `+ refill(now)`.

circuit_breaker.hpp / .cpp
  **CircuitBreaker** — estados `{Closed,Open,HalfOpen}` + ventana deslizante de errores.
    - `+ allow() -> bool`, `+ on_success()`, `+ on_failure()`.

load_balancer.hpp / .cpp
  **LoadBalancer** — `round-robin` / `least-connections` / `consistent-hashing` (partition key).

health_check.hpp / .cpp
  **HealthChecker** — activo (ping) + pasivo (errores reales); consume `/readyz`.

rest_gateway.hpp / .cpp
  **RestGateway** — [REACTOR-LOCAL] servidor HTTP del REST admin (§7.6).
    - Rutas `/api/v1/...`, semántica HTTP (safe/idempotente), **paginación**,
      errores **`ProblemDetail` (RFC 7807)**, **Bearer JWT**, contrato **OpenAPI**.
    - `+ route(HttpRequest) -> task<HttpResponse>`; traduce a `TopicManager`/métricas;
      `- to_problem_detail(Error) -> HttpResponse`.

proxy.hpp / .cpp
  **Proxy** — modo proxy (clientes "tontos"): enruta por consistent-hashing al líder.

connection_state.hpp
  **ConnectionState** — [REACTOR-LOCAL] `conn_id`, `negotiated_versions`,
      `auth_principal?`, `credits`, `inflight[correlation_id]`.

--------------------------------------------------------------------------------
4.9 nexus-server  (src/server/)  — exe — Fase 1b+
--------------------------------------------------------------------------------
Daemon del broker: orquesta reactores, conexiones, admin y observabilidad.

main.cpp
  - `bootstrap()`: parse `Config` → init `Logger` → crear `ReactorPool` →
    recuperar particiones (`recover_partition`) → arrancar `Listener`(es) →
    registrar **señales** (`SIGTERM`/`SIGINT` → apagado limpio vía `eventfd`) →
    `pool.run()`.

server.hpp / .cpp
  **Server** — [THREAD-SAFE] cohesiona ReactorPool + TopicManager + Ingress + AdminApi.
    - `+ start()`, `+ shutdown()` (drena conexiones, fsync final, transfiere liderazgos).

connection.hpp / .cpp
  **Connection** — [REACTOR-LOCAL] una conexión de cliente, corrutina dedicada.
    - `- socket_:Socket | TlsConnection`, `- state_:ConnectionState`, `- reader_:FrameReader`.
    - `+ serve() -> task<void>` — bucle: lee frame → despacha por `api_key` → responde;
      multiplexa por `correlation_id`; respeta créditos.

request_router.hpp / .cpp
  **RequestRouter** — mapea `api_key` → handler; **reenvía** al reactor dueño de la
    partición destino (cross-core) cuando la conexión cae en otro núcleo.

admin_api.hpp / .cpp
  **AdminApi** — implementa los endpoints REST (vía RestGateway) sobre `TopicManager`.

metrics.hpp / .cpp
  **MetricsRegistry** — [THREAD-SAFE] contadores/gauges/histogramas; exposición Prometheus
    en `/metrics` (throughput, lag, estado Raft —term/líder/commitIndex—, latencias).

--------------------------------------------------------------------------------
4.10 nexus-client  (client/cpp/)  — lib — Fase 1b
--------------------------------------------------------------------------------
Librería cliente nativa (smart-client). Depende solo de common/io/protocol.

client.hpp / .cpp
  **Client** — [THREAD-SAFE] punto de entrada; gestiona metadata y conexiones.
    - `- metadata_:MetadataCache`, `- pool_:ConnectionPool`.
    - `+ connect(seeds) -> expected<void>`, `+ producer() -> Producer`, `+ consumer(group) -> Consumer`.

metadata_cache.hpp / .cpp
  **MetadataCache** — `topic/partition → leader`; refresco ante `NotLeaderForPartition`.
    - `+ leader_for(topic, partition) -> expected<NodeId>`, `+ refresh()`.

connection_pool.hpp / .cpp
  **ConnectionPool** — **reutiliza** conexiones TCP+TLS (no abrir una por petición).
    - `+ get(node) -> expected<Connection*>`, `+ release(...)`.

producer.hpp / .cpp
  **Producer** — `+ send(topic, partition?, key, value, Acks) -> task<expected<Offset>>`;
    idempotencia (producer-id+sequence), créditos, reintentos con backoff+jitter (solo idempotentes).

consumer.hpp / .cpp
  **Consumer** — `+ subscribe(topics)`, `+ poll(timeout) -> std::vector<Record>`,
    `+ commit(offsets)`; membresía de grupo (Join/Sync/Heartbeat).

bindings/python/  (stretch)
  - `module.cpp` (pybind11) expone `Client`/`Producer`/`Consumer`.

--------------------------------------------------------------------------------
4.11 nexus-cli  (tools/cli/)  — exe — Fase 3
--------------------------------------------------------------------------------
CLI de administración `nexus-cli` (habla REST admin / protocolo).

main.cpp — dispatch de subcomandos.
topic_commands.cpp     — `topic create|list|describe|delete`.
group_commands.cpp     — `group list|describe`.
partition_commands.cpp — `partitions reassign`.
admin_commands.cpp     — `diagnostics`, `metrics`.
  - Cada comando: parse args → llama al REST admin (o a `nexus-client`) → imprime.

--------------------------------------------------------------------------------
4.12 nexus-bench  (tools/bench/)  — exe — Fase 1
--------------------------------------------------------------------------------
Generador de carga **open-loop** + histograma (metodología anti coordinated-omission, §8.2).

main.cpp — parsea parámetros, lanza el bench, imprime el informe.
load_generator.hpp / .cpp
  **LoadGenerator** — emite a **tasa fija independiente de las respuestas** (open-loop).
    - `+ run(BenchConfig) -> BenchReport`.
latency_histogram.hpp / .cpp
  **LatencyHistogram** — estilo **HdrHistogram**; reporta `p50/p99/p999/max`.
    - `+ record(ns)`, `+ percentile(p) -> ns`, `+ merge(other)`.
bench_config.hpp
  **BenchConfig** — `target_rate`, `batch_size`, `acks`, `payload_size`, `duration`, `warmup`.

--------------------------------------------------------------------------------
4.13 nexus-tests  (tests/)  — Fase 1+
--------------------------------------------------------------------------------
Matriz de testing (§8.1). GoogleTest; nombres `Metodo_Escenario_ResultadoEsperado`;
sanitizers (ASan/UBSan/TSan) en CI; TDD (rojo→verde→refactor).

tests/
├── unit/            # por módulo: record, index, crc32c, codec, raft_state, token_bucket…
├── property/        # round-trip encode/decode (RecordBatch, frames); invariantes del log y de Raft
├── integration/     # e2e: productor → broker → consumidor (un nodo)
├── crash/           # kill -9 a mitad de escritura; recuperación + truncado de cola torn
├── chaos/           # tc netem (partición de red) → failover; verificación de postura CP
├── sim/             # simulación determinista de Raft (reloj y red virtuales)
├── fuzz/            # decodificador del protocolo (entrada no confiable)
└── support/         # dobles: VirtualClock, VirtualNetwork, InMemoryFile, FakeProactor
  **VirtualClock / VirtualNetwork** — [test] inyectan tiempo/red deterministas
    para cumplir FIRST en pruebas concurrentes/distribuidas.

--------------------------------------------------------------------------------
4.14 deploy/  — Fase 3
--------------------------------------------------------------------------------
docker/Dockerfile
  - **multi-stage**: etapa build (toolchain C++ + vcpkg) → etapa runtime **distroless**
    (copia solo el binario); **usuario no-root** (`USER`); **`HEALTHCHECK`** → `/readyz`.
docker/docker-compose.yml
  - Cluster de **3 nodos** (`nexus-1..3`) + Prometheus + Grafana; `healthchecks` + `depends_on`.
k8s/
  - `Deployment` (réplicas), `Service`, `ConfigMap`/`Secret`, **probes** liveness/readiness,
    límites de CPU/memoria por pod.
.dockerignore — excluye `build/`, artefactos, secretos.

================================================================================
5. BUILD — archivos raíz (resumen)
================================================================================
CMakeLists.txt (raíz)
  - `cmake_minimum_required(VERSION 3.25)`; `project(NexusMQ CXX)`;
    `target_compile_features(... cxx_std_20)`.
  - Flags: `-Wall -Wextra -Wpedantic -Werror` (GCC/Clang) / `/W4 /WX` (MSVC);
    opción `NEXUS_SANITIZERS` (ASan/UBSan/TSan) para la build de pruebas.
  - `add_subdirectory(src/...)` por target; agrupa en *folders* de VS
    (`set_target_properties(... FOLDER "nucleo|exe|cliente|pruebas")`).
  - I/O por plataforma: `if(WIN32) → iocp_backend` `else() → io_uring_backend`.
CMakePresets.json — `linux-gcc`, `linux-clang`, `windows-msvc`.
vcpkg.json — deps: `liburing` (`"platform": "linux"`), `openssl`, `lz4`, `zstd`,
    `fmt`, `gtest`, `benchmark`, `pybind11` (stretch).

================================================================================
6. MAPA FASE → TARGETS (qué se construye en cada fase)
================================================================================
Fase 1   : nexus-common, nexus-storage (I/O bloqueante), nexus-bench, nexus-tests(unit/property/crash).
Fase 1b  : nexus-io (proactor), nexus-reactor, nexus-protocol, nexus-broker (mono-nodo),
           nexus-client, nexus-server (mono-nodo). Tests: integration.
Fase 2   : nexus-consensus (Raft), broker distribuido (grupos, rebalanceo). Tests: sim/chaos.
Fase 3   : nexus-ingress (TLS, REST admin), nexus-cli, observabilidad, deploy/.
Fase 4   : direct I/O, subconjunto Kafka, binding Python, backend IOCP (Windows).

================================================================================
FIN DEL DESGLOSE
================================================================================
> Documento de diseño preliminar. Las firmas y la lista de miembros son una
> proyección coherente con el anteproyecto v0.4.0; se refinarán al implementar
> cada fase (rojo→verde→refactor). Identificadores en inglés, doc-comments en español.
