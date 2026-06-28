================================================================================
DESGLOSE DETALLADO DE LA SOLUCIÓN — NexusMQ (C++23)
================================================================================
**Archivo:** `nexusmq-desglose-detallado.md`
**Versión:** 0.3.0 (amplía `nexusmq-desglose.md`; reconciliado con el estado as-built)
**Fecha:** 2026-06-28
**Naturaleza:** El plano de implementación, módulo a módulo. Nació como proyección de diseño
(pre-implementación) y se ha **reconciliado con el código real**: §4.15–§4.18 cubren los targets
añadidos durante la implementación (telemetry, cluster, kafka, ffi) y el estándar es **C++23**
(`cxx_std_23`, ADR-0011). Las firmas de los módulos originales son la **intención de diseño**;
los ajustes finos respecto al código se anotan en `hoja-de-ruta.md` ("Ajustes de diseño").

LEYENDA
  Visibilidad:  `+` público · `#` protegido · `-` privado.
  Afinidad:     [REACTOR-LOCAL] no thread-safe, de un reactor · [INMUTABLE] solo lectura ·
                [CROSS-CORE] comunicación entre reactores · [THREAD-SAFE] uso concurrente.
  Tipos clave:  `expected<T> = std::expected<T, Error>` · `task<T>` = corrutina ·
                `ByteSpan = std::span<const std::byte>` · `MutByteSpan = std::span<std::byte>`.
  Errores:      núcleo/hot-path → `expected<T>`; corrutinas → `task<expected<T>>`;
                plano de control → excepciones; wire → `WireError` (ADR-0009).
  Naming:       tipos PascalCase · funciones/miembros snake_case · miembros privados `_`.
  Estructura raíz, áreas, targets y grafo de dependencias: ver `nexusmq-desglose.md` §1–§3.

================================================================================
4.1  nexus-common   (src/common/)   — Fase 1   — deps: (ninguna)
================================================================================
Tipos transversales, bytes/vistas, checksum, error/resultado, config, logging, reloj.

----- types.hpp -----------------------------------------------------------------
Aliases de ancho fijo (semántica de dominio sobre `<cstdint>`):
  + using Offset       = std::int64_t;   // offset lógico monótono en una partición
  + using PartitionId  = std::int32_t;
  + using NodeId       = std::int32_t;
  + using Term         = std::int64_t;   // término de Raft
  + using Index        = std::int64_t;   // índice del log de Raft
  + using Epoch        = std::int32_t;   // leaderEpoch (fencing token)
  + using Crc          = std::uint32_t;
  + using ProducerId   = std::int64_t;
  + using Sequence     = std::int32_t;
  + enum class Codec    : std::uint8_t { None, Lz4, Zstd };
  + enum class Acks     : std::uint8_t { None = 0, Leader = 1, Quorum = 2 };
  + struct FetchResult { Buffer batches; Offset next_offset; Offset high_watermark; };

----- endian.hpp ----------------------------------------------------------------
Serialización little-endian explícita (nunca asumir endianness del host):
  + template<std::integral T> T   load_le(ByteSpan src) noexcept;
  + template<std::integral T> void store_le(T value, MutByteSpan dst) noexcept;

----- bytes.hpp / bytes.cpp -----------------------------------------------------
**Buffer**  [REACTOR-LOCAL]  — búfer propietario (RAII), crece bajo demanda.
  - data_ : std::unique_ptr<std::byte[]>
  - size_ : std::size_t
  - cap_  : std::size_t
  + explicit Buffer(std::size_t cap = 0);
  + void       reserve(std::size_t n);                 // realoca si cap_ < n
  + void       append(ByteSpan src);                   // copia al final
  + std::byte* tail(std::size_t n);                    // reserva n y devuelve puntero
  + ByteSpan   view() const noexcept;                  // vista de [0, size_)
  + MutByteSpan mut() noexcept;
  + void       clear() noexcept;                       // size_ = 0 (conserva cap_)
  + std::size_t size() const noexcept;

**ByteSpan / MutByteSpan**  [INMUTABLE]  — alias de `std::span` (vistas no propietarias).
  Gotcha documentado: no sobreviven al origen (vida útil).

----- crc32c.hpp / crc32c.cpp ---------------------------------------------------
Checksum CRC32C (Castagnoli), estándar de facto en logs; HW (SSE4.2) + fallback SW.
  + Crc  crc32c(ByteSpan data, Crc seed = 0) noexcept;   // dispatch HW/SW
  - Crc  crc32c_hw(ByteSpan, Crc) noexcept;              // _mm_crc32_u64
  - Crc  crc32c_sw(ByteSpan, Crc) noexcept;              // tabla precomputada
  - bool cpu_has_sse42() noexcept;                       // CPUID, cacheado

----- error.hpp -----------------------------------------------------------------
**Error**  [INMUTABLE]  — código + contexto; envoltorio de errores internos.
  - code_    : ErrorCode
  - context_ : std::string
  + Error(ErrorCode code, std::string context = {});
  + ErrorCode         code() const noexcept;
  + std::string_view  context() const noexcept;
  + std::string       message() const;                   // "<code>: <context>"
  + Error             with(std::string more) const;      // añade contexto al propagar
  + enum class ErrorCode { Ok, Io, Corrupt, OutOfSpace, NotLeader, NotFound,
        InvalidArgument, Timeout, Unauthorized, OutOfOrderSequence, Shutdown, Internal };

----- result.hpp ----------------------------------------------------------------
  + template<class T> using expected = std::expected<T, Error>;
  + #define NEXUS_TRY(expr)   // evalúa expr; si !has_value, return std::unexpected(error)
  + template<class T> expected<T> ok(T value);
  + std::unexpected<Error>        fail(ErrorCode, std::string ctx = {});

----- config.hpp / config.cpp ---------------------------------------------------
**Config**  [INMUTABLE tras carga]  — catálogo de configuración (§7.10), fichero + env.
  - values_ : std::unordered_map<std::string, std::string>
  + static expected<Config> load(const std::filesystem::path& file);
  + void   override_from_env(std::string_view prefix = "NEXUS_");   // 12-factor
  + template<class T> T get(std::string_view key, T fallback) const;
  // accesores tipados (uno por clave del §7.10):
  + NodeId        broker_id() const;
  + std::filesystem::path data_dir() const;
  + int           num_reactors() const;                  // 0 → nº de cores
  + std::size_t   segment_bytes() const;                 // def 1 GiB
  + Duration      retention_ms() const;                  // def 7 d
  + std::int64_t  retention_bytes() const;               // def -1 (∞)
  + std::size_t   max_message_bytes() const;             // def 1 MiB
  + std::size_t   max_batch_bytes() const;               // def 4 MiB
  + Codec         compression() const;
  + Acks          default_acks() const;                  // def Quorum
  + FsyncPolicy   fsync_policy() const;
  + Duration      raft_election_timeout() const;         // aleatorizado 1000–1500 ms
  + Duration      raft_heartbeat_interval() const;       // 150 ms
  + int           replication_factor() const;            // def 3
  + std::size_t   max_connections() const;
  + bool          tls_enabled() const;
  + std::uint16_t metrics_port() const;

**FsyncPolicy**  [INMUTABLE]  — `enum class { None, Interval, Commit }` + `Duration interval`.

----- logging.hpp / logging.cpp -------------------------------------------------
**Logger**  [THREAD-SAFE]  — log estructurado JSON con niveles y correlation IDs.
  + enum class Level { Trace, Debug, Info, Warn, Error };
  + void log(Level lvl, std::string_view msg, std::span<const Field> fields = {});
  + Logger with(std::string correlation_id) const;       // logger contextualizado
  + static Logger& global();
  - sink_ : Sink&                                        // stdout/fichero
  **Field** { std::string_view key; std::variant<...> value; };

----- clock.hpp -----------------------------------------------------------------
**MonotonicClock**  [THREAD-SAFE]  — timeouts/heartbeats/elección (nunca retrocede).
  + static MonoTime now() noexcept;                      // steady_clock
**WallClock**  [THREAD-SAFE]  — timestamps de record y retención (NO para ordenar).
  + static WallTime now() noexcept;                      // system_clock (NTP)
  + using Duration = std::chrono::nanoseconds;

================================================================================
4.2  nexus-io   (src/io/)   — Fase 1 (bloqueante) → 1b (proactor)   — deps: common
================================================================================
Abstracción de E/S por *completions* (proactor). ÚNICO módulo con código por plataforma
(io_uring en Linux; IOCP + Win32/Winsock en Windows, implementado y compile-verificado con MinGW,
ADR-0022; el handle es `NativeHandle`). El resto del sistema es agnóstico.

----- proactor.hpp --------------------------------------------------------------
**Proactor**  [REACTOR-LOCAL]  — puerto abstracto de E/S asíncrona (un anillo por reactor).
  + using Completion = std::move_only_function<void(std::int32_t result)>;
  + virtual ~Proactor() = default;
  + virtual void submit_read (int fd, MutByteSpan buf, std::uint64_t off, Completion) = 0;
  + virtual void submit_write(int fd, ByteSpan  buf, std::uint64_t off, Completion) = 0;
  + virtual void submit_fsync(int fd, bool datasync, Completion) = 0;     // IORING_OP_FSYNC
  + virtual void submit_accept(int listen_fd, Completion) = 0;
  + virtual void submit_recv (int fd, MutByteSpan, Completion) = 0;
  + virtual void submit_send (int fd, ByteSpan,    Completion) = 0;
  + virtual void submit_timer(MonoTime deadline, Completion) = 0;
  + virtual int  run_completions(int max) = 0;           // drena la CQ; nº procesadas
  + virtual void wake() = 0;                             // despierta desde otro hilo (eventfd)

----- io_uring_backend.hpp / .cpp   [Linux] -------------------------------------
**IoUringBackend : Proactor**  [REACTOR-LOCAL]
  - ring_       : io_uring                               // RAII: queue_init / queue_exit
  - reg_buffers_: std::vector<iovec>                     // registered buffers
  - fixed_files_: std::vector<int>                       // fixed file descriptors
  - wake_fd_    : int                                    // eventfd para wake()
  + explicit IoUringBackend(unsigned entries);
  + ~IoUringBackend();                                   // io_uring_queue_exit
  // implementa todos los submit_* encolando SQEs; run_completions procesa CQEs y
  // ejecuta la Completion asociada (guardada en user_data).
  - io_uring_sqe* get_sqe();
  - void          submit_pending();

----- iocp_backend.hpp / .cpp   [Windows, implementado — ADR-0022] --------------
**IocpBackend : Proactor**  — equivalente sobre IO Completion Ports (mismo contrato).
  Compile-verificado con MinGW (GetQueuedCompletionStatusEx/AcceptEx/PostQueuedCompletionStatus);
  runtime pendiente de Windows.

----- awaitable.hpp -------------------------------------------------------------
Awaitables que suspenden la corrutina y la reanudan en la completion (§3.3):
**ReadAwaitable / WriteAwaitable / FsyncAwaitable / AcceptAwaitable /
 RecvAwaitable / SendAwaitable / TimerAwaitable**  [REACTOR-LOCAL]
  - proactor_ : Proactor&
  - args…     : (fd, buf, off…)
  - result_   : std::int32_t
  + bool             await_ready() const noexcept;       // false → siempre suspende
  + void             await_suspend(std::coroutine_handle<> h);   // registra la op
  + expected<std::size_t> await_resume() const noexcept; // <0 → Error{Io}
  // Gotcha: el buffer debe vivir hasta la completion (no co_await sobre temporales).

----- file.hpp / file.cpp -------------------------------------------------------
**File**  [REACTOR-LOCAL]  — RAII sobre un descriptor de fichero.
  - fd_ : int                                            // -1 = cerrado
  + static expected<File> open(const std::filesystem::path&, int flags, mode_t = 0644);
  + ~File();                                             // ::close(fd_)
  + File(File&&) noexcept;  + File& operator=(File&&) noexcept;   // movible, no copiable
  // Fase 1 (bloqueante):
  + expected<std::size_t> pread (MutByteSpan, std::uint64_t off);
  + expected<std::size_t> pwrite(ByteSpan,    std::uint64_t off);
  + expected<void>        fsync(bool datasync = false);
  + expected<std::uint64_t> size() const;
  + expected<void>        truncate(std::uint64_t len);
  // Fase 1b (asíncrono vía Proactor):
  + task<expected<std::size_t>> async_pread (Proactor&, MutByteSpan, std::uint64_t off);
  + task<expected<std::size_t>> async_pwrite(Proactor&, ByteSpan,    std::uint64_t off);
  + task<expected<void>>        async_fsync (Proactor&, bool datasync = false);
  + int fd() const noexcept;

----- socket.hpp / socket.cpp ---------------------------------------------------
**Socket**  [REACTOR-LOCAL]  — RAII sobre socket TCP conectado.
  - fd_ : int
  + task<expected<std::size_t>> async_recv(Proactor&, MutByteSpan);
  + task<expected<std::size_t>> async_send(Proactor&, ByteSpan);
  + void set_nodelay(bool);   + void close() noexcept;   + int fd() const noexcept;

**Listener**  [REACTOR-LOCAL]  — RAII sobre socket de escucha.
  + static expected<Listener> bind(std::string_view host, std::uint16_t port, int backlog = 1024);
  + task<expected<Socket>> async_accept(Proactor&);

================================================================================
4.3  nexus-reactor   (src/reactor/)   — Fase 1b   — deps: common, io
================================================================================
El reactor thread-per-core propio (ADR-0005): un reactor por núcleo, estado por núcleo,
comunicación entre núcleos solo por paso de mensajes. Materializa el shared-nothing.

----- task.hpp ------------------------------------------------------------------
**task<T>**  — tipo de retorno de corrutina (lazy, una continuación, sin asignación si se elide).
  + struct promise_type { ... initial_suspend/final_suspend/return_value/unhandled_exception };
  + bool await_ready();  + void await_suspend(coroutine_handle<>);  + T await_resume();
  + ~task();   // destruye el frame si no se consumió

----- scheduler.hpp / scheduler.cpp ---------------------------------------------
**CoroScheduler**  [REACTOR-LOCAL]  — cola de corrutinas listas para reanudar.
  - ready_ : std::deque<std::coroutine_handle<>>
  + void schedule(std::coroutine_handle<> h);
  + int  run_ready();                                    // reanuda todas las listas; nº ejecutadas
  + bool empty() const noexcept;

----- spsc_queue.hpp ------------------------------------------------------------
**SpscQueue<T, Cap>**  [CROSS-CORE]  — ring lock-free single-producer/single-consumer.
  - alignas(64) head_ : std::atomic<std::size_t>         // consumidor (línea de caché propia)
  - alignas(64) tail_ : std::atomic<std::size_t>         // productor (línea de caché propia)
  - buf_  : std::array<T, Cap>
  + bool               try_push(T value);                // release; false si lleno
  + std::optional<T>   try_pop();                        // acquire; nullopt si vacío
  + std::size_t        size_approx() const noexcept;
  // alignas(std::hardware_destructive_interference_size) evita false sharing head/tail.

----- mpmc_queue.hpp ------------------------------------------------------------
**MpmcQueue<T, Cap>**  [THREAD-SAFE]  — ring lock-free multi-prod/multi-cons (Vyukov).
  - cells_ : std::array<Cell, Cap>                       // cada Cell tiene secuencia (anti-ABA)
  - alignas(64) enqueue_pos_ : std::atomic<std::size_t>
  - alignas(64) dequeue_pos_ : std::atomic<std::size_t>
  + bool try_push(T);   + std::optional<T> try_pop();    // CAS + contador de versión → sin ABA
  // Validado con ThreadSanitizer + estrés aleatorio en CI (matriz §8.1).

----- cross_core.hpp / cross_core.cpp -------------------------------------------
**CrossCoreMailbox**  [CROSS-CORE]  — N buzones SPSC entrantes hacia un reactor + wake.
  - inboxes_ : std::vector<SpscQueue<Message>>           // uno por reactor origen
  - proactor_: Proactor&                                 // para wake() del destino
  + void post(int from_core, Message msg);               // SPSC push + wake
  + int  drain(const std::function<void(Message&)>& handler);
  **Message** { int target_core; std::move_only_function<void()> work; };

----- allocator.hpp / allocator.cpp ---------------------------------------------
**ArenaAllocator**  [REACTOR-LOCAL]  — arena por núcleo; toda gestión cruda confinada aquí.
  - upstream_ : std::pmr::memory_resource*               // NUMA-aware (nodo del core)
  - arena_    : std::pmr::monotonic_buffer_resource
  + std::pmr::memory_resource* resource() noexcept;
  + void  reset() noexcept;                              // libera todo de golpe
  + template<class T, class... A> T* make(A&&...);       // placement new confinado + RAII

----- reactor.hpp / reactor.cpp -------------------------------------------------
**Reactor**  [REACTOR-LOCAL]  — dueño de proactor, scheduler, allocator, particiones.
  - core_id_   : int
  - proactor_  : std::unique_ptr<Proactor>
  - sched_     : CoroScheduler
  - alloc_     : ArenaAllocator
  - mailbox_   : CrossCoreMailbox
  - partitions_: std::vector<Partition*>                 // observadores; el broker las posee
  - stopping_  : std::atomic<bool>
  + explicit Reactor(int core_id, std::unique_ptr<Proactor>);
  + void run();                                          // bucle: completions → ready → mailbox
  + void spawn(task<void> coro);                         // programa en ESTE reactor
  + void submit_to(int core_id, std::move_only_function<void()> work);   // a OTRO reactor
  + void stop() noexcept;                                // apagado limpio (drena)
  + int  core_id() const noexcept;
  + ArenaAllocator& allocator() noexcept;
  + Proactor& proactor() noexcept;
  - bool poll_once();                                    // un giro del bucle

----- reactor_pool.hpp / reactor_pool.cpp ---------------------------------------
**ReactorPool**  [THREAD-SAFE]  — crea N reactores (uno por core), *pinned*, y los une.
  - reactors_ : std::vector<std::unique_ptr<Reactor>>
  - threads_  : std::vector<std::jthread>
  + void     start(int num_reactors);                    // afinidad por pthread_setaffinity_np
  + void     shutdown();                                 // stop() a todos + join (graceful)
  + Reactor& reactor_for(PartitionId) const;             // asignación partición → núcleo
  + Reactor& reactor(int core_id) const;
  + int      size() const noexcept;

================================================================================
4.4  nexus-storage   (src/storage/)   — Fase 1   — deps: common, io
================================================================================
Motor de log append-only (núcleo de aprendizaje). I/O bloqueante en F1 → io_uring en F1b.

----- varint.hpp ----------------------------------------------------------------
  + std::size_t put_varint(std::uint64_t, MutByteSpan);   + expected<std::uint64_t> get_varint(ByteSpan&);
  + std::uint64_t zigzag_encode(std::int64_t);            + std::int64_t zigzag_decode(std::uint64_t);

----- record.hpp / record.cpp ---------------------------------------------------
**RecordBatch**  [INMUTABLE en disco]  — unidad de escritura/replicación (§5.4).
  + base_offset    : Offset
  + length         : std::int32_t
  + crc            : Crc                                  // CRC32C del resto del batch
  + attrs          : std::uint16_t                        // codec + bits txn/idempotente
  + producer_id    : ProducerId
  + producer_epoch : std::int16_t
  + base_sequence  : Sequence
  + record_count   : std::int32_t
  + records        : ByteSpan                              // bloque (posible comprimido)
  + void                     encode(Buffer& out) const;
  + static expected<RecordBatch> decode(ByteSpan in);
  + bool                     verify_crc() const noexcept;
  + Offset                   last_offset() const noexcept; // base_offset + count - 1
  + Codec                    codec() const noexcept;

**Record**  — `length_delta:varint, attrs:i8, timestamp_delta:varint, offset_delta:varint,
                key:bytes?, value:bytes?, headers[]` (deltas zigzag).

----- record_batch_builder.hpp / .cpp -------------------------------------------
**RecordBatchBuilder**  [REACTOR-LOCAL]  — construye un batch (deltas, compresión, CRC).
  - records_ : Buffer
  - count_   : std::int32_t
  + void           add(ByteSpan key, ByteSpan value, std::span<const Header> = {});
  + RecordBatch    build(Offset base_offset, Codec, ProducerId, Sequence);   // calcula CRC
  + std::size_t    estimated_size() const noexcept;
  + void           reset();

----- index.hpp / index.cpp -----------------------------------------------------
**IndexEntry**  [INMUTABLE]  — `relative_offset:u32`, `file_position:u32`.
**SparseIndex**  [REACTOR-LOCAL]  — índice disperso de un segmento (`.index`, mmap).
  - base_offset_ : Offset
  - entries_     : std::span<const IndexEntry>           // sobre el mmap
  - bytes_since_ : std::size_t                            // para sembrar cada N bytes
  + static expected<SparseIndex> open(const std::filesystem::path&, Offset base);
  + void       maybe_append(Offset, std::uint32_t file_pos, std::size_t batch_len);
  + IndexEntry floor(Offset) const;                      // mayor entry ≤ offset (búsqueda binaria)
  + void       flush();                                  // persiste y sella

----- segment.hpp / segment.cpp -------------------------------------------------
**Segment**  [REACTOR-LOCAL]  — un tramo del log (`.log` + `.index`).
  - base_offset_ : Offset
  - log_         : File                                  // .log
  - index_       : SparseIndex                           // .index
  - size_bytes_  : std::size_t
  - max_ts_      : WallTime
  - state_       : enum class State { Active, Closed }
  + static expected<Segment> create(const std::filesystem::path& dir, Offset base);
  + static expected<Segment> open  (const std::filesystem::path& dir, Offset base);
  + expected<Offset>      append(const RecordBatch&);    // escribe + indexa; actualiza size_
  + task<expected<FetchResult>> read(Offset, std::size_t max_bytes, Proactor&);
  + expected<Offset>      recover();                     // valida CRC, devuelve último offset válido
  + void                  seal();                        // estado Closed; flush índice
  + bool                  is_full(std::size_t segment_bytes) const noexcept;
  + Offset                base_offset() const noexcept;

----- partition_log.hpp / partition_log.cpp -------------------------------------
**PartitionLog**  [REACTOR-LOCAL]  — secuencia de segmentos de una partición.
  - dir_              : std::filesystem::path
  - segments_         : std::vector<std::unique_ptr<Segment>>   // ordenados por base_offset
  - active_           : Segment*                          // observador no propietario
  - log_start_offset_ : Offset
  - log_end_offset_   : Offset
  - recovery_point_   : Offset                            // último fsync confirmado
  - cfg_              : const LogConfig&
  + static expected<PartitionLog> open(std::filesystem::path dir, const LogConfig&);
  + expected<Offset>            append(const RecordBatch&);          // rota si is_full (§7.11 #1)
  + task<expected<FetchResult>> read(Offset, std::size_t max_bytes, Proactor&);  // §7.11 #3
  + expected<void>             recover();                            // trunca cola torn (§7.11 #2)
  + task<expected<void>>       fsync(Proactor&, FsyncPolicy);        // asíncrono bajo reactor
  + void                       enforce_retention(const RetentionPolicy&, WallTime now);
  + Offset                     log_end_offset() const noexcept;
  + Offset                     log_start_offset() const noexcept;
  - expected<void>             roll_segment();                       // sella activo, crea nuevo
  - Segment*                   segment_for(Offset) const;            // búsqueda binaria

**LogConfig**  [INMUTABLE]  — `segment_bytes, segment_ms, index_interval_bytes, fsync_policy`.

----- retention.hpp / retention.cpp ---------------------------------------------
**RetentionPolicy**  [INMUTABLE]  — por tiempo/tamaño.
  + retention_ms : Duration
  + retention_bytes : std::int64_t
  + bool eligible(const Segment&, std::size_t total_bytes, WallTime now) const;
**LogCompactor**  [REACTOR-LOCAL]  — compactación por clave (último valor por clave).
  + task<expected<void>> compact(PartitionLog&, Proactor&);

----- recovery.hpp / recovery.cpp -----------------------------------------------
  + expected<PartitionLog> recover_partition(const std::filesystem::path& dir, const LogConfig&);
  // orquesta: descubre segmentos → open → recover() del último → fija log_end_offset.

================================================================================
4.5  nexus-protocol   (src/protocol/)   — Fase 1b   — deps: common
================================================================================
Protocolo binario propio (ADR-0004): framing, codec, mensajes, errores, compresión, créditos.

----- codec.hpp -----------------------------------------------------------------
Helpers de (de)serialización, todos con chequeo de límites (decodificador defensivo):
  + class Encoder { + void put_u8/u16/u32/i64(...); + void put_varint(...); + void put_bytes(ByteSpan);
                    + void put_string(std::string_view); - Buffer& out_; };
  + class Decoder { + expected<std::uint32_t> get_u32(); + expected<std::uint64_t> get_varint();
                    + expected<ByteSpan> get_bytes(); + expected<std::string_view> get_string();
                    - ByteSpan in_; - std::size_t pos_; };   // nunca lee fuera de in_

----- frame.hpp / frame.cpp -----------------------------------------------------
**FrameHeader**  [INMUTABLE]  — `length:u32 | api_key:u16 | api_version:u16 |
                                  correlation_id:u32 | flags:u16` (§7.2).
  + ApiKey       api_key;     + std::uint16_t api_version;
  + std::uint32_t correlation_id;  + std::uint16_t flags;   + std::uint32_t length;
  + void                       encode(Encoder&) const;
  + static expected<FrameHeader> decode(Decoder&);
  + bool has_credit_update() const noexcept;               // bit en flags

**FrameReader**  [REACTOR-LOCAL]  — lee frames longitud-prefijo de un Socket/TlsConnection.
  - sock_ : Socket&   - buf_ : Buffer
  + task<expected<Frame>> read_frame(Proactor&, std::size_t max_frame);   // valida length
**FrameWriter**  [REACTOR-LOCAL]
  + task<expected<void>> write_frame(Proactor&, const FrameHeader&, ByteSpan payload);

  + enum class ApiKey : std::uint16_t { ApiVersions, Metadata, Produce, Fetch,
        OffsetCommit, OffsetFetch, JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
        CreateTopic, DeleteTopic };

----- messages.hpp / messages.cpp -----------------------------------------------
Structs request/response por operación (§7.2.1), cada uno con `encode(Encoder&)` /
`static decode(Decoder&) -> expected<...>`:
  **ApiVersionsRequest** { client_version }            → **ApiVersionsResponse** { ranges[] }
  **MetadataRequest**    { topics[]? }                 → **MetadataResponse** { brokers[], topics[] }
  **ProduceRequest**     { topic, partition, acks, batch }
                         → **ProduceResponse** { base_offset, error_code, throttle_ms }
  **FetchRequest**       { topic, partition, fetch_offset, max_bytes, min_bytes, max_wait_ms }
                         → **FetchResponse** { batches, high_watermark, log_start_offset, error_code }
  **OffsetCommitRequest/Response**, **OffsetFetchRequest/Response**
  **JoinGroupRequest/Response**, **SyncGroupRequest/Response**, **HeartbeatRequest/Response**, **LeaveGroup…**
  **CreateTopicRequest/Response**, **DeleteTopicRequest/Response**
  // Sub-structs: BrokerMeta{node_id,host,port}, PartitionMeta{id,leader_node_id,replicas[],leader_epoch}.

----- error_code.hpp ------------------------------------------------------------
**WireError**  [INMUTABLE]  — códigos de error del protocolo (§7.2.2):
  + enum class WireError : std::int16_t { None=0, NotLeaderForPartition, LeaderNotAvailable,
        UnknownTopicOrPartition, OffsetOutOfRange, NotEnoughReplicas, RequestTimedOut,
        CorruptMessage, MessageTooLarge, OutOfOrderSequence, DuplicateSequence, Throttled,
        RebalanceInProgress, UnsupportedVersion, Unauthorized, InvalidRequest };
  + bool      is_retryable(WireError) noexcept;
  + WireError from_error(const Error&) noexcept;           // traducción interna → wire (en el borde)

----- versioning.hpp ------------------------------------------------------------
  + struct ApiVersionRange { ApiKey key; std::uint16_t min; std::uint16_t max; };
  + std::uint16_t negotiate(ApiKey, std::uint16_t client_max, const ApiVersionRange& server);

----- compression.hpp / compression.cpp -----------------------------------------
**Compressor**  [THREAD-SAFE]  — none/LZ4/Zstd por batch, con guarda anti *decompression bomb*.
  + Buffer            compress(Codec, ByteSpan in);
  + expected<Buffer>  decompress(Codec, ByteSpan in, std::size_t max_out_bytes);
  // rechaza si el tamaño descomprimido o el ratio superan el límite (Error{InvalidArgument}).

----- credits.hpp ---------------------------------------------------------------
**CreditWindow**  [REACTOR-LOCAL]  — control de flujo por créditos (§6.3 / §7.11 #4).
  - credits_   : std::int32_t
  - waiter_    : std::coroutine_handle<>                  // corrutina frenada esperando crédito
  + task<void> acquire(std::int32_t cost);                // se SUSPENDE si credits_ < cost
  + void       grant(std::int32_t n);                     // reanuda al waiter si lo había
  + std::int32_t available() const noexcept;

================================================================================
4.6  nexus-consensus   (src/consensus/)   — Fase 2   — deps: common, io, protocol, storage, telemetry
================================================================================
Raft por partición (ADR-0003): un grupo Raft por partición; el log de Raft ES el log.

----- raft_state.hpp ------------------------------------------------------------
**RaftPersistentState**  [REACTOR-LOCAL]  — en disco (se persiste con fsync antes de responder).
  + current_term : Term
  + voted_for    : std::optional<NodeId>
  + // el log se delega a RaftLog
  + expected<void> persist(File&) const;   + static expected<RaftPersistentState> load(File&);
**RaftVolatileState**  — `commit_index:Index, last_applied:Index`;
                         líder: `next_index : std::unordered_map<NodeId,Index>`, `match_index : …`.
**RaftLogEntry**  [INMUTABLE]  — `term:Term, index:Index, type:{Data,Config}, payload:ByteSpan`.
**Snapshot**  [INMUTABLE]  — `last_included_index, last_included_term, state:Buffer`.

----- raft_log.hpp / raft_log.cpp -----------------------------------------------
**RaftLog**  [REACTOR-LOCAL]  — vista (term,index) sobre el PartitionLog subyacente.
  - log_ : PartitionLog&
  + expected<Index>  append(std::span<const RaftLogEntry>);
  + expected<void>   truncate_from(Index);                // ante conflicto en AppendEntries
  + expected<Term>   term_at(Index) const;
  + Index            last_index() const noexcept;
  + Term             last_term() const noexcept;
  + std::vector<RaftLogEntry> entries_from(Index, std::size_t max) const;

----- raft_rpc.hpp --------------------------------------------------------------
  **RequestVoteArgs** { term, candidate_id, last_log_index, last_log_term, pre_vote:bool }
  **RequestVoteReply** { term, vote_granted }
  **AppendEntriesArgs** { term, leader_id, prev_log_index, prev_log_term, entries[], leader_commit, leader_epoch }
  **AppendEntriesReply** { term, success, conflict_index }
  **InstallSnapshotArgs/Reply** { term, leader_id, snapshot }   // todos encode/decode sobre el codec.

----- raft_node.hpp / raft_node.cpp ---------------------------------------------
**RaftNode**  [REACTOR-LOCAL]  — máquina de estados de una réplica de partición.
  - role_            : enum class Role { Follower, Candidate, Leader }
  - persistent_      : RaftPersistentState
  - volatile_        : RaftVolatileState
  - log_             : RaftLog
  - self_            : NodeId
  - peers_           : std::vector<NodeId>
  - leader_epoch_    : Epoch
  - election_deadline_ : MonoTime                          // aleatorizado (reloj monotónico)
  - transport_       : RaftTransport&                      // envío de RPC (inyectado)
  + task<expected<Index>>  propose(const RecordBatch&);    // solo líder (§7.11 #1)
  + AppendEntriesReply     on_append_entries(const AppendEntriesArgs&);   // §7.11 #5
  + RequestVoteReply       on_request_vote(const RequestVoteArgs&);
  + void                   on_install_snapshot(const InstallSnapshotArgs&);
  + void                   tick(MonoTime now);             // vence election/heartbeat
  + Index                  commit_index() const noexcept;
  + Term                   current_term() const noexcept;
  + bool                   is_leader() const noexcept;
  + Epoch                  leader_epoch() const noexcept;
  + std::optional<NodeId>  leader_hint() const noexcept;
  - void  become_follower(Term);  - void become_candidate();  - void become_leader();
  - void  start_election(bool pre_vote);
  - void  advance_commit_index();                          // mayoría de match_index
  - task<void> replicate_to(NodeId);                       // AppendEntries a un peer
  - void  apply_committed();                               // aplica [last_applied+1, commit_index]

**RaftTransport**  [REACTOR-LOCAL]  — puerto abstracto de envío de RPC entre nodos.
  + virtual task<expected<AppendEntriesReply>> send_append(NodeId, const AppendEntriesArgs&) = 0;
  + virtual task<expected<RequestVoteReply>>   send_vote(NodeId, const RequestVoteArgs&) = 0;

----- election.hpp / election.cpp -----------------------------------------------
  **PreVote** — evita disrupción por nodos que se reincorporan.
  **LeadershipTransfer** — `task<expected<void>> transfer(RaftNode&, NodeId target)`.
  **Learner** — réplica que replica sin votar hasta ponerse al día.

================================================================================
4.7  nexus-broker   (src/broker/)   — Fase 1b–2   — deps: common, protocol, reactor, storage,
                                                            consensus, telemetry
================================================================================
El núcleo del broker: topics, particiones, grupos, offsets, idempotencia, backpressure.

----- topic.hpp / topic.cpp -----------------------------------------------------
**TopicMetadata**  [INMUTABLE]  — `name, partition_count, replication_factor,
        config{retention_ms, retention_bytes, segment_bytes, compaction, compression}, created_at`.
**Topic**  [REACTOR-LOCAL por partición]  — agrupa las particiones de un topic.
  - meta_       : TopicMetadata
  - partitions_ : std::unordered_map<PartitionId, std::unique_ptr<Partition>>
  + Partition*  partition(PartitionId) noexcept;
  + const TopicMetadata& meta() const noexcept;

----- partition_state.hpp -------------------------------------------------------
**PartitionState**  [INMUTABLE/observable]  — `topic, partition_id, leader_epoch,
        leader_node_id, replica_node_ids[], high_watermark, log_start_offset,
        log_end_offset, owner_reactor_id`.

----- producer_session.hpp ------------------------------------------------------
**ProducerSession**  [REACTOR-LOCAL]  — idempotencia por (producer_id, partición) (§5.9).
  - producer_id    : ProducerId
  - epoch          : std::int16_t
  - last_sequence_ : Sequence
  + enum class SeqCheck { Accept, Duplicate, Gap };
  + SeqCheck check(Sequence base_seq, std::int32_t count) noexcept;   // expected/dup/gap
  + void     advance(Sequence next) noexcept;

----- partition.hpp / partition.cpp ---------------------------------------------
**Partition**  [REACTOR-LOCAL]  — une PartitionLog + RaftNode; unidad de serialización.
  - log_       : PartitionLog
  - raft_      : RaftNode                                 // solo Fase 2; en 1b, ack local
  - state_     : PartitionState
  - producers_ : std::unordered_map<ProducerId, ProducerSession>
  - reactor_   : Reactor&
  + task<expected<Offset>> produce(const RecordBatch&, Acks);   // HOT PATH (§7.11 #1):
        // valida CRC/tamaño → idempotencia → raft.propose (o append local) → según acks
  + task<expected<FetchResult>> fetch(Offset, std::size_t max_bytes);   // hasta high_watermark
  + Offset  high_watermark() const noexcept;             // = raft.commit_index (Fase 2)
  + bool    is_leader() const noexcept;
  + Epoch   leader_epoch() const noexcept;
  + void    on_raft_committed(Index);                    // avanza high_watermark, despierta fetchers
  - WireError map_seq_check(ProducerSession::SeqCheck) noexcept;

----- produce_handler.hpp / fetch_handler.hpp / .cpp ----------------------------
**ProduceHandler / FetchHandler**  [REACTOR-LOCAL]  — corrutinas: mensaje de wire ↔ Partition.
  + task<ProduceResponse> handle(const ProduceRequest&, ConnectionState&);
  + task<FetchResponse>   handle(const FetchRequest&, ConnectionState&);   // long-poll (min_bytes/max_wait)

----- consumer_group.hpp / consumer_group.cpp -----------------------------------
**ConsumerGroup**  [REACTOR-LOCAL]  — `group_id, generation_id, state, members[], leader_member_id`.
  - state_   : enum class State { Empty, PreparingRebalance, CompletingRebalance, Stable, Dead }
  - members_ : std::unordered_map<std::string, Member>   // memberId → {clientId, assignment}
  + JoinGroupResponse  join(const JoinGroupRequest&);
  + SyncGroupResponse  sync(const SyncGroupRequest&);    // el líder reparte; los demás reciben
  + expected<void>     heartbeat(std::string_view member_id);
  + void               leave(std::string_view member_id);
  - void               trigger_rebalance();              // incrementa generation_id

----- offset_manager.hpp / offset_manager.cpp -----------------------------------
**OffsetManager**  [REACTOR-LOCAL]  — almacena commits de offset de los grupos.
  - commits_ : std::unordered_map<OffsetKey, OffsetCommit>   // {group,topic,partition} → offset
  + expected<void>    commit(const OffsetCommit&);
  + expected<Offset>  fetch(std::string_view group, std::string_view topic, PartitionId);
**OffsetCommit**  [INMUTABLE]  — `group_id, topic, partition, committed_offset, metadata, commit_ts`.

----- topic_manager.hpp / topic_manager.cpp -------------------------------------
**TopicManager**  [THREAD-SAFE / plano de control]  — crea/borra topics, asigna particiones.
  - topics_ : std::unordered_map<std::string, std::shared_ptr<Topic>>   // protegido por mutex
  - pool_   : ReactorPool&
  + expected<TopicMetadata>      create_topic(const CreateTopicSpec&);   // asigna particiones a cores
  + expected<void>               delete_topic(std::string_view name);
  + expected<TopicDescription>   describe(std::string_view name) const;
  + Metadata                     metadata() const;                       // para MetadataResponse
  + expected<void>               reassign_partitions(const ReassignPlan&);
  + std::shared_ptr<Topic>       get(std::string_view name) const;

----- backpressure.hpp / dlq.hpp ------------------------------------------------
**BackpressureController**  [REACTOR-LOCAL]  — créditos por conexión/topic; defensa ante sobrecarga.
**DeadLetterQueue**  [REACTOR-LOCAL]  — destino de mensajes no procesables tras N intentos.

================================================================================
4.8  nexus-ingress   (src/ingress/)   — Fase 3   — deps: common, io, wire, cluster
================================================================================
Ingress en dos modos (ADR-0006) + gateway REST de administración (§7.6).

----- tls.hpp / tls.cpp ---------------------------------------------------------
**TlsContext**  [THREAD-SAFE]  — RAII sobre `SSL_CTX` (OpenSSL); TLS 1.3; carga certs/CA.
  - ctx_ : SSL_CTX*                                       // SSL_CTX_free en destructor
  + static expected<TlsContext> server(cert, key, ca_for_mtls = {});
  + bool require_client_cert() const noexcept;            // mTLS intra-cluster
**TlsConnection**  [REACTOR-LOCAL]  — RAII sobre `SSL`; handshake y E/S cifrada sobre Socket.
  - ssl_ : SSL*    - sock_ : Socket
  + task<expected<void>>        handshake(Proactor&);
  + task<expected<std::size_t>> async_recv(Proactor&, MutByteSpan);
  + task<expected<std::size_t>> async_send(Proactor&, ByteSpan);
  + std::optional<std::string>  peer_principal() const;   // del cert (mTLS/authz)

----- rate_limiter.hpp / rate_limiter.cpp ---------------------------------------
**TokenBucket**  [REACTOR-LOCAL]  — limitación por cliente/topic.
  - tokens_ : double  - rate_ : double  - cap_ : double  - last_ : MonoTime
  + bool allow(double cost = 1.0);                       // refill perezoso + consumo
  + void configure(double rate, double burst);

----- circuit_breaker.hpp / circuit_breaker.cpp ---------------------------------
**CircuitBreaker**  [REACTOR-LOCAL]  — estabilidad (Nygard).
  - state_ : enum class State { Closed, Open, HalfOpen }
  - window_ : SlidingWindow                              // tasa de error reciente
  + bool allow() noexcept;                               // false si Open
  + void on_success() noexcept;   + void on_failure() noexcept;

----- load_balancer.hpp / load_balancer.cpp -------------------------------------
**LoadBalancer**  [REACTOR-LOCAL]  — round-robin / least-connections / consistent-hashing.
  + NodeId pick(const Request&) const;                   // por partition key en consistent-hash

----- health_check.hpp / health_check.cpp ---------------------------------------
**HealthChecker**  [REACTOR-LOCAL]  — activo (ping periódico) + pasivo (errores reales).
  + void  observe(NodeId, bool ok);   + bool healthy(NodeId) const;

----- rest_gateway.hpp / rest_gateway.cpp ---------------------------------------
**RestGateway**  [REACTOR-LOCAL]  — servidor HTTP del REST admin (§7.6).
  - admin_   : AdminApi&
  - metrics_ : MetricsRegistry&
  - authz_   : JwtVerifier
  + task<HttpResponse> route(const HttpRequest&);        // /api/v1/...
  - HttpResponse       to_problem_detail(const Error&, int status) const;  // RFC 7807
  - expected<Page>     parse_pagination(const HttpRequest&) const;         // page/size
  - expected<Principal> authenticate(const HttpRequest&) const;            // Bearer JWT
  // Contrato publicado en docs/openapi.yaml.

----- proxy.hpp / proxy.cpp -----------------------------------------------------
**Proxy**  [REACTOR-LOCAL]  — modo proxy: enruta clientes "tontos" al líder (consistent-hash).
  + task<void> forward(Connection& client);

----- connection_state.hpp ------------------------------------------------------
**ConnectionState**  [REACTOR-LOCAL]  — `conn_id, negotiated_versions, auth_principal?,
        credits:CreditWindow, inflight: std::unordered_map<std::uint32_t, ...>`.

================================================================================
4.9  nexus-server   (src/server/)   — lib + nexusd (exe) — Fase 1b+   — deps: common, io, reactor, wire, broker, cluster, ingress, kafka, telemetry
================================================================================
Daemon del broker: orquesta reactores, conexiones, admin y observabilidad.

----- main.cpp ------------------------------------------------------------------
  + int main(int argc, char** argv);
  - expected<void> bootstrap(const Config&);
      // 1) Logger::global() ← config   2) ReactorPool.start(num_reactors)
      // 3) recover_partition(...) por partición asignada   4) Listener.bind + accept loop
      // 5) install_signal_handlers(SIGTERM/SIGINT → eventfd → Server::shutdown)
      // 6) pool.run() (bloquea hasta shutdown)
  - void install_signal_handlers(Server&);                // solo async-signal-safe en el handler

----- server.hpp / server.cpp ---------------------------------------------------
**Server**  [THREAD-SAFE]  — cohesiona ReactorPool + TopicManager + Ingress + AdminApi + Metrics.
  - pool_     : ReactorPool
  - topics_   : TopicManager
  - ingress_  : std::optional<Ingress>
  - admin_    : AdminApi
  - metrics_  : MetricsRegistry
  + expected<void> start();
  + void           shutdown();                            // drena conexiones, fsync final,
                                                          // transfiere liderazgos, une hilos
  + RequestRouter& router() noexcept;

----- connection.hpp / connection.cpp -------------------------------------------
**Connection**  [REACTOR-LOCAL]  — una conexión de cliente, corrutina dedicada.
  - sock_   : std::variant<Socket, TlsConnection>
  - state_  : ConnectionState
  - reader_ : FrameReader   - writer_ : FrameWriter
  - router_ : RequestRouter&
  + task<void> serve(Proactor&);                         // bucle: read_frame → dispatch → write
  - task<void> dispatch(const Frame&);                   // por api_key; respeta créditos
  - task<void> send_response(std::uint32_t corr_id, ByteSpan payload);

----- request_router.hpp / request_router.cpp -----------------------------------
**RequestRouter**  [THREAD-SAFE]  — mapea api_key → handler y enruta a la partición dueña.
  - server_ : Server&
  + task<Frame> route(const Frame&, ConnectionState&);
  - Reactor&    owner_of(const std::string& topic, PartitionId);   // cross-core si toca
  // si la partición vive en otro core → Reactor::submit_to(owner, work) y espera el resultado.

----- admin_api.hpp / admin_api.cpp ---------------------------------------------
**AdminApi**  [THREAD-SAFE]  — lógica de los endpoints REST sobre TopicManager.
  + expected<TopicMetadata>    create_topic(const CreateTopicSpec&);
  + expected<void>             delete_topic(std::string_view);
  + expected<TopicDescription> describe_topic(std::string_view);
  + std::vector<GroupSummary>  list_groups(Page) const;
  + expected<void>             reassign(const ReassignPlan&);

----- metrics.hpp / metrics.cpp -------------------------------------------------
**MetricsRegistry**  [THREAD-SAFE]  — contadores/gauges/histogramas; exposición Prometheus.
  + Counter&   counter(std::string_view name, Labels = {});
  + Gauge&     gauge(std::string_view name, Labels = {});
  + Histogram& histogram(std::string_view name, Labels = {});
  + std::string render_prometheus() const;               // texto para /metrics
  // métricas clave: produce/fetch rate, consumer lag, raft term/leader/commit_index, latencias.

================================================================================
4.10  nexus-client   (src/client/)   — lib — Fase 1b   — deps: common, io, protocol
================================================================================
Librería cliente nativa (smart-client). NO depende de broker/consensus.

----- client.hpp / client.cpp ---------------------------------------------------
**Client**  [THREAD-SAFE]  — punto de entrada; gestiona metadata y conexiones.
  - metadata_ : MetadataCache    - pool_ : ConnectionPool    - reactor_ : Reactor
  + static expected<Client> connect(std::span<const Endpoint> seeds, ClientConfig);
  + Producer producer(ProducerConfig = {});
  + Consumer consumer(std::string group, ConsumerConfig = {});
  + task<expected<Metadata>> refresh_metadata();

----- metadata_cache.hpp / metadata_cache.cpp -----------------------------------
**MetadataCache**  [THREAD-SAFE]  — `topic/partition → leader`; refresco bajo NotLeader.
  - map_ : std::unordered_map<TopicPartition, NodeId>
  + expected<NodeId> leader_for(std::string_view topic, PartitionId) const;
  + void             invalidate(std::string_view topic);
  + task<expected<void>> refresh(ConnectionPool&);

----- connection_pool.hpp / connection_pool.cpp ---------------------------------
**ConnectionPool**  [THREAD-SAFE]  — reutiliza conexiones TCP+TLS (no una por petición).
  - conns_ : std::unordered_map<NodeId, std::vector<PooledConn>>
  + task<expected<PooledConn>> acquire(NodeId);
  + void                       release(PooledConn);

----- producer.hpp / producer.cpp -----------------------------------------------
**Producer**  [THREAD-SAFE]  — publica con idempotencia y créditos.
  - client_ : Client&    - session_ : ProducerSession    - builder_ : RecordBatchBuilder
  + task<expected<Offset>> send(std::string topic, std::optional<PartitionId>,
                                ByteSpan key, ByteSpan value, Acks = Acks::Quorum);
  + task<expected<void>>   flush();
  // reintentos con backoff+jitter solo sobre ops idempotentes; refresca metadata ante NotLeader.

----- consumer.hpp / consumer.cpp -----------------------------------------------
**Consumer**  [THREAD-SAFE]  — consume por grupo, con commit de offset.
  - client_ : Client&    - group_ : std::string    - assignment_ : std::vector<TopicPartition>
  + task<expected<void>>           subscribe(std::span<const std::string> topics);
  + task<expected<std::vector<Record>>> poll(Duration timeout);
  + task<expected<void>>           commit(std::span<const OffsetCommit>);
  + task<expected<void>>           close();
  // membresía: JoinGroup/SyncGroup/Heartbeat; rebalanceo cooperativo.

----- bindings/python/module.cpp  (stretch) -------------------------------------
  + PYBIND11_MODULE(nexusmq, m)  — expone Client/Producer/Consumer a Python.

================================================================================
4.11  nexus-cli   (tools/cli/)   — exe — Fase 3   — deps: common, protocol, client
================================================================================
CLI de administración `nexus-cli` (habla REST admin o el protocolo).

  main.cpp                 + int main(...)  — dispatch de subcomandos (CLI11/propio).
  topic_commands.cpp       + run_topic(args)      — create | list | describe | delete
  group_commands.cpp       + run_group(args)      — list | describe (lag)
  partition_commands.cpp   + run_partitions(args)  — reassign (con clave de idempotencia)
  admin_commands.cpp       + run_admin(args)       — diagnostics | metrics
  // cada comando: parsea → llama AdminApi/REST/cliente → imprime tabla/JSON.

================================================================================
4.12  nexus-bench   (tools/bench/)   — exe — Fase 1   — deps: common, protocol, client
================================================================================
Generador de carga open-loop + histograma (metodología anti coordinated-omission, §8.2).

----- main.cpp ------------------------------------------------------------------
  + int main(...)  — parsea BenchConfig, ejecuta, imprime informe (p50/p99/p999/max).

----- bench_config.hpp ----------------------------------------------------------
**BenchConfig**  — `target_rate (msg/s), batch_size, acks, payload_size, duration, warmup,
                    producers, topic`.

----- load_generator.hpp / load_generator.cpp -----------------------------------
**LoadGenerator**  [THREAD-SAFE]  — emite a **tasa fija independiente de las respuestas**.
  - client_ : Client&   - hist_ : LatencyHistogram
  + BenchReport run(const BenchConfig&);                 // open-loop: programa por reloj, no por ack
  // registra la latencia esperada-vs-real (corrige coordinated omission).

----- latency_histogram.hpp / latency_histogram.cpp -----------------------------
**LatencyHistogram**  [THREAD-SAFE]  — estilo HdrHistogram (resolución alta, rango amplio).
  + void           record(std::int64_t ns);
  + void           record_corrected(std::int64_t ns, std::int64_t expected_interval);
  + std::int64_t   percentile(double p) const;           // p50/p99/p999
  + std::int64_t   max() const;
  + void           merge(const LatencyHistogram&);

================================================================================
4.13  nexus-tests   (tests/)   — Fase 1+   — deps: (todos) + GoogleTest
================================================================================
Matriz de testing (§8.1). Nombres `Metodo_Escenario_ResultadoEsperado`; TDD (rojo→verde→
refactor); sanitizers ASan/UBSan/TSan en CI; el no-determinismo se INYECTA, no se tolera.

tests/
├── unit/         — record, record_batch_builder, sparse_index, crc32c, codec, varint,
│                   raft_node (transiciones), token_bucket, circuit_breaker, credit_window…
├── property/     — round-trip encode/decode (RecordBatch, frames, mensajes);
│                   invariantes del log (offsets monótonos) y de Raft (un líder por término).
├── integration/  — e2e mono-nodo: producer → broker → consumer; offsets y acks.
├── crash/        — kill -9 a mitad de append → recover(): valida CRC, trunca cola torn.
├── chaos/        — tc netem (partición) → failover; verifica postura CP (no divergencia).
├── sim/          — DeterministicRaftTest: N RaftNode sobre VirtualClock + VirtualNetwork;
│                   reproduce elecciones, splits, pérdidas de mensajes de forma repetible.
├── fuzz/         — libFuzzer sobre Decoder/decode de RecordBatch y frames (entrada hostil).
└── support/
    **VirtualClock**   [test]  — reloj inyectable: + void advance(Duration); + MonoTime now();
    **VirtualNetwork** [test]  — entrega/retrasa/pierde/particiona mensajes de forma determinista.
    **InMemoryFile**   [test]  — File en memoria para tests de storage sin disco.
    **FakeProactor**   [test]  — completions deterministas para tests del reactor.

================================================================================
4.15  nexus-telemetry   (src/telemetry/)   — Fase 3   — deps: common   (ADR-0017)
================================================================================
metrics.hpp / .cpp
  **Counter** — [THREAD-SAFE] `+ inc(n=1)` (atomic relaxed), `+ value()`.
  **Gauge**   — [THREAD-SAFE] `+ set(v)`, `+ inc(n)`, `+ dec(n)`.
  **Histogram** — [THREAD-SAFE] `+ observe(double)`; buckets configurables.
  **MetricsRegistry** — [THREAD-SAFE] registro de series; `+ describe(name, help)`,
    factorías de Counter/Gauge/Histogram, `+ render_prometheus() -> std::string` (texto 0.0.4).
logging.hpp / .cpp
  **LogLevel** `{Trace,Debug,Info,Warn,Error}`; **Field** (clave/valor estructurado).
  **Logger** — [THREAD-SAFE] log estructurado; `+ set_min_level`, `+ enabled(level)`,
    `+ add_context(Field)`, `+ log(level, msg, fields)` + helpers `trace/debug/info/warn/error`.
tracing.hpp / .cpp
  **TraceId/SpanId/SpanContext** (W3C; `valid()`, `sampled()`), **SpanData**.
  **IdGenerator** (interfaz) / **RandomIdGenerator**; **Span** (`set_attribute`, `sampled`);
  **Tracer** — `+ start_root(name, sampled)`, `+ start_child(parent, name)`,
    `+ start_from_remote(remote, name)` (contexto recibido de otro proceso).

================================================================================
4.16  nexus-cluster   (src/cluster/)   — Fase 2/4b   — deps: common, io, protocol, consensus   (ADR-0025)
================================================================================
Transporte inter-nodo de Raft: plano separado del de cliente.
peer_directory.hpp / .cpp
  **PeerAddress** (`node`, host, puerto). **PeerDirectory** — `+ contains(node)`, `+ empty()`,
    resolución `NodeId → PeerAddress`.
raft_transport.hpp / .cpp
  **RaftTransport : RaftMessageSink** — [REACTOR-LOCAL] envía `RaftEnvelope` por conexión saliente
    (un **PeerLink** por par); `+ send(const RaftEnvelope&) override`; `Spawner`/`Config` inyectados.
raft_receiver.hpp / .cpp
  - `+ serve_raft_connection(Proactor&, Socket, RaftEnvelopeHandler) -> task<void>`: lee sobres
    longitud-prefijo y los entrega al handler (rutea por `(topic, partition)`).

================================================================================
4.17  nexus-kafka   (src/kafka/)   — Fase 4   — deps: common   (ADR-0029)
================================================================================
Subset Kafka **puro** (sin E/S ni broker): codec big-endian por versión.
gateway.hpp / .cpp
  **KafkaBroker** — interfaz: `+ metadata/produce/fetch/list_offsets(req) -> task<...>` (virtual).
  **KafkaGateway** — `explicit KafkaGateway(KafkaBroker&)`; `+ handle_request(ByteSpan) ->
    task<expected<Buffer>>` (decodifica cabecera, despacha por ApiKey/versión, codifica respuesta).
messages.hpp / .cpp
  **ApiKey** `{Produce=0,Fetch=1,ListOffsets=2,Metadata=3,ApiVersions=18}`; cabeceras de
    petición/respuesta por versión; `is_flexible(api_key, version)`, `supported_apis()`.
codec.hpp / .cpp · produce.* · fetch.* · metadata.* · list_offsets.* · record_batch.* · error_code.hpp
  - Codecs por API con variante **clásica/flexible**; `Decoder::skip`; **RecordBatch v2** opaco
    (`peek_record_batch`/`set_base_offset`); `KafkaError:i16`. Contrato en `docs/kafka.md`.

================================================================================
4.18  nexus-ffi   (src/ffi/)   — Fase 4   — lib SHARED — deps: common, telemetry   (ADR-0020)
================================================================================
nexus_ffi.cpp  (extern "C": ABI estable para el binding Python por ctypes)
  - `const char* nexus_version()`.
  - `int nexus_traceparent_format(trace_hi, trace_lo, span_id, flags, char* out, ...)`.
  - `int nexus_traceparent_parse(const char* header, uint64_t* trace_hi, ...)` (W3C traceparent).
  Wrapper Python en `bindings/python/` (no requiere `python3-dev` para compilar el core).

================================================================================
4.14  deploy/   — Fase 3
================================================================================
deploy/docker/Dockerfile
  - FROM <toolchain> AS build  (vcpkg install + cmake --preset linux-clang + build)
  - FROM gcr.io/distroless/cc AS runtime   (copia SOLO el binario nexus-server)
  - USER nonroot:nonroot
  - HEALTHCHECK CMD ["/nexus-server","--health"]   # consulta /readyz
deploy/docker/docker-compose.yml
  - servicios nexus-1, nexus-2, nexus-3 (3 nodos) + prometheus + grafana;
    healthchecks + depends_on; volúmenes para data.dir.
deploy/k8s/
  - Deployment (réplicas + StatefulSet para data.dir), Service, ConfigMap/Secret,
    probes liveness (/healthz) y readiness (/readyz), límites CPU/mem por pod.
.dockerignore — excluye build/, .git/, secretos.

================================================================================
5.  BUILD — archivos raíz (detalle)
================================================================================
CMakeLists.txt (raíz)
  - cmake_minimum_required(VERSION 3.25); project(NexusMQ CXX);
  - target_compile_features(nexus_options INTERFACE cxx_std_23)   # ADR-0011
  - option(NEXUS_SANITIZERS "ASan/UBSan en pruebas" OFF) / option(NEXUS_TSAN ... OFF)
  - add_compile_options(-Wall -Wextra -Wpedantic -Werror)   # /W4 /WX en MSVC
  - I/O por plataforma:  if(WIN32) target_sources(nexus-io PRIVATE io/iocp_backend.cpp)
                         else()    target_sources(nexus-io PRIVATE io/io_uring_backend.cpp)
  - add_subdirectory por target (capas con target_link_libraries PUBLIC/PRIVATE)
  - set_target_properties(... FOLDER "nucleo|exe|cliente|pruebas")   # carpetas en el IDE
CMakePresets.json   — presets: linux-gcc · linux-clang · windows-msvc (cada uno con su toolchain).
vcpkg.json          — dependencies: liburing ("platform":"linux"), openssl, lz4, zstd, fmt,
                      gtest, benchmark, pybind11 (feature opcional "python").
.clang-format / .clang-tidy — estilo y chequeos (incluye Core Guidelines) versionados.

================================================================================
6.  MAPA FASE → TARGETS
================================================================================
Fase 1  : nexus-common, nexus-storage (I/O bloqueante), nexus-bench, nexus-tests(unit/property/crash).
Fase 1b : nexus-io (proactor), nexus-reactor, nexus-protocol, nexus-wire, nexus-broker (mono-nodo),
          nexus-client, nexus-server (mono-nodo). Tests: integration.
Fase 2  : nexus-consensus (Raft), nexus-cluster (transporte inter-nodo), broker distribuido
          (grupos, rebalanceo). Tests: sim/chaos.
Fase 3  : nexus-ingress (TLS, REST admin), nexus-telemetry, nexus-cli, observabilidad, deploy/.
Fase 4  : direct I/O, nexus-kafka (subset, §4.17), nexus-ffi (binding Python, §4.18), IOCP Windows.
Fase 4b : cierre — W (Windows: nexusd completo, ADR-0028), D (deuda diferida), L (nexus-loadgen).
          Estado as-built: Fases 1→4 implementadas.

================================================================================
FIN DEL DESGLOSE DETALLADO
================================================================================
> Proyección de diseño coherente con el anteproyecto v0.4.0. Las firmas concretas
> (tipos exactos, const/noexcept, parámetros) se afinan al implementar cada fase,
> bajo TDD y con la disciplina shared-nothing (afinidad anotada por tipo).
