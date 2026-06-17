# Hoja de ruta — NexusMQ

> **Plan de desarrollo vivo.** Se actualiza tras **cada paso**: se marcan `[x]` las tareas hechas y
> se añaden las descubiertas. Mapea las fases e hitos del anteproyecto a tareas granulares y a los
> *targets*/archivos del desglose.
>
> **Leyenda:** `[ ]` pendiente · `[~]` en curso · `[x]` hecho. Cada tarea referencia su *target*
> (`nexus-*`) y/o archivo del desglose. **Fasing estricto:** Fase 1 es **monohilo + I/O bloqueante**
> (cero reactor, cero io_uring); el reactor llega en 1b, Raft en 2.
>
> Fuentes: `anteproyecto.md` (§4.5 roadmap, §4.6 hitos Fase 1), `Desglose/nexusmqdesglose.md`
> (§6 mapa fase→targets), `Desglose/nexusmqdesglosedetallado.md` (firmas).

**Estado actual:** **FASE 3 EN CURSO** (Ingress + operación). Cerrada la **Fase 2** (C1–C12): sobre el broker *thread-per-core* de Fase 1b se
añade el **consenso Raft por partición** (`nexus-consensus`: estado/log/RPC, `RaftNode` como máquina de
estados síncrona sin E/S con pre-vote, replicación, *high-watermark* por mayoría, transferencia de
liderazgo y learners; ADR-0014/0015), la **integración en el broker** (`ReplicatedPartition`, ADR-0016),
los **grupos de consumidores** (FSM generacional + Join/Sync/Heartbeat/Leave + `GroupCoordinator`), el
**routing cross-core multi-reactor** (`call_on` + `PartitionRouter`) y un **arnés de simulación
determinista** que verifica elección, failover, partición y **chaos** (Leader Completeness,
anti-split-brain, reinicios rodantes) comprobando las invariantes de seguridad. **319 tests** verdes en
GCC/libstdc++, Clang/libc++, **ASan/UBSan** y **TSan**; CI verde. Siguiente fase: **Fase 3 — Ingress +
operación**.

> **Resumen de cierre de Fase 1b.** (histórico) Entregable: broker de un nodo *thread-per-core* que publica y consume con un cliente nativo, hablando el protocolo binario propio sobre io_uring. Targets nuevos/cerrados: `nexus-reactor`, `nexus-protocol`, `nexus-wire` (ADR-0013), `nexus-broker`, `nexus-client`, `nexus-server` (+ `Socket`/`Listener` async en `nexus-io`). Modelo de errores `expected<T>` en el núcleo y traducción wire↔núcleo en el borde (`from_error`/`to_error`); RAII; TDD; sin Raft. **Ajustes de diseño respecto al desglose (anotados por hito):** `task<T>` reubicado a `common/`; backend io_uring directo sobre el uapi (ADR-0012); framing en `nexus-wire` con protocolo puro (ADR-0013); broker **síncrono** en 1b (E/S de almacenamiento bloqueante; la versión async llega con Raft); `Connection`→corrutina libre `serve_connection`; cliente síncrono de un solo nodo; `CreditWindow` reubicado de protocol a broker. **Diferido a Fase 2 (decisiones de fasing, no deuda):** `File` async + ruta de almacenamiento async, *multi-reactor* con routing por partición, grupos de consumidores (Join/Sync/Heartbeat + rebalanceo), persistencia del topic de offsets, y el cableado end-to-end de créditos en la ruta de *push*.

---

## Andamiaje del repositorio (previo a M1)

- [x] Hoja de ruta (`DocumentacionProvisional/hoja-de-ruta.md`).
- [x] `README.md` (visión, estado, arquitectura, build, licencia).
- [x] `LICENSE.md` (PolyForm Noncommercial 1.0.0).
- [x] `CLAUDE.md` (contexto del proyecto + import de la normativa de la biblioteca).

---

## Fase 1 — Storage engine (I/O bloqueante, sin reactor)

> Entregable: motor de log monopartición con cifras de rendimiento.
> Targets: `nexus-common`, `nexus-storage`, `nexus-bench`, `nexus-tests` (unit/property/crash).

### M1 — Esqueleto (CMake + vcpkg + CI + primer test + bench vacío)

**Objetivo:** árbol `nexusmq/` que **compila y pasa un test trivial desde el minuto cero**.

Estructura de build y herramientas:
- [x] `CMakeLists.txt` raíz: `cmake_minimum_required(VERSION 3.25)`, `project(NexusMQ CXX)`, `cxx_std_20` (vía target INTERFACE `nexus_options`).
- [x] Flags de calidad: `-Wall -Wextra -Wpedantic -Werror` (GCC/Clang) / `/W4 /WX` (MSVC); opción `NEXUS_SANITIZERS` (ASan/UBSan). *(TSan llega con la concurrencia en 1b; es incompatible con ASan.)*
- [x] `CMakePresets.json`: `linux-gcc`, `linux-clang`, `linux-gcc-asan` (windows-msvc se añade en su momento).
- [x] `vcpkg.json` (manifest): `gtest`, `benchmark`, `fmt`. Integración vcpkg opcional (toolchain solo si `VCPKG_ROOT`); sin vcpkg → `FetchContent`.
- [x] `.clang-format` versionado (Google base, 100 col, 4 espacios, `InsertNewlineAtEOF`).
- [x] `.clang-tidy` versionado (bugprone/cppcoreguidelines/modernize/performance/readability + `readability-identifier-naming` con el naming del proyecto).
- [x] `.gitignore` (excluir `build/`, `vcpkg_installed/`, `data/`). *(`.dockerignore` llega en Fase 3 con `deploy/`.)*

Primer componente de dominio (vertical mínima, TDD rojo→verde):
- [x] `src/common/` → target `nexus-common` (lib) con `version.hpp`/`.cpp` (`nexus::version() -> std::string_view`); versión inyectada por CMake (`NEXUSMQ_VERSION`, DRY); Doxygen en español.
- [x] Test GoogleTest en `tests/unit/common/version_test.cpp` (target `nexus-tests`); nombre `Metodo_Escenario_ResultadoEsperado`.
- [x] Integración con CTest (`enable_testing()` + `gtest_discover_tests`); GoogleTest vía vcpkg-o-FetchContent (`cmake/Dependencies.cmake`).

Harness de benchmark vacío y CI:
- [x] `tools/bench/` → target `nexus-bench` (exe): `main.cpp` mínimo que enlaza Google Benchmark (vía vcpkg-o-FetchContent) y corre un *benchmark* trivial.
- [x] CI (GitHub Actions, `.github/workflows/ci.yml`): build+test en GCC y Clang con `-Werror`, job de sanitizers (ASan/UBSan), job de lint (`clang-format --dry-run --Werror` + `clang-tidy` sobre `src/`).

**Verificación de cierre de M1:** ✅ `cmake --preset linux-gcc && cmake --build … && ctest …` compila y pasa 2/2; ASan/UBSan verde; formato y clang-tidy limpios. **M1 cerrado** (pendiente solo confirmar el primer run de CI verde en GitHub).

### M2 — Record + CRC32C ✅
- [x] `nexus-common`: `types.hpp` (aliases de ancho fijo, `Codec`, helpers little-endian `load_le`/`store_le`).
- [x] `nexus-common`: `bytes.hpp/.cpp` (`Buffer` RAII **sobre `std::vector`** —ajuste del desglose, ver más abajo—, `ByteSpan`/`MutByteSpan`).
- [x] `nexus-common`: `crc32c.hpp/.cpp` — CRC32C SSE4.2 (`_mm_crc32_u64`) con detección de CPU en runtime y **fallback** software por tabla; *property test* hw==sw.
- [x] `nexus-common`: `error.hpp` (`Error`, `ErrorCode`, `expected<T>` = `std::expected`, ADR-0009/0011). *(Diferido: `NEXUS_TRY` → se usan los monádicos `and_then`/`or_else`; `clock.hpp`, `config.hpp`, `logging.hpp` → cuando se necesiten.)*
- [x] `nexus-common`: `record.hpp/.cpp` (`RecordBatch` §5.4: `encode`/`decode`/`last_offset`; CRC32C + `decode` defensivo con `expected`). *(Diferido: `Record` individual con varint/zigzag y `record_batch_builder` → al construir records reales; el payload se trata como bytes opacos.)*
- [x] Tests: **property-based** round-trip `decode(encode(x)) == x`; detección de corrupción (truncado + bit volteado); CRC32C hw vs sw.

**Ajustes de diseño respecto al desglose (M2):**
- `Buffer` se respalda con `std::vector<std::byte>` (no `unique_ptr<std::byte[]>`): convención "prefiere `vector`" + Regla de Cero, sin footguns. Misma interfaz; `std::pmr::vector` daría allocators por núcleo más adelante.
- **C++23 + libc++ en Clang** (ADR-0011): `std::expected` lo exige; se subió `cxx_std_23` y el preset/CI de Clang usan `-stdlib=libc++`.

### M3 — Segment (`.log` + `.index`)
- [x] **M3.1** `nexus-io`: `file.hpp/.cpp` — `File` RAII (Fase 1 **bloqueante**: `pread`/`pwrite`/`fsync`; `open`).
- [x] **M3.2** `nexus-storage`: `index.hpp/.cpp` (`IndexEntry`, `SparseIndex` con `floor` por búsqueda binaria, `open`/`maybe_append`/`flush`). Nuevo target `nexus-storage`.
- [x] **M3.3** `nexus-storage`: `segment.hpp/.cpp` — un tramo del log (`.log` + `.index`).
  - [x] **M3.3a** `create`/`open` + `append` (escribe `.log` e indexa) + `seal` + `is_full`. Ficheros `<base:020>.log`/`.index`.
  - [x] **M3.3b** `read(offset, max_bytes) -> FetchResult` (seek por `floor` + barrido; §7.11 #3). Nuevos: `FetchResult` (`storage/fetch_result.hpp`) y `RecordBatch::peek`/`encoded_size`/`RecordBatchView` (cabecera pública `kHeaderSize`) en `nexus-common` para recorrer el log sin copiar/revalidar.
  - [x] **M3.3c** `recover()` (valida CRC + trunca cola *torn*; §7.11 #2; reconstruye el índice) — base de M4. Nuevos: `File::truncate` (ftruncate) y `SparseIndex::reset`.
- [x] Tests: append/read; índice disperso localiza el batch; *seek* correcto; recuperación de cola *torn*/CRC corrupto.

**Ajustes de diseño respecto al desglose (M3.2):**
- `SparseIndex` mantiene las entradas en un `std::vector<IndexEntry>` **propietario** (no `std::span` sobre `mmap`): Fase 1 es E/S bloqueante y RAII estricto; el `mmap` de lectura queda como optimización medida de M6 ("lectura mmap"). Misma interfaz pública.
- `SparseIndex` **posee el `File`** del `.index` (RAII), casando con las firmas `open(path, base)` / `flush()` del desglose detallado.
- `floor(offset)` devuelve la entrada centinela `{0,0}` cuando no hay ancla `≤ offset` (índice vacío o offset previo a la primera ancla): significa «barre desde el inicio del segmento», simplificando al consumidor (`Segment`).
- `index_interval_bytes` parametriza la densidad (0 = índice denso). El `.index` se reconstruye en la recuperación (M4), por lo que `open` ignora una cola parcial y solo rechaza (`Corrupt`) entradas no estrictamente crecientes.
- **clang-tidy:** desactivado `bugprone-easily-swappable-parameters` (`chore(tidy)`): choca con las firmas del motor de log (`maybe_append(offset, file_pos, batch_len)`, futuras `read(offset, max_bytes)`…), todas enteros distintos por contrato. El resto de avisos (designated-initializers, use-ranges) se **arreglaron en código**.

### M4 — Log de partición (rolling + recuperación)
- [x] `nexus-storage`: `partition_log.hpp/.cpp` (`LogConfig`, secuencia de segmentos).
  - [x] **M4a** `open` (descubre/abre segmentos + recupera el activo) + `append` con **rotación** al superar `segment_bytes` + `log_start/end_offset` + `segment_count`. El log **asigna** el offset base autoritativo.
  - [x] **M4b** `read(offset, max_bytes)` **cruzando segmentos** (seek por índice; §7.11 #3) + `segment_for` (binaria); `OutOfRange` bajo `log_start`.
  - [x] **M4c** recuperación a nivel partición: reabrir tras *crash* en el activo (cola *torn*/CRC corrupto) trunca sin perder confirmados.
- [x] `nexus-storage`: `recovery` — **plegado en `PartitionLog::open`** por ahora (un orquestador `recover_partition` aparte llegará si el server recupera varias particiones al arrancar).

**Ajustes de diseño respecto al desglose (M4):**
- El **log asigna** el offset base (`append` reasigna `base_offset = log_end_offset`; el CRC no lo cubre). Revisable en la capa Partition/Raft (el líder asignará antes de replicar; sería un ADR si cambia).
- `cfg_` se guarda **por valor** (no `const LogConfig&`): evita una referencia colgante; `LogConfig` es un POD pequeño.
- `LogConfig` por ahora: `segment_bytes`, `index_interval_bytes`. `segment_ms` (rotación por tiempo), `fsync_policy`/`recovery_point` (M5) y retención (M6) se añadirán en su hito.
- Recuperación **plegada en `open`** (recupera solo el segmento activo; los sellados se confían).

### M5 — Durabilidad ✅
- [x] **M5a** `nexus-storage`: `FsyncPolicy` (`None`/`Interval`/`Commit`) en `LogConfig`; `Segment::sync` (fsync sin sellar); `PartitionLog::sync` + `recovery_point` (avanza por política de `append` y al sellar en la rotación).
- [x] **M5b** Test **crash** (`tests/crash/`): proceso hijo confirma N batches (fsync) + cola *torn* y muere con **`kill -9`** (SIGKILL); el padre recupera sin perder lo confirmado y trunca la cola. Verde también bajo ASan.

**Ajuste de diseño (M5):** la **durabilidad real** (sobrevivir a corte de energía) la da `fsync`/`recovery_point`; un `kill -9` no pierde la *page cache* del kernel, así que el test demuestra la recuperación *end-to-end* (confirmados sobreviven, cola en vuelo se trunca). La simulación de corte de energía con inyección de fallos queda fuera de alcance de Fase 1.

### M6 — Retención + benchmarks
- [x] **M6a** `nexus-storage`: `retention.hpp` (`RetentionPolicy` por tiempo/tamaño) + `PartitionLog::enforce_retention` (borra sellados antiguos por tamaño o por *mtime* del `.log`; **nunca el activo**; avanza `log_start_offset`).
- [x] **M6b** `nexus-bench`: `LatencyHistogram` (estilo HdrHistogram, `p50/p99/p999/max`, error acotado) + `BenchConfig`; benchmark de `append` (throughput + percentiles + **impacto de `fsync`**: None/Interval/Commit), metodología §8.2 (warm-up descartado, sin *coordinated omission*). Ejecutable `nexus-bench [op_count]`.

**Ajuste de diseño (M6a):** la antigüedad de un segmento se toma del *mtime* del `.log` (los records aún no llevan timestamp; con timestamp se usará el máximo del segmento). `RetentionPolicy::eligible` del desglose se **pliega** en `enforce_retention` (la retención por tamaño es acumulativa, no un predicado por segmento). `enforce_retention(now)` no recibe reloj: usa `file_clock::now()` (sin abstracción de `WallClock` todavía).

**Ajuste de diseño (M6b):** el **generador open-loop** se **difiere a Fase 1b** (sin red no es significativo: en el motor monohilo se mide la **latencia de servicio** de `append`, no una tasa de llegada). `nexus-bench` deja de usar Google Benchmark (arnés propio con `LatencyHistogram`). `<print>`/`std::println` evitados (GCC 14+): se usa `std::format` + `std::cout` (compatible con el GCC 13 del CI). La lectura `mmap` queda como optimización futura (no se midió en M6).

---

## Fase 1b — Reactor + broker monolítico *(no empezar hasta cerrar Fase 1)*

> Reactor thread-per-core propio (io_uring + corutinas), protocolo binario, cliente C++, produce/fetch.
> Targets: `nexus-io` (proactor), `nexus-reactor`, `nexus-protocol`, `nexus-broker` (mono-nodo),
> `nexus-client`, `nexus-server` (mono-nodo). Tests: integración.

- [x] `nexus-io` (para 1b): puerto `Proactor` + backend **io_uring** (un anillo por reactor, R4/R5); `awaitable`s; `Socket`/`Listener` async **✅**. *(`File` async se **difiere a Fase 2**: la ruta de almacenamiento se vuelve async con Raft; en 1b el log usa E/S bloqueante, decisión anotada en B2. No es deuda: es fasing.)*
  - [x] **R7** `io/socket.hpp/.cpp` — `Socket` (TCP conectado, RAII solo-movible, `async_recv`/`async_send` vía `Proactor`, `set_nodelay`) y `Listener` (`bind` plano de control → `socket`/`SO_REUSEADDR`/`bind`/`listen`; `async_accept` → `Socket`; `local_port`). Conexión = plano de control; transporte = hot-path async. Test e2e por loopback con io_uring real (eco `accept`→`recv`→`send`); los del envoltorio usan `FakeProactor`. **Ajustes:** (1) `task<T>` pasa de `reactor/task.hpp` a **`common/task.hpp`** (tipo de vocabulario bajo todas las capas; rompe el ciclo io→reactor que aparecía al devolver `task` desde io); (2) los puntos de personalización de corrutina (`await_ready`/`initial_suspend`/`final_suspend`/`return_void`) se vuelven **no estáticos** (el lenguaje los invoca sobre la instancia); (3) `.clang-tidy` desactiva `readability-convert-member-functions-to-static` (chocaría con (2)) y `cppcoreguidelines-avoid-reference-coroutine-parameters` (las corrutinas reciben refs a objetos REACTOR-LOCAL que las sobreviven).
- [x] `nexus-reactor`: corrutinas, scheduler, colas, allocator, reactor, pool (R1–R7 completos).
  - [x] **R1** `common/task.hpp` — `task<T>` (corrutina perezosa, transferencia simétrica, solo-movible) + `sync_wait`. *(Reubicado de `reactor/task.hpp` a `common/` en R7: tipo de vocabulario, va bajo todas las capas.)* *(`task` en minúscula = vocabulario estilo std, como `expected`.)*
  - [x] **R2** `reactor/scheduler.hpp` — `CoroScheduler` (`schedule`/`run_ready`) + `yield_to` (cesión cooperativa, intercala corrutinas en el hilo).
  - [x] **R3** `reactor/spsc_queue.hpp` (`SpscQueue<T,Cap>`, anillo SPSC release/acquire, `alignas(kCacheLineSize)` anti *false sharing*) + `reactor/mpmc_queue.hpp` (`MpmcQueue<T,Cap>` de Vyukov, celdas con nº de secuencia anti-ABA). Validadas con **ThreadSanitizer** (preset `linux-gcc-tsan` + job CI) con estrés productor/consumidor. *(Cap potencia de dos; `kCacheLineSize=64` fijo para evitar `-Winterference-size`.)*
  - [x] **R4** `io/proactor.hpp` — puerto `Proactor` (submit_read/write/fsync/accept/recv/send/timer + run_completions + wake; `Completion` = callback estilo io_uring) + `io/awaitable.hpp` (7 awaitables vía base CRTP, mapean el resultado a `expected`) + `tests/support/fake_proactor.hpp` (doble de test: completions deterministas). Soporte: `common/move_only_function.hpp` (`std::move_only_function` **no está en libc++ 21**; reimplementado portátil) y `MonoTime` en `common/types.hpp`. *(El backend io_uring real es R5; aquí solo el puerto + doble.)*
  - [x] **R5** `io/io_uring_backend.hpp/.cpp` — `IoUringBackend : Proactor` **directo sobre el uapi del kernel** (`io_uring_setup`/`io_uring_enter` por `syscall`, anillos `mmap` con `SINGLE_MMAP`, barreras acquire/release), **sin liburing** (ADR-0012): cero dependencias. CMake lo compila solo donde existe `<linux/io_uring.h>` (`NEXUS_HAVE_IOURING`); smoke-test con E/S real (write/read/fsync/timer/fd inválido) que se **omite** (`GTEST_SKIP`) si io_uring no está disponible. Validado en local en GCC/Clang/ASan/TSan. *(`wake` queda no-op hasta R6.)*
  - [x] **R6** `Reactor`, `CrossCoreMailbox`, `ArenaAllocator`, `ReactorPool` (afinidad).
    - [x] **R6a** `reactor/allocator.hpp` — `ArenaAllocator` (arena monótona reactor-local sobre
      `std::pmr::monotonic_buffer_resource`; `resource()`/`reset()`/`make<T>` con `construct_at`
      confinado). Libera en bloque (sin destructores → `make` exige `T` trivialmente destruible).
    - [x] **R6b** `reactor/cross_core.hpp/.cpp` — `CrossCoreMailbox` (N buzones SPSC, uno por núcleo
      origen + `wake` del destino) y `Message{target_core, work}`. `post` aplica contrapresión
      (cede el hilo si el buzón está lleno) y despierta al destino; `drain` lo consume el reactor
      dueño (único consumidor). Estrés multi-productor/único-consumidor validado bajo TSan.
      `nexus-reactor` pasa a STATIC. `FakeProactor::wake` ahora es atómico (es cross-hilo).
    - [x] **R6c** Proactor: espera bloqueante `wait_completions(max, deadline)` (cede la CPU hasta
      una completion, un `wake` o el deadline) + `wake` real por **eventfd** en el anillo io_uring
      (lectura persistente re-armada; el `wake` escribe el eventfd e interrumpe `io_uring_enter`).
      Timeout vía `IORING_ENTER_EXT_ARG` si el kernel lo admite. `FakeProactor` no bloquea (drena).
      Tests io_uring: la espera vuelve por la op, por el `wake` (otro hilo) y por el deadline.
    - [x] **R6d** `reactor/reactor.hpp/.cpp` — `Reactor` (dueño de proactor/scheduler/allocator/mailbox).
      `run` repite `poll_once` (lista→buzón→espera E/S bloqueante si ocioso; las completions reanudan
      sus corrutinas en el acto). `spawn` posee el frame detached y lo recoge al terminar; `submit_to`
      postea al buzón del peer (cableados con `connect_peers`); `stop` marca el flag y hace `wake`.
      Ajustes del desglose: ctor toma `num_cores` (lo necesita el buzón), `poll_once` público (test
      paso a paso / embebido), `partitions_` se difiere al broker. Tests con FakeProactor.
    - [x] **R6e** `reactor/reactor_pool.hpp/.cpp` — `ReactorPool` (N reactores, uno por núcleo,
      *pinned* con `pthread_setaffinity_np`). `start(n, factory)` crea los reactores (proactor por
      **factoría inyectada** —DIP—: io_uring en prod, doble en tests), cablea peers y lanza un hilo
      por reactor; `shutdown` hace `stop`+`join` (RAII en el dtor); `reactor`/`reactor_for`/`size`.
      Test e2e con io_uring real: `submit_to` despierta (eventfd) a un reactor bloqueado y ejecuta.
      Ajuste: `start` toma una factoría (el desglose ponía solo `int`); `threads_` usa `std::thread`.
- [x] `nexus-protocol`: framing, codec, mensajes, errores (P1–P6 completos; framing async en `nexus-wire`). *(El protocolo es puro encode/decode, sin async — ADR-0013.)*
  - [x] **P1** `nexus-common`: `varint.hpp/.cpp` (LEB128 + zigzag, decodificador defensivo). *(Primitiva, junto a `load_le`/`store_le`; wire en **little-endian**, consistente con el almacenamiento.)*
  - [x] **P2** `protocol/codec.hpp/.cpp` — `Encoder`/`Decoder` (cursor con chequeo de límites; u8/u16/u32/i16/i32/i64, varint, bytes/string longitud-prefijo, zero-copy). Nuevo target `nexus-protocol`.
  - [x] **P3** `protocol/error_code.hpp/.cpp` — `WireError` (i16, tamaño de wire) + `is_retryable` + `from_error` (traducción núcleo→wire en el borde).
  - [x] **P4** `protocol/frame.hpp/.cpp` — `FrameHeader` (`length|api_key|api_version|correlation_id|flags`, encode/decode defensivo) + `ApiKey` + `length_for`/`has_credit_update`.
  - [x] **P5** `protocol/messages.hpp/.cpp` — request/response por operación; `versioning`. *(compression/credits → cuando entren LZ4/Zstd y el control de flujo; mensajes de grupo de consumidores → con el broker.)*
    - [x] **P5a** `versioning` (`ApiVersionRange` + `negotiate`) + ApiVersions + Metadata (sub-structs `BrokerMeta`/`PartitionMeta`/`TopicMeta`). Vectores con contador acotado (anti-DoS).
    - [x] **P5b** Produce + Fetch (batch/batches como bytes opacos zero-copy) + `Acks` en `nexus-common`.
    - [x] **P5c** CreateTopic/DeleteTopic + OffsetCommit/OffsetFetch.
  - [x] **P6** `FrameReader`/`FrameWriter` async — **en target nuevo `nexus-wire`** (no en protocol). `read_frame` lee `length:u32`, valida (cota inferior = resto de cabecera; superior = `max_frame`, anti-DoS) y reensambla lecturas parciales; devuelve `Frame{header, payload}` (payload zero-copy en el búfer del lector, válido hasta la siguiente lectura). `write_frame` recalcula `length` del payload, codifica la cabecera y envía cabecera+payload reintentando envíos parciales. Tests con `FakeProactor` (entrega de bytes determinista) + e2e request/response por loopback con io_uring real. **Ajustes:** (1) **ADR-0013** — `nexus-wire` (common+io+protocol) aloja el framing-sobre-conexión; `nexus-protocol` queda **puro** (codec/cabecera/mensajes, sin E/S ni async). El desglose ponía `FrameReader` en protocol; conflicto con el grafo (protocol→common) resuelto a favor de capas limpias. (2) `Buffer` gana `extend`/`truncate` (cola mutable para `recv` sin copia). (3) `task<T>`: `result()`/`await_resume` con NOLINT justificado (acceso a optional garantizado por el flujo; punto de personalización sin `[[nodiscard]]`).
- [x] `nexus-broker` (mono-nodo): `Topic`/`Partition` (produce/fetch hot-path), `producer_session` (idempotencia), `offset_manager`, `topic_manager`, backpressure (`CreditWindow`). **Completo para 1b** (B1–B6); replicación/Raft, grupos de consumidores y cableado de créditos en *push* → Fase 2.
  - [x] **B1** `broker/producer_session.hpp` — `ProducerSession` (idempotencia por (producer_id, partición), §5.9): `check(base_seq, count)` → `Accept`/`Duplicate`/`Gap` (`base==expected` acepta; `<expected` ya consumido = duplicado; `>expected` o solape parcial = hueco→`OUT_OF_ORDER_SEQUENCE`) + `advance`. Header-only; tipos `ProducerId`/`Sequence` en `common/types.hpp`. Target `nexus-broker` creado (INTERFACE por ahora; pasará a STATIC con la primera `.cpp`).
  - [x] **B2** `broker/partition.hpp/.cpp` — `Partition` une `PartitionLog` + idempotencia: `produce` (no idempotente → anexa; idempotente → `check`: duplicado se reconoce sin re-anexar, hueco → `OutOfRange`) y `fetch` (delega en `PartitionLog::read`). `high_watermark = log_end_offset`, `is_leader = true` (mono-nodo). `nexus-broker` pasa a **STATIC**. **Ajustes 1b (anotados):** síncrona (sin Raft ni E/S async → no `task<>` todavía; llegará en Fase 2 con `co_await raft.propose`); sin `Acks`/`PartitionState`/`reactor_` aún (replicación/routing de Fase 2); el offset que se devuelve ante un duplicado es el último del log (no se rastrea el original por secuencia); *fencing* por epoch diferido.
  - [x] **B3** `broker/topic.hpp` — `Topic` (contenedor de `Partition` por `PartitionId`: `add_partition`/`partition`) + `TopicMetadata` (`name`, `partition_count`, `replication_factor`, `TopicConfig{segment_bytes, retention_*, compaction, compression}`, `created_at_ms`). Header-only. `replication_factor=1` en 1b; `compaction`/`compression` reservados (Fase 4).
  - [x] **B4** `broker/topic_manager.hpp/.cpp` — `TopicManager` (plano de control, THREAD-SAFE con mutex): `create_topic` abre un `PartitionLog` por partición bajo `data_dir/<topic>/<pid>/`, `get`/`delete_topic`, `describe(node_id)` → `vector<TopicMeta>` para `MetadataResponse`. `nexus-broker` enlaza `nexus::protocol`. 1b: routing a núcleos (multi-reactor) diferido; los topics se crean al arrancar.
  - [x] **B5** `broker/request_router.hpp/.cpp` — `RequestRouter` (puente protocolo↔dominio, sin red): `dispatch(ApiKey, body, out)` decodifica la petición, llama a `TopicManager`/`Partition` y codifica la respuesta, traduciendo errores del núcleo a `WireError` (`from_error`). Soporta ApiVersions, Metadata, Produce, Fetch, CreateTopic, DeleteTopic (grupos → `Unsupported` hasta Fase 2). Testeable en memoria (codifica/decodifica sin sockets). *(Se ubica en el broker, no en el servidor, para testearlo sin io; la `Connection` del servidor solo lo enmarca.)*
  - [x] **B6a** `broker/offset_manager.hpp/.cpp` — `OffsetManager` (REACTOR-LOCAL): almacén en memoria de offsets confirmados por `(grupo, topic, partición)` con `commit`/`fetch` (último commit gana; `NotFound` si el grupo no ha confirmado). El `RequestRouter` lo **posee** (uno por router/reactor, alineado con la afinidad de partición) y cablea `OffsetCommit`/`OffsetFetch` (salen de `Unsupported`; sin commit previo → `offset = -1`, sin error). `Client::commit_offset`/`fetch_offset` en `nexus-client`. Tests: unit de `OffsetManager`, router (commit→fetch y sin-commit) y e2e de cliente (commit visible desde otra conexión). **Nota:** la membresía de grupo (Join/Sync/Heartbeat, rebalanceo) y la **persistencia** del topic interno de offsets son de Fase 2; aquí el almacén es volátil.
  - [x] **B6b** `broker/credit_window.hpp` — `CreditWindow` (REACTOR-LOCAL, header-only): *backpressure por créditos* (§6.3 / §7.11 #4). El emisor `co_await acquire(cost)` antes de escribir; si no hay créditos suficientes **se frena** (suspende la corrutina, no descarta ni crece sin límite); el receptor concede con `grant(n)`, que reanuda al emisor cuando ya cabe. Awaitable propio (`Acquire`) con un único `waiter_` (modelo por conexión/partición). Tests conduciendo las corrutinas a mano (sin reactor): no-suspende-con-créditos, se-frena-y-reanuda, grant-insuficiente, frenado-en-la-segunda-reserva. **Ajuste del desglose (anotado):** el desglose ubicaba `credits.hpp` en `nexus-protocol`, pero ADR-0013 dejó el protocolo **puro** (sin async/corrutinas) → reubicado a `nexus-broker`. **Pendiente Fase 2:** el cableado end-to-end (gating de la ruta de *push* a consumidores según créditos concedidos + el campo de crédito en `flags`) llega con el consumidor *streaming* asíncrono; aquí se entrega y prueba el mecanismo.
- [x] **CLI** `nexus-client` (cliente nativo, mono-nodo): `client/endpoint.hpp` (`Endpoint{host,port}`) + `client/client.hpp/.cpp` (`Client`: conexión TCP **bloqueante** vía `Socket::connect` + petición/respuesta enmarcada con `correlation_id` espejado; ops de bajo nivel `create_topic`/`delete_topic`/`metadata`/`produce`/`fetch`; búfer de recepción interno respalda las vistas zero-copy hasta la siguiente petición) + `client/producer.hpp/.cpp` (`Producer::send`/`send_batch`: empaqueta los valores como records longitud-prefijo en el blob opaco del batch y lo publica; traduce `WireError`→`Error` con `to_error`) + `client/consumer.hpp/.cpp` (`Consumer::poll`: `Fetch` desde la posición, decodifica los `RecordBatch` en `ConsumedRecord{offset,value}` y avanza). Nuevo `to_error` (wire→núcleo) en `nexus-protocol` (borde del cliente, ADR-0009). e2e de alto nivel (`tests/e2e/client_e2e_test.cpp`): round-trip produce/consume con offsets, `send_batch`, metadata y error de topic inexistente contra un `Server` real. **Ajuste del desglose (anotado):** cliente **síncrono de un solo nodo** para 1b; `MetadataCache`/`ConnectionPool` (multi-broker), reintentos con backoff y **grupos de consumidores** (`subscribe`/`commit`/Join/Sync/Heartbeat) se difieren a Fase 2 (el broker devuelve `Unsupported` para grupos). deps: common/io/protocol (sin reactor: el cliente no lo necesita en 1b).
- [x] **SRV** `nexus-server` (mono-nodo): `Socket::connect` (conexión TCP saliente, plano de control, para cliente y tests) + `server/connection.hpp/.cpp` (`serve_connection`: bucle leer-trama→`RequestRouter::dispatch`→escribir-trama; la respuesta **espeja** `api_key`/`api_version`/`correlation_id`) + `server/server.hpp/.cpp` (`Server`: orquesta `TopicManager`+`RequestRouter`+`Reactor` io_uring+`Listener`; `bind`/`run`/`stop`; bucle de aceptación que lanza una corrutina por conexión) + `server/main.cpp` (`nexusd`: parseo de args `--port`/`--data-dir`/`--host`/`--topic n:parts`, señales SIGINT/SIGTERM → `stop()` vía eventfd async-signal-safe). **Ajustes:** (1) `Connection` del desglose → **corrutina libre `serve_connection`** que posee el socket en su *frame* (evita miembros auto-referenciados); (2) `RequestRouter` vive en `nexus-broker` (testeable sin red), no en el servidor; (3) **N=1 reactor** en 1b (multi-reactor con routing por partición → escalada thread-per-core posterior).
- [x] Tests de **integración** e2e: productor → broker → consumidor (un nodo) — `tests/e2e/broker_e2e_test.cpp`: cliente **crudo** bloqueante (`Socket::connect` + framing manual por `::send`/`::recv`) que produce 3 records y luego hace fetch contra un `Server` real en su propio hilo (reactor io_uring); valida `base_offset`/`high_watermark`/batches y el espejo de `correlation_id`. Cierra el lazo protocolo↔red↔dominio. Verde en GCC/Clang/ASan/**TSan** (cruza hilos: hilo del reactor + `stop()` desde el hilo de test). *(El e2e con el `Producer`/`Consumer` de alto nivel llegará con `nexus-client`.)*

---

## Fase 2 — Distribución (Raft por partición) *(COMPLETA ✅)*

> Targets: `nexus-consensus` (Raft); `nexus-broker` distribuido. Tests: unit/property/sim/chaos.
> Deps de `nexus-consensus` (desglose §4.6): common, storage, protocol.
>
> **Resumen de cierre de Fase 2.** Entregable: consenso **Raft por partición** completo y verificado
> por simulación determinista. Target nuevo `nexus-consensus` (`RaftLogEntry`/estado, RPC con
> (de)serialización sobre el codec, `RaftLog` sobre `PartitionLog` (ADR-0014), `RaftNode` como
> **máquina de estados síncrona sin E/S** (ADR-0015) con elección **pre-vote** (§9.6), replicación y
> avance de `commit_index`=*high-watermark* por mayoría del término (§5.4), transferencia de liderazgo
> vía `TimeoutNow` (§3.10) y **learners** no votantes (§4.2.1)). Integración en el broker:
> `ReplicatedPartition` como tipo paralelo a `Partition` (ADR-0016). **Grupos de consumidores**
> (FSM de membresía generacional + mensajes Join/Sync/Heartbeat/Leave en el protocolo + `GroupCoordinator`
> cableado en el `RequestRouter`). **Routing cross-core multi-reactor**: `call_on` (petición/respuesta
> entre núcleos) y `PartitionRouter` (`partición → núcleo dueño` por módulo, enruta vía `call_on`).
> Verificación: **arnés de simulación** (reloj + red virtuales) con escenarios de elección, failover,
> partición y **chaos** (Leader Completeness, anti-split-brain, reinicios rodantes), comprobando las
> invariantes de seguridad en cada paso. **319 tests** verdes en GCC/libstdc++, Clang/libc++,
> **ASan/UBSan** y **TSan**; CI verde. **Diferido (decisiones de fasing, no deuda):** el *swap-in* en
> caliente del stack Raft/multi-reactor en el `Server` vivo (con transporte real) — la infraestructura
> (`ReplicatedPartition`, `call_on`, `PartitionRouter`) queda lista como bloque de construcción;
> persistencia en disco del estado persistente de Raft (`persist`/`load`); snapshots/compactación de
> log. Siguiente fase: **Fase 3 — Ingress + operación**.
>
> **Convenciones fijadas en C1 (vinculantes para la fase):** índices de Raft **1-based** con
> centinela `0` ("antes de la primera entrada"; casa con el pseudocódigo §7.11 #5
> `log[prevLogIndex]`); el mapeo exacto índice↔`Offset` del `PartitionLog` se fija en `RaftLog`
> (C4, **ADR-0014**). `RaftLogEntry` **posee** su `payload` (tipo de valor, como `RecordBatch`;
> el desglose preveía `ByteSpan`). `RaftRole` se eleva a `raft_state.hpp` (vocabulario compartido).

- [x] **C1** `consensus/raft_state.hpp/.cpp` — tipos de estado de Raft: `RaftRole`
  (`Follower`/`Candidate`/`Leader`), `RaftEntryType` (`Data`/`Config`), `RaftLogEntry`
  (`term,index,type,payload` propietario), `Snapshot` (`last_included_index/_term, state`),
  `RaftPersistentState` (`current_term`/`voted_for` con invariantes: término monótono, un voto por
  término — `advance_term` resetea el voto) y `RaftVolatileState` (`commit_index`/`last_applied` +
  progreso de líder `next_index`/`match_index` por peer). Nuevo target `nexus-consensus` (STATIC).
  Tests unit de los invariantes. **Persistencia en disco (`persist`/`load`) diferida** a un
  incremento posterior (la simulación determinista opera en memoria).
- [x] **C2** `consensus/raft_rpc.hpp/.cpp` — `RequestVoteArgs/Reply`, `AppendEntriesArgs/Reply`,
  `InstallSnapshotArgs/Reply` con `encode`/`decode` defensivo sobre el codec de `nexus-protocol`
  (helpers `encode_entry`/`decode_entry`/`encode_snapshot`/`decode_snapshot`; contador de `entries`
  acotado anti-DoS). Tests round-trip + entrada truncada + tipo inválido.
- [x] **C3** `storage` — `Segment::truncate_to(offset)` + `PartitionLog::truncate_to(offset)`:
  truncado del log por **frontera de batch** (capacidad que necesita la resolución de conflictos de
  Raft, `truncate_from`, §7.11 #5). Multi-segmento (borra los posteriores, ficheros incluidos),
  retrocede `log_end_offset`/`recovery_point`, deja el segmento objetivo activo; `InvalidArgument`
  si cae a mitad de un batch, `OutOfRange` fuera de `[log_start, log_end]`. Tests unit (Segment +
  PartitionLog: corte en/entre segmentos, mitad de batch, no-op, reapertura).
- [x] **C4** **ADR-0014** + `consensus/raft_log.hpp/.cpp` — `RaftLog` (vista `(term,index)` sobre
  `PartitionLog`): `append`/`truncate_from`/`term_at`/`last_index`/`last_term`/`entries_from` +
  `offsets_at`. **Modelo (ADR-0014):** una entrada de Raft ↔ un `RecordBatch`; el **índice es el
  ordinal de entrada** (1-based, +1/entrada), espacio distinto del offset por record; el mapeo
  `índice → (term, base, last, type)` se persiste en un **sidecar** de 25 B/entrada (`raft-meta`),
  dejando el `RecordBatch` y `nexus-storage` intactos. `truncate_from` delega en
  `PartitionLog::truncate_to` (C3). Tests unit (append/índices/términos, `offsets_at`,
  `entries_from` con `max`, truncado+reanudación, reapertura recupera el estado).
- [x] **C5** **ADR-0015** + `consensus/raft_node.hpp/.cpp` — `RaftNode` como **máquina de estados
  síncrona sin E/S** (entradas `tick`/`on_*` con `now` inyectado → cola de `RaftMessage` drenable
  con `take_messages`; reemplaza el par corrutina `propose` + `RaftTransport` del desglose). C5
  cubre: elección (`tick` → `become_candidate` → `RequestVote`; `become_leader` por mayoría;
  *self-elección* en cluster de 1), voto (`on_request_vote` con regla de log al-día §5.4 y un voto
  por término), `on_append_entries` (term/leadership, *log matching* §7.11 #5 con `conflict_index`,
  anexado/truncado de entradas, avance de `commit_index` por `leader_commit`), `become_follower`
  por término mayor, *heartbeats* del líder. RNG sembrado por nodo (election timeout reproducible).
  Tests deterministas (`now` inyectado, mensajes enrutados a mano): auto-elección, elección a 3
  nodos, heartbeat evita elección, denegación por log obsoleto, *step-down*, anexado+commit,
  `prev_log_index` inexistente.
- [x] **C6** `propose` + replicación del líder en `raft_node.hpp/.cpp`. `propose(RecordBatch)`
  (solo líder; `Unsupported` si no): codifica el batch, anexa una entrada del término actual al
  `RaftLog`, replica a los peers y reevalúa `commit_index`; devuelve el índice asignado. Líder:
  `replicate_to` envía `AppendEntries` desde el `next_index` del peer (acota a
  `kMaxEntriesPerAppend=64`; vacío = *heartbeat*) y recuerda el último índice enviado (`last_sent_`);
  `replicate_all` recorre todos los peers (también la ronda de *heartbeats*). `on_append_entries_reply`:
  *step-down* por término mayor; en éxito fija `match_index`/`next_index` y llama
  `advance_commit_index`; en fallo retrocede `next_index` por la pista `conflict_index` y reintenta.
  `advance_commit_index` confirma el mayor índice replicado en **mayoría** del término actual (§5.4)
  → `commit_index` es la **high-watermark**. `become_leader`/`tick` replican (heartbeat). Tests:
  `propose` en no-líder da error, nodo único confirma inmediato, réplica a 3 nodos confirma por
  mayoría, seguidor atrasado alcanza la cola por retroceso de `next_index`.
- [x] **C7** pre-vote, leadership transfer, learners. **Ajuste del desglose (anotado):** el desglose
  preveía un `election.hpp/.cpp` con firmas de corrutina/transporte (`task<expected<void>>
  transfer(...)`); ADR-0015 eliminó corrutinas y `RaftTransport` del núcleo de consenso, así que
  estas funciones se integran **dentro de `RaftNode`** (la FSM síncrona) en vez de un módulo aparte
  con firmas obsoletas. Se subdivide en incrementos pequeños:
  - [x] **C7a** **pre-vote** (§9.6). Nuevo rol `RaftRole::PreCandidate`: al vencer el *election
    timeout*, `tick` arranca `start_pre_election` (sondea con el término *prospectivo* `current+1`
    y `pre_vote=true`, **sin** subir el término ni votarse a sí mismo); con mayoría de pre-votos
    pasa a `become_candidate` (elección real). `make_pre_vote_reply` concede el pre-voto solo si el
    log está al día **y** expiró el *lease* del líder (`now >= election_deadline_`) **y** no nos
    creemos líder, sin mutar estado. Un nodo aislado nunca sube su término → al reincorporarse no
    fuerza el *step-down* del líder vigente. Tests: el sondeo no sube el término, denegación por
    *lease* vigente, concesión sin líder y con log al día, nodo aislado conserva su término.
  - [x] **C7b** **leadership transfer** (§3.10). Nuevo RPC `TimeoutNowArgs{term, leader_id}` (con
    round-trip sobre el codec) y entrada en la `variant` de `RaftMessage`. `transfer_leadership(target)`
    (solo líder; `Unsupported`/`InvalidArgument` si no): si el objetivo está al día le envía
    `TimeoutNow`, si va rezagado lo pone al día (`maybe_transfer_to` reusa `replicate_to` y dispara
    `TimeoutNow` desde `on_append_entries_reply` al alcanzar la cola). `on_timeout_now` arranca una
    elección **real** inmediata (salta pre-vote y *lease*): el objetivo, con el log al día, gana y el
    líder anterior cede al ver el término mayor. Tests: error si no es líder, error si el destino no
    es peer, transferencia con objetivo al día, y con objetivo rezagado que primero alcanza la cola.
  - [x] **C7c** **learners** (§4.2.1). El ctor de `RaftNode` acepta `learners` (subconjunto de
    `peers`): el líder les replica (`replicate_all` no cambia), pero `is_voter` los excluye del
    quórum (`cluster_size` cuenta solo votantes; `advance_commit_index` ignora su `match_index`), no
    se les pide voto (`broadcast_request_vote`) ni se cuentan sus votos, un learner-self nunca se
    postula (`tick`/`on_timeout_now`) y no es destino válido de transferencia. Tests: el learner no
    se autoelige, recibe replicación pero su ack no confirma (sí lo hace el del votante), y rechazo
    de transferencia hacia un learner.
- [x] **C8** Arnés de **simulación determinista** (`tests/sim/raft_sim.hpp`): `VirtualClock`
  (avanza solo bajo control) + `Cluster` con red virtual (latencia fija; las respuestas de `on_*`
  también viajan por la red) y fallos inyectables (`partition`/`heal`, `crash`/`restart`). En cada
  paso comprueba invariantes de seguridad: **a lo sumo un líder por término** (§5.2) y **las
  entradas confirmadas no divergen** (§5.4). Escenarios: elección a 5 nodos converge a un líder;
  failover al caer el líder (emerge otro en término mayor); propuesta replicada y confirmada en el
  clúster; partición que aísla a la minoría (con pre-vote no infla su término ni disrumpe; la
  mayoría conserva su líder; sana y converge). **El arnés detectó un bug real del pre-vote (C7a):**
  el *lease* se comprobaba contra `election_deadline_` (siempre armado), bloqueando la elección
  inicial; ahora se rastrea `last_leader_contact_` (solo `AppendEntries` válido) y se concede el
  pre-voto si no hubo contacto de líder en un *election timeout* — justifica el valor del arnés.
- [x] **C9** **ADR-0016** + `nexus-broker`: `ReplicatedPartition` (`broker/replicated_partition.{hpp,cpp}`)
  como **tipo paralelo** a `Partition` (no se muta esta; el desglose preveía mutarla). Compone la pila
  por `unique_ptr` (`PartitionLog`+`RaftLog`+`RaftNode`) → **movible** pese a las referencias internas
  autorreferenciales. `produce` (solo líder; `Unsupported` si no) aplica idempotencia (§5.9) y
  **propone** al `RaftNode`, traduciendo el índice de Raft a su último offset vía `RaftLog::offsets_at`;
  `high_watermark()` = offset (exclusivo) de `commit_index` → **`acks=quorum`** (una escritura no es
  visible hasta que el quórum la confirma). La FSM no se conduce sola: `raft()` expone la superficie
  para el portador (reactor/arnés). Pruebas (`tests/unit/broker/replicated_partition_test.cpp`) con
  reloj y red virtuales: nodo único confirma al proponer; 3 nodos no avanzan el high-watermark hasta
  el quórum y los seguidores se ponen al día; no-líder → `Unsupported`; idempotencia duplicado/hueco.
  El **cambio en caliente** del broker (con transporte real) se difiere a **C11**.
- [x] **C10** grupos de consumidores + rebalanceo (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`).
  Se subdividió en incrementos pequeños:
  - [x] **C10a** `broker/consumer_group.{hpp,cpp}` — `ConsumerGroup`, FSM de membresía **síncrona y
    sin E/S** (mismo principio que `RaftNode`, ADR-0015: reloj inyectado `MonoTime now`, expiración de
    sesión en `tick`, deterministas). Protocolo *eager* generacional: estados
    `Empty→PreparingRebalance→CompletingRebalance→Stable` (+`Dead`); un cambio de membresía (`join`
    nuevo, `leave`, sesión expirada) abre un rebalanceo en el que **todos** reingresan, el primero es
    el **líder** y reparte en `sync`; la `generation` numera cada ronda y descarta peticiones
    obsoletas. `join` (id vacío ⇒ id generado), `sync` (líder reparte → `Stable`; seguidor recibe su
    asignación, opaca), `heartbeat` (`Ok`/`RebalanceInProgress`), `leave`, `tick(now)`. Nuevo alias
    `Generation` en `common/types.hpp`. 15 tests: alta/líder, reparto, rebalanceo al entrar/salir,
    heartbeat en rebalanceo, expiración por `tick`, generación obsoleta, miembro desconocido.
  - [x] **C10b** mensajes de protocolo de grupo en `protocol/messages.{hpp,cpp}` (las `ApiKey` ya
    existían): `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` request/response + sub-structs
    `GroupMember`/`GroupAssignment` (DTOs wire que el borde traduce a/desde `MemberInfo`/
    `MemberAssignment` del dominio). `encode`/`decode` sobre el codec (suscripciones/asignaciones como
    `bytes`; listas con `varint`+elementos; `error_code:i16`). Tests de round-trip (con miembros y
    asignaciones) y de `decode` truncado en `messages_test.cpp`.
  - [x] **C10c** `broker/group_coordinator.{hpp,cpp}` — `GroupCoordinator` (REACTOR-LOCAL): mapa
    `group_id → ConsumerGroup`; `join` **crea** el grupo (alta del primer consumidor), el resto exige
    grupo existente (`NotFound`); `tick(now)` expira sesiones de todos los grupos. Cableado en
    `RequestRouter` (lo **posee**, como a `OffsetManager`): despacha `JoinGroup`/`SyncGroup`/
    `Heartbeat`/`LeaveGroup` traduciendo wire↔dominio en el borde — `GroupMember`/`GroupAssignment`
    ↔ `MemberInfo`/`MemberAssignment`, errores con `from_error`, y `RebalanceInProgress`/seguidor sin
    asignación → `WireError::RebalanceInProgress`. El reloj se toma del `steady_clock` en el borde
    (Fase 1b síncrona; el reactor lo inyectará y conducirá `tick` en C11). Tests: flujo
    join→sync→heartbeat→leave en el router, heartbeat a grupo desconocido, `ApiKey` desconocida, y
    unidades del coordinador (crea en join, `NotFound`, expiración por `tick`).
- [x] **C11** cross-core message passing / routing de particiones multi-reactor. La infraestructura
  de reactores ya existía (`Reactor::submit_to`/`spawn`, `CrossCoreMailbox` SPSC, `ReactorPool` con
  `reactor_for(partition) = partition % size()`). Se subdivide en incrementos pequeños:
  - [x] **C11a** `reactor/cross_core_call.hpp` — `call_on(self, target, fn)`: primitivo de
    **petición/respuesta cross-core** (header-only). Un *awaiter* que, al suspenderse, postea `fn` al
    reactor destino (corre **en su hilo**, sobre su estado reactor-local) y postea de vuelta la
    reanudación al origen con el resultado; la sincronización la da el buzón SPSC (release/acquire),
    sin candados. Es el mecanismo de enrutado: el dueño de una partición ejecuta la operación en su
    núcleo y devuelve la respuesta al núcleo que atiende la conexión. Tests deterministas con dos
    reactores conducidos paso a paso (`poll_once`): ejecuta en destino y reanuda con resultado,
    transporta el valor calculado, y no se resuelve sin drenar al destino.
  - [x] **C11b** `reactor/partition_router.hpp` — `PartitionRouter` (header-only): mapea
    `partición → núcleo dueño` (`owner_core(p) = p % core_count`, la misma regla que
    `ReactorPool::reactor_for`) y enruta la operación al dueño vía `call_on`, reanudando al llamante
    con el resultado (viaje local si el dueño es su propio reactor; si no, cruza núcleos por el buzón
    SPSC). Tests deterministas con dos reactores conducidos paso a paso: reparto por módulo de
    núcleos, asignación de offsets por partición devuelta al llamante, y comprobación de que el estado
    de cada partición vive **solo** en su núcleo dueño (sin solaparse pares/impares).
- [x] **C12** Tests **chaos**/failover deterministas → postura **CP**; cierre de Fase 2. Sobre el
  arnés de simulación (C8: reloj + red virtuales) se añaden tres escenarios que estresan la postura
  **CP** sin depender de la red real: (1) **Leader Completeness** (§5.4) — una entrada confirmada
  antes de la caída del líder está en el nuevo líder y sigue confirmada (lo confirmado no se pierde en
  el failover); (2) **anti-split-brain** — el líder aislado en minoría (1 de 5) anexa local pero **no
  confirma**, la mayoría elige otro líder y confirma, y al sanar la red el viejo líder cede y **trunca
  su cola no confirmada** adoptando el log de la mayoría; (3) **caos** — reinicios rodantes (a lo sumo
  un nodo caído, siempre con quórum) con propuestas que mantienen el progreso y, al final, convergen
  todos los logs. **Ajuste de diseño:** el chaos NO usa `tc netem` (requeriría root/netns y sería
  *flaky*, contra la norma de tests deterministas): se modela como **fallos inyectados en la red
  virtual** (`partition`/`heal`, `crash`/`restart`), reproducible por semilla. En cada paso el arnés
  verifica las invariantes de seguridad (a lo sumo un líder por término; las entradas confirmadas no
  divergen). Tests verdes en GCC/Clang/**ASan**.

---

## Fase 3 — Ingress + operación *(EN CURSO)*

> Targets: `nexus-ingress`, `nexus-cli`; observabilidad (`nexus-server`); `deploy/`.
> Deps de `nexus-ingress` (desglose §4.8): common, io, protocol, wire.
>
> **Convenciones fijadas (vinculantes para la fase):** las primitivas con noción de tiempo
> (`TokenBucket`, `CircuitBreaker`, `HealthChecker`) reciben el **reloj inyectado** (`MonoTime now`
> por parámetro), no lo leen internamente — igual que la FSM de Raft (ADR-0015) — para pruebas
> deterministas y para que el reactor controle el tiempo (**ajuste del desglose**, que mostraba
> `allow(cost)` sin `now`). El HTTP, JWT y la criptografía de soporte (SHA-256/HMAC) se implementan
> **a mano** (núcleo de aprendizaje, deterministas, *property/known-answer tests*); OpenSSL se reserva
> para el transporte TLS (I17) y se compila **condicionalmente** (como io_uring), sin bloquear el resto.

### Bloque I.A — Primitivas de resiliencia (`nexus-ingress`, deterministas, sin deps externas)
- [x] **I1** `ingress/rate_limiter.hpp` — `TokenBucket` (header-only, REACTOR-LOCAL): cubo de fichas
  con **relleno perezoso** (`rate` fichas/s, tope `burst`, arranca lleno) y reloj inyectado;
  `allow(now, cost=1)` admite/rechaza descontando, `available(now)` consulta, `configure(rate, burst)`
  recorta a la nueva capacidad. Nuevo target `nexus-ingress` (INTERFACE hasta la primera `.cpp`).
  Tests deterministas: ráfaga inicial, rechazo al agotar, relleno con el tiempo, tope de capacidad,
  coste variable, reloj anterior no acredita, reconfiguración.
- [x] **I2** `ingress/circuit_breaker.{hpp,cpp}` — `CircuitBreaker` (Nygard, REACTOR-LOCAL): tres
  estados `Closed/Open/HalfOpen` con **ventana deslizante** de resultados (anillo de `window_size`,
  contador de fallos incremental) y reloj inyectado. Closed dispara a Open al alcanzar `failure_ratio`
  con `>= min_samples`; Open rechaza hasta `open_timeout` y entonces pasa a HalfOpen; HalfOpen admite
  hasta `half_open_probes` sondas y cierra si todas tienen éxito o reabre al primer fallo. `nexus-ingress`
  pasa a **STATIC**. Tests deterministas: no dispara con pocas muestras, dispara por tasa de error,
  rechaza en el timeout, acota sondas en HalfOpen, cierra al recuperarse, reabre si una sonda falla.
- [ ] **I3** `ingress/load_balancer.{hpp,cpp}` — `LoadBalancer` (round-robin / least-connections /
  consistent-hashing por *partition key*).
- [ ] **I4** `ingress/health_check.{hpp,cpp}` — `HealthChecker` (activo: ping periódico; pasivo:
  errores reales; alimenta `/readyz`).

### Bloque I.B — Observabilidad (`nexus-server`)
- [ ] **I5** `MetricsRegistry` (contadores/gauges/histogramas + exposición Prometheus `/metrics`).
- [ ] **I6** logs estructurados JSON + *correlation IDs* (`common/logging.hpp`).

### Bloque I.C — HTTP + REST admin
- [ ] **I7** HTTP/1.1: `HttpRequest`/`HttpResponse` + parser defensivo (TDD + fuzzing).
- [ ] **I8** RFC 7807 `ProblemDetail` + paginación (`page`/`size`) + helpers de JSON para DTOs.
- [ ] **I9** `common`: SHA-256 + HMAC-SHA256 a mano (*known-answer tests* de RFC/NIST).
- [ ] **I10** `JwtVerifier` (HS256, base64url, `exp`/`nbf`/`iat` con reloj inyectado).
- [ ] **I11** `AdminApi` sobre `TopicManager`/`GroupCoordinator` (create/delete/describe/list/reassign).
- [ ] **I12** `RestGateway` (`/api/v1/...` → `AdminApi`, RFC 7807, paginación, autenticación JWT).
- [ ] **I13** `/healthz` + `/readyz` (checks disk/raft/replicationLag/startup, JSON).
- [ ] **I14** Cableado en `nexus-server`: puerto HTTP de admin (RestGateway + Metrics + health).

### Bloque I.D — CLI
- [ ] **I15** `nexus-cli`: esqueleto + parseo de args + subcomandos `topic` (create/list/describe/delete).
- [ ] **I16** `nexus-cli`: subcomandos `group` / `partitions` / `diagnostics`.

### Bloque I.E — TLS + modos de ingress
- [ ] **I17** OpenSSL (condicional) + `TlsContext`/`TlsConnection` (TLS 1.3, mTLS; e2e loopback con
  certs autofirmados, *skip* si no hay OpenSSL).
- [ ] **I18** `Proxy` (modo proxy: enruta clientes "tontos" al líder por *consistent-hash*) +
  `ConnectionState`.

### Bloque I.F — Despliegue + documentación
- [ ] **I19** `deploy/`: `Dockerfile` (multi-stage → distroless, no-root, `HEALTHCHECK` → `/readyz`),
  `docker-compose.yml` (3 nodos + Prometheus/Grafana), `k8s/` (probes), `.dockerignore`.
- [ ] **I20** `docs/openapi.yaml` (contrato REST) + `docs/protocol.md` (protocolo binario) + cierre de Fase 3.

---

## Fase 4 — Stretch *(opcional, tras 3)*

- [ ] Productor idempotente *effectively-once* por partición; DLQ; compactación por clave.
- [ ] Compresión LZ4/Zstd por batch (anti *decompression bomb*).
- [ ] *Direct I/O* (`O_DIRECT`) + caché/readahead propios.
- [ ] Subconjunto **Kafka-compatible** (`ApiVersions`/`Metadata`/`Produce`/`Fetch`) → habla con `kcat`.
- [ ] *Binding* Python (pybind11); tracing distribuido.
- [ ] Backend **IOCP** (Windows) en `nexus-io`; preset `windows-msvc`.

---

## Documentación de diseño que crecerá por fases

- [ ] `docs/protocol.md` (Anexo A): spec del protocolo binario (framing, correlation IDs, versionado, créditos) — Fase 1b.
- [ ] `docs/openapi.yaml`: contrato del REST admin (RFC 7807) — Fase 3.
- [ ] `docs/adr/adr-NNNN-*.md`: extraer los ADR del §9 del anteproyecto a archivos individuales (commit `docs:`).
