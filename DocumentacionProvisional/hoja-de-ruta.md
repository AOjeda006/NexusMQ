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

**Estado actual:** **FASE 1 COMPLETA ✅** (M1–M6). `nexus-common` + `nexus-io` + `nexus-storage` + `nexus-bench`: motor de log monopartición con E/S bloqueante (File RAII, RecordBatch+CRC32C, SparseIndex, Segment, PartitionLog con rotación + lectura cruzando segmentos + recuperación de cola *torn* + política de `fsync`/`recovery_point` + retención por tamaño/tiempo) y benchmark con histograma de latencias. **66 tests** verdes en GCC/libstdc++, Clang/libc++ y ASan/UBSan; CI verde. Siguiente fase: **Fase 1b — Reactor + broker monolítico** (io_uring + corrutinas, protocolo binario, cliente C++).

> **Resumen de cierre de Fase 1.** Entregable: motor de log append-only monopartición, durable y recuperable, con cifras de rendimiento. Targets: `nexus-common`, `nexus-io`, `nexus-storage`, `nexus-bench`, `nexus-tests` (unit/property/crash). Modelo de errores `expected<T>` en todo el núcleo; RAII estricto; TDD rojo→verde→refactor; sin reactor ni io_uring (eso es 1b). Ajustes de diseño respecto al desglose anotados por hito (Buffer sobre vector, índice en vector propietario, el log asigna offsets, `cfg_` por valor, recuperación plegada en `open`, retención por *mtime*, open-loop diferido a 1b).

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

- [ ] `nexus-io`: puerto `Proactor` + backend **io_uring** (un anillo por reactor); `awaitable`s; `File`/`Socket` async; `Listener`.
- [ ] `nexus-reactor`: `Reactor`, `CoroScheduler`, `task<T>`, `SpscQueue` (alignas anti *false sharing*), `MpmcQueue` (ABA), `CrossCoreMailbox`, `ArenaAllocator`, `ReactorPool` (afinidad).
- [~] `nexus-protocol`: framing, codec, mensajes, errores. *(El reactor async se intercala; el protocolo es puro encode/decode, sin async, y va primero por ser TDD-puro.)*
  - [x] **P1** `nexus-common`: `varint.hpp/.cpp` (LEB128 + zigzag, decodificador defensivo). *(Primitiva, junto a `load_le`/`store_le`; wire en **little-endian**, consistente con el almacenamiento.)*
  - [x] **P2** `protocol/codec.hpp/.cpp` — `Encoder`/`Decoder` (cursor con chequeo de límites; u8/u16/u32/i16/i32/i64, varint, bytes/string longitud-prefijo, zero-copy). Nuevo target `nexus-protocol`.
  - [ ] **P3** `protocol/error_code.hpp` — `WireError` + `is_retryable` + `from_error` (traducción núcleo→wire en el borde).
  - [ ] **P4** `protocol/frame.hpp/.cpp` — `FrameHeader` (`length|api_key|api_version|correlation_id|flags`) + `ApiKey`.
  - [ ] **P5** `protocol/messages.hpp/.cpp` — request/response por operación (ApiVersions/Metadata/Produce/Fetch…); `versioning`. *(compression/credits → cuando entren LZ4/Zstd y el control de flujo.)*
  - [ ] **P6** `FrameReader`/`FrameWriter` async (requieren el proactor; van tras el reactor).
- [ ] `nexus-broker` (mono-nodo): `Topic`/`Partition` (produce/fetch hot-path), `producer_session` (idempotencia), `offset_manager`, `topic_manager`, backpressure.
- [ ] `nexus-client`: `Client`, `MetadataCache`, `ConnectionPool`, `Producer`, `Consumer`.
- [ ] `nexus-server`: `main.cpp` (bootstrap + señales SIGTERM/SIGINT vía eventfd), `Server`, `Connection`, `RequestRouter`.
- [ ] Tests de **integración** e2e: productor → broker → consumidor (un nodo).

---

## Fase 2 — Distribución (Raft por partición) *(no empezar hasta cerrar 1b)*

> Targets: `nexus-consensus`; `nexus-broker` distribuido. Tests: sim/chaos.

- [ ] `nexus-consensus`: `RaftPersistentState`/`RaftVolatileState`/`RaftLogEntry`/`Snapshot`; `raft_rpc` (RequestVote/AppendEntries/InstallSnapshot); `RaftLog` (sobre PartitionLog); `RaftNode` (FSM, `propose`/`on_append_entries`/`on_request_vote`/`tick`); `election` (pre-vote, leadership transfer, learners).
- [ ] `nexus-broker`: integrar Raft en `Partition`; `acks=quorum`; high-watermark = commitIndex; grupos de consumidores + rebalanceo; cross-core message passing.
- [ ] Tests: **simulación determinista** (reloj/red virtuales) de elecciones y *splits*; **chaos** (`tc netem`) → failover y postura **CP**.

---

## Fase 3 — Ingress + operación *(no empezar hasta cerrar 2)*

> Targets: `nexus-ingress`, `nexus-cli`; observabilidad; `deploy/`.

- [ ] `nexus-ingress`: `TlsContext`/`TlsConnection` (TLS 1.3, mTLS), `TokenBucket`, `CircuitBreaker`, `LoadBalancer`, `HealthChecker`, `RestGateway` (`/api/v1`, RFC 7807, paginación, JWT, OpenAPI), `Proxy`.
- [ ] `nexus-server`: `AdminApi`, `MetricsRegistry` (`/metrics` Prometheus), `/healthz`/`/readyz`, logs JSON, tracing.
- [ ] `nexus-cli`: subcomandos `topic`/`group`/`partitions`/`diagnostics`.
- [ ] `deploy/`: `Dockerfile` (multi-stage → distroless, no-root, HEALTHCHECK → `/readyz`), `docker-compose.yml` (3 nodos + Prometheus/Grafana), `k8s/` (probes).

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
