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

**Estado actual:** Fase 1 · **M1 (Esqueleto)** — cimientos ✅ · primera librería + test verde ✅; siguiente: `.clang-format`/`.clang-tidy`, bench vacío y CI (sub-paso 3).

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
- [ ] `.clang-format` versionado (estilo del proyecto; naming lo refuerza el formateador).
- [ ] `.clang-tidy` versionado (chequeos Core Guidelines).
- [x] `.gitignore` (excluir `build/`, `vcpkg_installed/`, `data/`). *(`.dockerignore` llega en Fase 3 con `deploy/`.)*

Primer componente de dominio (vertical mínima, TDD rojo→verde):
- [x] `src/common/` → target `nexus-common` (lib) con `version.hpp`/`.cpp` (`nexus::version() -> std::string_view`); versión inyectada por CMake (`NEXUSMQ_VERSION`, DRY); Doxygen en español.
- [x] Test GoogleTest en `tests/unit/common/version_test.cpp` (target `nexus-tests`); nombre `Metodo_Escenario_ResultadoEsperado`.
- [x] Integración con CTest (`enable_testing()` + `gtest_discover_tests`); GoogleTest vía vcpkg-o-FetchContent (`cmake/Dependencies.cmake`).

Harness de benchmark vacío y CI:
- [ ] `tools/bench/` → target `nexus-bench` (exe): `main.cpp` mínimo que enlaza Google Benchmark y corre un *benchmark* trivial (esqueleto del generador open-loop).
- [ ] CI (GitHub Actions, `.github/workflows/ci.yml`): compila en Linux (GCC/Clang) con `-Werror`, corre `clang-format --dry-run`, `clang-tidy`, los tests y los sanitizers.

**Verificación de cierre de M1:** `cmake --preset linux-gcc && cmake --build --preset linux-gcc && ctest --preset linux-gcc` compila y pasa el test trivial; CI en verde.

### M2 — Record + CRC32C
- [ ] `nexus-common`: `types.hpp` (aliases de ancho fijo, `Codec`, helpers little-endian `load_le`/`store_le`).
- [ ] `nexus-common`: `bytes.hpp/.cpp` (`Buffer` RAII, `ByteSpan`/`MutByteSpan`).
- [ ] `nexus-common`: `crc32c.hpp/.cpp` — CRC32C SSE4.2 (`_mm_crc32_u64`) con detección de CPU en runtime y **fallback** software por tabla.
- [ ] `nexus-common`: `error.hpp` (`Error`, `ErrorCode`, `expected<T>`, `NEXUS_TRY`); `clock.hpp` (`MonotonicClock`/`WallClock`); `config.hpp/.cpp`; `logging.hpp/.cpp`.
- [ ] `nexus-storage`: `record.hpp/.cpp` (`RecordBatch` §5.4 `encode`/`decode`/`verify_crc`/`last_offset`; `Record`).
- [ ] `nexus-storage`: `record_batch_builder.hpp/.cpp` (varint/zigzag, deltas, CRC).
- [ ] Tests: **property-based** round-trip `decode(encode(x)) == x`; CRC32C hw vs sw coinciden.

### M3 — Segment (`.log` + `.index`)
- [ ] `nexus-io`: `file.hpp/.cpp` — `File` RAII (Fase 1 **bloqueante**: `pread`/`pwrite`/`fsync`; `open`).
- [ ] `nexus-storage`: `index.hpp/.cpp` (`IndexEntry`, `SparseIndex` con `floor` por búsqueda binaria, `load`).
- [ ] `nexus-storage`: `segment.hpp/.cpp` (`append`, `read`, `seal`, `recover`).
- [ ] Tests: append/read; índice disperso localiza el batch; *seek* correcto.

### M4 — Log de partición (rolling + recuperación)
- [ ] `nexus-storage`: `partition_log.hpp/.cpp` (`append` con rotación de segmento, `read` cruzando segmentos vía índice §7.11 #3).
- [ ] `nexus-storage`: `recovery.hpp/.cpp` (`recover_partition`) — valida CRC y **trunca cola *torn*** (§7.11 #2).
- [ ] Tests: lectura cruzando segmentos; recuperación tras cola incompleta/corrupta.

### M5 — Durabilidad
- [ ] `nexus-common`/`nexus-storage`: política de `fsync` (`none`/`interval`/`commit`); `recovery_point`.
- [ ] Tests **crash**: `kill -9` a mitad de escritura → recuperación sin pérdida de lo confirmado.

### M6 — Retención + benchmarks
- [ ] `nexus-storage`: `retention.hpp/.cpp` (`RetentionPolicy` por tiempo/tamaño; ciclo de vida de segmento; nunca borra el activo).
- [ ] `nexus-bench`: `load_generator` open-loop + `latency_histogram` (HdrHistogram, `p50/p99/p999/max`); `bench_config`.
- [ ] Benchmarks: throughput, percentiles, impacto de `fsync`, lectura `mmap`, con metodología §8.2 (núcleos aislados, descartar warm-up).

---

## Fase 1b — Reactor + broker monolítico *(no empezar hasta cerrar Fase 1)*

> Reactor thread-per-core propio (io_uring + corutinas), protocolo binario, cliente C++, produce/fetch.
> Targets: `nexus-io` (proactor), `nexus-reactor`, `nexus-protocol`, `nexus-broker` (mono-nodo),
> `nexus-client`, `nexus-server` (mono-nodo). Tests: integración.

- [ ] `nexus-io`: puerto `Proactor` + backend **io_uring** (un anillo por reactor); `awaitable`s; `File`/`Socket` async; `Listener`.
- [ ] `nexus-reactor`: `Reactor`, `CoroScheduler`, `task<T>`, `SpscQueue` (alignas anti *false sharing*), `MpmcQueue` (ABA), `CrossCoreMailbox`, `ArenaAllocator`, `ReactorPool` (afinidad).
- [ ] `nexus-protocol`: `FrameHeader`/`FrameReader`/`FrameWriter`, `codec` (varint/zigzag, decodificador defensivo), `messages` (ApiVersions/Metadata/Produce/Fetch/OffsetCommit…), `error_code` (`WireError`), `versioning`, `compression`, `credits`.
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
