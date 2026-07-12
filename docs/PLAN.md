# PLAN — NexusMQ · cierre v1.0 (cifrado en reposo · tiered storage · exactly-once)

> Plan de registro (**fuente de verdad del estado del trabajo**). Se lee al arrancar y tras cada
> `/compact`, y se actualiza al avanzar. Mantenerlo coherente con el repo (git) en todo momento.

## Objetivo

Implementar las tres features que estaban en «trabajo futuro» (cap. 30) como **tres hitos
secuenciales**, cada uno con su **ADR nuevo** (empiezan en `adr-0031`; `adr-0030` ya existe),
llevando el proyecto a **v1.0**. Cada feature es **opcional con degradación limpia**: sin
configurar, el broker se comporta como hoy. Árbol **siempre compilable y en verde** entre commits.

## Alcance / No-objetivos

- **Dentro:** (A) Cifrado en reposo AEAD AES-256-GCM por bloque de escritura; (B) Tiered storage de
  segmentos sellados a un puerto `StorageTier` (impl. local); (C) Exactly-once multi-partición
  («effectively-once» end-to-end) solo en superficie nativa, con Transaction Coordinator (Raft
  propio), control records, LSO + `read_committed`, y API `initTransactions/begin/commit/abort`.
- **Fuera:** rotación de claves (cifrado) — futuro documentado; adaptador S3 real (solo puerto +
  impl. local; S3 tras `find_package` opcional); transacciones en el subset Kafka; entrega
  *exactly-once* literal (solo effectively-once honesto).

## Entorno / flujo de trabajo (NO obvio — leer siempre)

- **Dos clones:** canónico en **Windows** `c:\...\NexusMQ` (mis herramientas de fichero + firma SSH
  con `signingkey` de ruta Windows) y uno nativo en **WSL** `/home/andres_dev/NexusMQ` (atrasado,
  **no se usa**). Editar/commit/firmar/PDF en el clon **Windows**.
- **Build/test** vía **WSL Ubuntu** sobre `/mnt/c/...`: toolchain cmake 4.2.3, gcc 15.2, clang 21.1.8
  (+ libc++), clang-tidy/format 21.1.8, ninja. Presets usan FetchContent (no requieren VCPKG_ROOT).
  Ejemplo: `wsl -d Ubuntu -- bash -lc 'cd "/mnt/c/.../NexusMQ" && cmake --build --preset linux-gcc && ctest --preset linux-gcc'`.
- **PDF:** `docs/pdf` con **node de Windows** (v25) + Playwright/Chromium ya instalado.
- **Firma:** SSH, `commit.gpgsign=true`, committer `Andrés Ojeda Rodríguez
  <andresojedarodriguez@gmail.com>`; **sin** Co-Authored-By ni trailers de IA. Directo a `main`.

## Decisiones tomadas

> Producto ya decidido por el autor (no volver a preguntar). Técnicas propias → ADR nuevo.

- 2026-07-12 — Las tres features son **opt-in con degradación limpia** (patrón TLS/compresión).
- 2026-07-12 — **Cifrado (Hito A):** AES-256-GCM (OpenSSL/AES-NI); granularidad **por bloque de
  escritura** (= un `RecordBatch`/`append`), nunca por record. KEK 256-bit desde entorno/config,
  jamás en repo. **Invariante nº1: no reutilizar nonce con la misma clave.**
- 2026-07-12 — **Diseño cripto elegido (→ ADR-0031):** DEK **por segmento** = HKDF-SHA256(KEK,
  salt aleatoria por segmento) → acota el uso de cada clave a un segmento y aísla el radio de daño.
  **Nonce de 96-bit aleatorio por bloque** (robusto ante truncado/re-append: nonce fresco cada vez;
  con DEK por segmento el nº de cifrados por clave queda muy por debajo del límite RBG). Frame de
  bloque autodescriptivo `version|flags|base_offset|record_count|ct_len|nonce|tag|ciphertext`;
  `base_offset`/`record_count` en claro **autenticados** (AAD) para permitir *traversal* sin
  descifrar (offsets/tamaños no son secretos; claves/valores sí van cifrados). **Cabecera de
  segmento** (magic+salt) al inicio del `.log` → autodetección cifrado/plano y logs mixtos.
  **Integridad on-disk = tag GCM** (supera al CRC para ciphertext); el CRC32C del batch se conserva
  **dentro** del plaintext (defensa en profundidad). Rotación de claves = futuro documentado.
- 2026-07-12 — **Tiered (Hito B, → ADR-0032):** puerto `StorageTier` + impl. local (directorio
  objeto). Offload solo de segmentos **sellados**; reclamación local **solo tras offload
  confirmado**; lectura transparente. Offload del **ciphertext tal cual** (interop con Hito A).
- 2026-07-12 — **Exactly-once (Hito C, → ADR-0033 + ADR-0034 reconciliación 2PC):** reutiliza
  `ProducerSession` (producer_id + época de fencing + dedup). Añade Transaction Coordinator (Raft
  propio), control records COMMIT/ABORT (record type nuevo, versionado), LSO + nivel de aislamiento,
  API nativa. 2PC **logueado y recuperable** (no el 2PC en memoria bloqueante prohibido por la
  biblioteca) → ADR que lo reconcilia.

## Estado actual

Hito A en curso. Arranque completado (entorno verificado, parto en verde: 789/789 en `linux-gcc`).
**Siguiente:** primer commit del Hito A = primitiva cripto (`EncryptionKey`/`SegmentCipher`) con
TDD rojo→verde. Aún sin código nuevo commiteado.

## Checklist

### Hito A — Cifrado en reposo (ADR-0031)
- [ ] **A1 · Primitiva cripto** — `segment_crypto.{hpp,cpp}`: `EncryptionKey` (from_hex/bytes,
  HKDF-SHA256 → DEK), `SegmentCipher` (AES-256-GCM seal/open por bloque, nonce aleatorio),
  `encryption_available()`. CMake enlaza `OpenSSL::Crypto` bajo `NEXUS_HAVE_OPENSSL`. Criterio:
  tests unitarios rojo→verde de round-trip, tag-mismatch→Corrupt, HKDF determinista, nonces
  distintos; `GTEST_SKIP` si no hay OpenSSL. Verde en gcc+clang+ASan.
- [ ] **A2 · Framing del segmento** — `Segment::create/open` aceptan `const EncryptionKey*`
  (default null = plano). Cabecera de segmento + bloques cifrados; `data_start_`; read/recover/
  position_of/rebuild_index sobre bloques. Criterio: los 789 tests siguen verdes (plano intacto) +
  nuevos tests de segmento cifrado (round-trip, seal/reopen, recover cola torn, truncate+append).
- [ ] **A3 · Detección de manipulación + durabilidad** — tests: 1 byte alterado en ciphertext →
  fallo autenticado (no datos silenciosos); reinicio recupera y descifra; property-based del formato
  de bloque; invariante no-reutilización de nonce. Verde bajo ASan.
- [ ] **A4 · Plumbing daemon** — `LogConfig` lleva `shared_ptr<const EncryptionKey>`; flag
  `--encryption-key` / env `NEXUS_ENCRYPTION_KEY`; propagación topic_manager→PartitionLog→Segment.
  Criterio: e2e sin clave = comportamiento de hoy; con clave, datos en disco cifrados.
- [ ] **A5 · Docs + ADR + PDF** — ADR-0031 + índice; caps. 09/25/26/27 + `30`; `docs/protocol.md`
  (formato on-disk); README (no-objetivos, conteo ADR). Regenerar PDF. Puerta de calidad completa
  (gcc, clang, ASan, format, tidy) verde. Commit + push.

### Hito B — Tiered storage (ADR-0032)
- [ ] **B1 · Puerto `StorageTier`** + DTOs (interfaz + degradación limpia).
- [ ] **B2 · Impl. local** (directorio objeto) + metadatos de segmentos en tier.
- [ ] **B3 · Ciclo sellar→offload→reclamar→leer** transparente; idempotencia; reclamación solo tras
  confirmación; interop con cifrado (offload ciphertext).
- [ ] **B4 · Plumbing + métricas** (observabilidad del tier) + config/política.
- [ ] **B5 · Docs + ADR + diagrama + PDF** (09/25/26/12 + `30`). Puerta de calidad (incl. ASan).

### Hito C — Exactly-once multi-partición (ADR-0033 + ADR-0034)
- [ ] **C1 · Codec de control records** COMMIT/ABORT (record type versionado) + property-based.
- [ ] **C2 · Transaction Coordinator** (FSM sin E/S, Raft propio) — begin/prepare/commit/abort.
- [ ] **C3 · LSO por partición + nivel de aislamiento + filtrado de abortados** (consumidor).
- [ ] **C4 · Fencing por época** (reutiliza `ProducerSession`) + API cliente.
- [ ] **C5 · Simulación determinista** (reloj/red virtuales, RNG sembrado): commit atómico visible;
  abort invisible; fallo de coordinador a mitad → resuelve; fencing; chaos. Verde ASan + TSan.
- [ ] **C6 · Docs + ADR(s) + diagramas + PDF** (10/13/16/21/12 + `30` + `protocol.md`).

### Cierre
- [ ] Actualizar `30-limitaciones-y-trabajo-futuro.md`, README; proponer etiqueta **v1.0**; resumen
  por hito. Puerta de calidad verde en ambos compiladores + ASan/TSan.

## Notas / riesgos

- Build sobre `/mnt/c` desde WSL es más lento que nativo; se asume por simplicidad (un solo árbol
  editable + firma Windows). Builds incrementales lo mitigan.
- 6 tests de compresión hacen `GTEST_SKIP` (lz4/zstd ausentes): esperado (degradación limpia).
- Cifrado: la cabecera de segmento al inicio del `.log` desplaza el *scan* (`data_start_`); vigilar
  que el camino **plano** quede byte-idéntico al de hoy (regresión: 789 tests).
