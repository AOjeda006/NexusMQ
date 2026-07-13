# NexusMQ

[![CI](https://github.com/AOjeda006/NexusMQ/actions/workflows/ci.yml/badge.svg)](https://github.com/AOjeda006/NexusMQ/actions/workflows/ci.yml)

NexusMQ es un *message broker* de **log particionado** (estilo Kafka): productores **añaden**
registros al final de un log append-only y consumidores **leen** desde el offset que elijan y
avanzan a su ritmo. Se construye **desde cero** como vehículo de aprendizaje y pieza de portfolio:
el objetivo es demostrar **profundidad técnica en un sistema coherente** (correctitud distribuida
+ latencia + funcionamiento *end-to-end*) y **criterio de ingeniería**, no acumular funcionalidades.

No pretende competir con Kafka en producción, sino **demostrar**, en una base de código propia y
comprensible, las técnicas que hacen viable un sistema así. La arquitectura es la **frontera actual**
del diseño de brokers (la que usa Redpanda), no la de hace una década.

## Arquitectura (resumen)

- **Shared-nothing thread-per-core.** Un **reactor por núcleo físico**, *pinned* a su CPU, dueño
  exclusivo de su anillo io_uring, su *allocator* y su subconjunto de particiones. **No hay estado
  mutable compartido** entre reactores: se comunican por **paso de mensajes** (colas SPSC *cross-core*).
  Elimina la clase de bug más cara (carreras sobre estado compartido).
- **Raft por partición.** Cada partición es su propio **grupo Raft**: su log replicado *es* el log de
  la partición (el WAL). El *high-watermark* es el `commitIndex` de Raft; el modelo de `acks`
  (`0`/`1`/`quorum`) se asienta sobre su *commit*. Postura **CP** (consistencia sobre disponibilidad).
- **I/O por *completions* (proactor).** Abstracción común a **io_uring** (Linux) e **IOCP**
  (Windows): el port a Windows está **verificado en runtime con MSVC** (VS 2026, `/W4 /WX`; ADR-0023),
  con `nexusd` completo portado (señales, afinidad y backend por plataforma; ADR-0028). Sobre el
  proactor se asientan las **corutinas de C++23**.
- **Protocolo binario propio** (framing + multiplexing por *correlation ID* + versionado) con
  **gateway REST** para administración y un **subconjunto Kafka-compatible** ya implementado
  (listener en `--kafka-port`, interop `kcat`; ADR-0029).
- **Capa de *ingress* en dos modos:** cliente nativo directo al líder (primario) + proxy/REST (opt-in).
- **Cifrado en reposo opcional.** El log en disco puede cifrarse con **AES-256-GCM** (AEAD por bloque
  de escritura, DEK por segmento derivada de una KEK de entorno; ADR-0031), reutilizando la misma
  dependencia OpenSSL que TLS. Sin clave, el log se escribe en claro (degradación limpia).
- **Almacenamiento por niveles opcional.** Con `--tier-dir` el broker **descarga los segmentos
  sellados fríos** a un puerto `StorageTier` (adaptador local por defecto, listo para S3), reclama el
  disco local y **rehidrata** de forma transparente al leerlos (ADR-0032); interopera con el cifrado.
  Sin el flag, no descarga nada (degradación limpia).
- ***Exactly-once* multi-partición nativo.** Transacciones con un `TransactionCoordinator` (2PC
  **logueado y recuperable**, no bloqueante), **marcadores de control** COMMIT/ABORT, **LSO** y lectura
  `read_committed` (ADR-0033 / ADR-0034). Es *effectively-once* **honesto** (deduplicación + visibilidad
  atómica), validado por **simulación determinista** con *failover* del coordinador.

La solución son **15 librerías `nexus-*`** (núcleo) más los ejecutables (`nexusd`, `nexus-cli`,
`nexus-bench`, `nexus-loadgen`), las *tools* de soporte y las pruebas. Ver el
[diagrama de dependencias](docs/diagramas/03-grafo-dependencias.md) y el
[catálogo por subsistema](docs/tecnica/18-catalogo-por-subsistema.md) para el detalle.

### Roadmap por fases

| Fase   | Nombre                      | Entregable demoable |
| ------ | --------------------------- | ------------------- |
| **1**  | Storage engine              | Motor de log monopartición con cifras de rendimiento. **I/O bloqueante, sin reactor.** |
| **1b** | Reactor + broker monolítico | Broker de un nodo *thread-per-core*: publicar/consumir con cliente nativo. |
| **2**  | Distribución                | Cluster tolerante a fallos (Raft por partición, *failover*). |
| **3**  | Ingress + operación         | Plataforma operable y observable (TLS, REST admin, CLI, métricas). |
| **4**  | *Stretch*                   | Idempotencia, DLQ, compactación, LZ4/Zstd, *direct I/O*, subconjunto Kafka, IOCP. |

El recorrido por fases, en retrospectiva, está en
**[`docs/tecnica/29-historia-de-desarrollo.md`](docs/tecnica/29-historia-de-desarrollo.md)**.

## Build

Requisitos: **CMake ≥ 3.25**, un compilador **C++23** (GCC o Clang **con libc++**), **vcpkg** y
**Ninja**.

```bash
# configurar y compilar con un preset
cmake --preset linux-gcc
cmake --build --preset linux-gcc

# ejecutar las pruebas
ctest --preset linux-gcc
```

Presets disponibles: `linux-gcc`, `linux-clang` (Clang/libc++), `linux-gcc-release` (optimizado),
`linux-gcc-asan` (ASan/UBSan), `linux-gcc-tsan` (TSan) y, en Windows, `windows-msvc` y
`windows-clang-cl`.

Dependencias (vía vcpkg, modo *manifest*): `fmt`, `gtest`, `benchmark` y `openssl`. **io_uring** se
usa **directamente sobre el uapi del kernel** (sin `liburing`; ADR-0012) y la **compresión** LZ4/Zstd
es **opcional** (se compila solo si `find_package` las encuentra; igual que OpenSSL en ADR-0019, que
degrada a texto en claro si falta). Todo el *toolchain* y las dependencias son **gratuitos y open
source**: desarrollo, testing y *benchmarking* se realizan **íntegramente sin coste** en una máquina
local (cluster de 3 nodos vía Docker Compose).

## Documentación

La documentación técnica **final** vive en **[`docs/`](docs/)**:

- **[`docs/tecnica/`](docs/tecnica/)** — la documentación técnica completa (30 capítulos en 7 partes:
  visión, arquitectura, contratos, implementación, calidad, operación y decisiones). Empieza por su
  [índice de lectura](docs/tecnica/README.md). Es la **fuente de verdad** del *qué* y el *porqué*.
- **[`docs/adr/`](docs/adr/)** — los 34 *Architecture Decision Records* (**ADR-0001..0034**), uno por
  fichero, con su [índice](docs/adr/README.md).
- **[`docs/diagramas/`](docs/diagramas/)** — los 25 diagramas (Mermaid) de arquitectura, runtime,
  almacenamiento, consenso, protocolos, ingress y operación.
- Contratos **as-built**: [`protocol.md`](docs/protocol.md) (protocolo binario),
  [`kafka.md`](docs/kafka.md) (subset Kafka), [`openapi.yaml`](docs/openapi.yaml) (REST admin) y
  [`benchmarks.md`](docs/benchmarks.md) (latencias).
- **[Documentación técnica en PDF](docs/pdf/NexusMQ-documentacion-tecnica.pdf)** — todo lo anterior
  (capítulos + diagramas + ADR) compilado en un único documento; ver [`docs/pdf/`](docs/pdf/) para
  regenerarlo.

## Licencia

**PolyForm Noncommercial License 1.0.0** — ver [`LICENSE.md`](LICENSE.md). Provisional/revisable,
coherente con el resto del portfolio del autor. © 2026 Andrés Ojeda Rodríguez.
