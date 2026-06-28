# NexusMQ

> **Broker de mensajería distribuido de alto rendimiento** en C++23, con arquitectura
> **shared-nothing thread-per-core** y **Raft por partición**.
>
> ⚠️ **Estado: en desarrollo — Fases 1→4 implementadas** (motor de log, reactor+broker,
> Raft por partición, ingress/operación y *stretch*). Código y documentación **provisionales**;
> se revisarán antes de publicar en el portfolio. **GitHub Actions desactivadas temporalmente**
> (cuota; se reactivan al publicar); la puerta de calidad se mantiene en local.

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

La solución son **15 librerías `nexus-*`** (núcleo) más los ejecutables (`nexusd`, `nexus-cli`,
`nexus-bench`, `nexus-loadgen`), las *tools* de soporte y las pruebas. Ver el desglose para el grafo
de dependencias y el detalle por clase.

### Roadmap por fases

| Fase   | Nombre                      | Entregable demoable |
| ------ | --------------------------- | ------------------- |
| **1**  | Storage engine              | Motor de log monopartición con cifras de rendimiento. **I/O bloqueante, sin reactor.** |
| **1b** | Reactor + broker monolítico | Broker de un nodo *thread-per-core*: publicar/consumir con cliente nativo. |
| **2**  | Distribución                | Cluster tolerante a fallos (Raft por partición, *failover*). |
| **3**  | Ingress + operación         | Plataforma operable y observable (TLS, REST admin, CLI, métricas). |
| **4**  | *Stretch*                   | Idempotencia, DLQ, compactación, LZ4/Zstd, *direct I/O*, subconjunto Kafka, IOCP. |

El plan detallado y vivo está en **[`DocumentacionProvisional/hoja-de-ruta.md`](DocumentacionProvisional/hoja-de-ruta.md)**.

## Build

> Objetivo primario: **Linux x86-64**. El árbol completo compila y pasa la suite con GCC/libstdc++
> y Clang/libc++; Windows (MSVC + clang-cl) está verificado en runtime (ADR-0023/0028).

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

Todo el diseño vive en **[`DocumentacionProvisional/`](DocumentacionProvisional/)**:

- **[`anteproyecto.md`](DocumentacionProvisional/anteproyecto.md)** — visión, alcance, arquitectura
  y decisiones de arquitectura (**ADR-0001..0029**). Fuente de verdad del *qué* y el *porqué*.
- **[`docs/`](docs/)** — contratos as-built: [`protocol.md`](docs/protocol.md) (protocolo binario),
  [`kafka.md`](docs/kafka.md) (subset Kafka), [`openapi.yaml`](docs/openapi.yaml) (REST admin) y
  [`benchmarks.md`](docs/benchmarks.md) (latencias).
- **[`Desglose/nexusmqdesglose.md`](DocumentacionProvisional/Desglose/nexusmqdesglose.md)** — vista de
  conjunto (targets, dependencias, fases).
- **[`Desglose/nexusmqdesglosedetallado.md`](DocumentacionProvisional/Desglose/nexusmqdesglosedetallado.md)**
  — el plano de implementación (clases, campos, métodos).
- **[`hoja-de-ruta.md`](DocumentacionProvisional/hoja-de-ruta.md)** — plan de desarrollo vivo.

## Licencia

**PolyForm Noncommercial License 1.0.0** — ver [`LICENSE.md`](LICENSE.md). Provisional/revisable,
coherente con el resto del portfolio del autor. © 2026 Andrés Ojeda Rodríguez.
