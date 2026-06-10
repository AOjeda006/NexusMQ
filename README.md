# NexusMQ

> **Broker de mensajería distribuido de alto rendimiento** en C++20, con arquitectura
> **shared-nothing thread-per-core** y **Raft por partición**.
>
> ⚠️ **Estado: en desarrollo — Fase 1.** Documento y código **provisionales**; se revisarán
> antes de publicar en el portfolio.

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
- **I/O por *completions* (proactor).** Abstracción común a **io_uring** (Linux, primero) e **IOCP**
  (Windows, después), sobre la que se asientan las **corutinas de C++20**.
- **Protocolo binario propio** (framing + multiplexing por *correlation ID* + versionado) con
  **gateway REST** para interoperabilidad. Subconjunto Kafka-compatible diferido a *stretch*.
- **Capa de *ingress* en dos modos:** cliente nativo directo al líder (primario) + proxy/REST (opt-in).

La estructura prevista son **14 *targets* CMake** en 4 áreas (núcleo de librerías, ejecutables,
cliente, pruebas). Ver el desglose para el grafo de dependencias y el detalle por clase.

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

> El esqueleto compilable (CMake + vcpkg + CI + primer test) es el hito **M1** de la Fase 1; estas
> instrucciones se completan a medida que aterriza. Objetivo primario: **Linux x86-64**.

Requisitos previstos: **CMake ≥ 3.25**, un compilador C++20 (GCC/Clang), **vcpkg** y **Ninja**.

```bash
# (previsto, M1) configurar y compilar con un preset
cmake --preset linux-gcc
cmake --build --preset linux-gcc

# (previsto) ejecutar las pruebas
ctest --preset linux-gcc
```

Dependencias (vía vcpkg, modo *manifest*): `liburing` (solo Linux), `openssl`, `lz4`, `zstd`,
`fmt`, `gtest`, `benchmark`. Todo el *toolchain* y las dependencias son **gratuitos y open source**:
desarrollo, testing y *benchmarking* se realizan **íntegramente sin coste** en una máquina local
(cluster de 3 nodos vía Docker Compose).

## Documentación

Todo el diseño vive en **[`DocumentacionProvisional/`](DocumentacionProvisional/)**:

- **[`anteproyecto.md`](DocumentacionProvisional/anteproyecto.md)** — visión, alcance, arquitectura
  y decisiones de arquitectura (**ADR-0001..0009**). Fuente de verdad del *qué* y el *porqué*.
- **[`Desglose/nexusmqdesglose.md`](DocumentacionProvisional/Desglose/nexusmqdesglose.md)** — vista de
  conjunto (targets, dependencias, fases).
- **[`Desglose/nexusmqdesglosedetallado.md`](DocumentacionProvisional/Desglose/nexusmqdesglosedetallado.md)**
  — el plano de implementación (clases, campos, métodos).
- **[`hoja-de-ruta.md`](DocumentacionProvisional/hoja-de-ruta.md)** — plan de desarrollo vivo.

## Licencia

**PolyForm Noncommercial License 1.0.0** — ver [`LICENSE.md`](LICENSE.md). Provisional/revisable,
coherente con el resto del portfolio del autor. © 2026 Andrés Ojeda Rodríguez.
