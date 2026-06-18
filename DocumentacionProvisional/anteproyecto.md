---
tipo: anteproyecto
proyecto: NexusMQ
version: 0.4.0
estado: borrador
revisado: 2026-06-10
autor: Andrés Ojeda Rodríguez
tags: [nexusmq, message-broker, sistemas-distribuidos, cpp20, anteproyecto, raft, storage-engine, shared-nothing, thread-per-core, io_uring, configuracion, modelo-de-errores, doxygen, observabilidad]
---

# NexusMQ — Anteproyecto y Diseño Preliminar

> **Broker de mensajería distribuido de alto rendimiento con capa de *ingress* inteligente**, escrito en C++20, con arquitectura ***shared-nothing thread-per-core*** y **Raft por partición**. Documento de pre-desarrollo: fija la visión, el alcance, el análisis tecnológico, la arquitectura, los patrones de diseño y las decisiones de arquitectura (ADRs) **antes** de escribir código.

---

## 0. Control de versiones del documento

| Versión | Fecha       | Estado   | Cambios                                                                 |
| ------- | ----------- | -------- | ----------------------------------------------------------------------- |
| 0.1.0   | 2026-06-07  | borrador | Versión inicial: visión, alcance, análisis tecnológico, arquitectura, patrones y ADR-0001/0002. ADR-0003/0004 quedan *propuestos*. |
| 0.2.0   | 2026-06-07  | borrador | **Cierra** ADR-0003 (**Raft por partición**) y ADR-0004 (**binario propio + gateway REST**). **Añade** ADR-0005 (concurrencia *shared-nothing thread-per-core* con reactor propio), ADR-0006 (*ingress* en dos modos), ADR-0007 (postura de consistencia: CP / PACELC PC-EC) y ADR-0008 (viabilidad de coste cero). Integra **12 refinamientos** (ver tabla de trazabilidad). Nuevo §4.8 (estudio de coste cero) y §7.8 (replicación), §7.9 (seguridad). |
| 0.3.0   | 2026-06-07  | borrador | **10 mejoras de profundidad** (ver §0.2), inspiradas en la metodología de documentación de proyectos previos: §3.7 (reloj y tiempo, *fija* la referencia colgante de R8), §3.8 (convenciones Doxygen + afinidad de reactor), §4.3 reescrito con **RNF cuantificados** (dev vs producción), §4.9 (objetivo de producción vs entorno de desarrollo), §5.8 (**modelo de datos interno** con campos), §5.9 (**máquinas de estado y ciclos de vida**), §7.2 ampliado (**catálogo de operaciones** + **taxonomía de errores** del protocolo), §7.6 ampliado (**REST admin** + *health/readiness*), §7.9 ampliado (*hardening*), §7.10 (**catálogo de configuración**), §7.11 (**pseudocódigo crítico**), §8.1 (**matriz de testing**), §8.5 (**runbook**). |
| 0.3.1   | 2026-06-07  | borrador | Nota aclaratoria: la **solución única** (árbol CMake) es **multiplataforma** y **no** bloquea el *target* Windows posterior — el mismo árbol cambia de *preset* a MSVC y solo añade el adaptador IOCP en `src/io/` (ampliado en §5.2 y en las consecuencias de ADR-0001). |
| 0.4.0   | 2026-06-10  | borrador | **Conformidad con la normativa de referencia de C++/sistemas:** REST admin a **`/api/v1`** + **RFC 7807** + paginación + OpenAPI + **JWT** (§7.6); convenciones **Docker** (multi-stage→distroless, no-root, *healthcheck*, *scan*) (§5.3); build con **`-Werror`** + `clang-tidy`/`clang-format`/`cppcheck` (§3.2); **Conventional Commits** + ramas + *build-once-promote* (§8.4); **`alignas` (false sharing)** + manejo de **ABA** en colas lock-free (§6.3); **apagado limpio** `SIGTERM`/`SIGINT` (§7.4); **TDD** + naming de tests (§8.1). **Nuevo ADR-0009** (política de manejo de errores por capa). Estado de ADR-0003/0005 saneado a `aceptado` (la nota "provisional revisable" pasa al cuerpo). |
| 0.5.0   | 2026-06-11  | borrador | **Nuevo ADR-0010** (entorno de desarrollo: migración del IDE de **Visual Studio a VS Code sobre WSL** —*Remote-WSL* + *CMake Tools*—; reemplaza la elección de IDE de ADR-0001, que por lo demás sigue vigente). Actualiza §3.2 (*toolchain*), la tabla de decisiones y el índice de ADRs (§6.7). Presets **estandarizados en `linux-*` (Ninja)**; se descartan los `wsl-ubuntu*` y los *vendor maps* de Visual Studio. |
| 0.6.0   | 2026-06-11  | borrador | **Nuevo ADR-0011** (estándar **C++23** + **libc++ en Clang** para `std::expected`, mecanismo del modelo de errores de ADR-0009). Sube `cxx_std_23`; GCC con libstdc++, Clang con `-stdlib=libc++` (preset `linux-clang` y CI, incluido clang-tidy). Actualiza §3.1/§3.2 y la tabla de decisiones. |
| 0.7.0   | 2026-06-13  | borrador | **Nuevo ADR-0013** (capa **`nexus-wire`** para el framing sobre conexión —`FrameReader`/`FrameWriter`—; `nexus-protocol` se mantiene puro). Sincroniza el catálogo de ADRs (§6.7) con **ADR-0012** (backend io_uring directo sobre el uapi, sin liburing) y ADR-0013. |
| 0.8.0   | 2026-06-14  | borrador | **Nuevo ADR-0014** (modelo del **log de Raft**, Fase 2): una entrada de Raft ↔ un `RecordBatch`; el **índice de Raft es el ordinal de entrada** (espacio distinto del offset por record); `RaftLog` envuelve el `PartitionLog` y persiste `term`/offsets por entrada en un **sidecar** de tamaño fijo, dejando el `RecordBatch` y `nexus-storage` **sin cambios**. Actualiza el catálogo de ADRs (§6.7). |
| 0.9.0   | 2026-06-14  | borrador | **Nuevo ADR-0015** (`RaftNode`, Fase 2): núcleo de Raft como **máquina de estados síncrona sin E/S** (entradas `tick`/`on_*` con `now` inyectado → cola de mensajes drenable), en vez del par corrutina `propose` + `RaftTransport` del desglose; habilita la **simulación determinista** (reloj/red virtuales). Actualiza el catálogo de ADRs (§6.7). |

> **Naturaleza del documento.** Es un documento **vivo** en estado `borrador`. Las secciones de diseño detallado por subsistema (§7) y la estrategia de calidad (§8) se profundizarán por fases. Una vez `aceptado`, un ADR no se edita: se reemplaza por otro nuevo. La decisión de arquitectura central (ADR-0005) se fija como **provisional revisable**: se reconsiderará a la luz de los *benchmarks* de la Fase 1.

### 0.1 Refinamientos integrados en v0.2.0 (trazabilidad)

Los 12 refinamientos acordados se reparten por el documento; esta tabla permite auditarlos:

| #   | Refinamiento                                  | Aterriza en                                  |
| --- | --------------------------------------------- | -------------------------------------------- |
| R1  | Semánticas de entrega precisas                | §7.3, §10                                     |
| R2  | Modelo de ACK y durabilidad                   | §4.3 (RNF), §5.5, §7.8                         |
| R3  | Postura de consistencia (CAP / PACELC)        | §5.7, §4.3 (RNF), ADR-0007                     |
| R4  | Metodología de benchmarks (rigor anti-CO)     | §8.2                                           |
| R5  | Backpressure por créditos                     | §6.3, §6.4, §7.5                               |
| R6  | I/O de storage (*direct I/O* + caché propia)  | §3.3, §7.1, ADR-0002 (matiz)                   |
| R7  | Seguridad (TLS/mTLS, authn/authz)             | §4.3 (RNF), §4.4, §7.9                         |
| R8  | Reloj y tiempo (monotónico vs *wall-clock*)   | §3.7, §10                                      |
| R9  | Tabla de técnicas revisada (shared-nothing)   | §3.5                                           |
| R10 | Coordinación entre núcleos (*cross-core*)     | §5.1, §6.3, §7.4                               |
| R11 | Modelo de memoria por núcleo (NUMA/hugepages) | §3.5, §5.1, §6.3                               |
| R12 | Formato de registro / *record batch*          | §5.4, §7.1                                     |

### 0.2 Mejoras de profundidad integradas en v0.3.0 (trazabilidad)

Diez mejoras destinadas a llevar el documento de "diseño de alto nivel" a "diseño construible", con el rigor de poder derivar de él el desglose de la solución y la operación. Esta tabla permite auditarlas:

| #   | Mejora                                                  | Aterriza en          |
| --- | ------------------------------------------------------- | -------------------- |
| M1  | Catálogo de configuración (claves + defaults)           | §7.10                |
| M2  | Taxonomía de errores del protocolo (*wire*)             | §7.2.2               |
| M3  | Modelo de datos interno (estructuras y campos)          | §5.8                 |
| M4  | Catálogo de operaciones (protocolo binario + REST admin)| §7.2.1, §7.6         |
| M5  | Pseudocódigo crítico                                    | §7.11                |
| M6  | RNF cuantificados (desarrollo vs producción)            | §4.3                 |
| M7  | Objetivo de producción vs entorno de desarrollo         | §4.9                 |
| M8  | Convenciones de documentación de código (Doxygen)       | §3.8                 |
| M9  | Máquinas de estado y ciclos de vida                     | §5.9                 |
| M10 | Matriz de testing + *runbook* + *health/readiness*      | §8.1, §8.5, §7.6     |

> Además, v0.3.0 **corrige** la referencia colgante de R8 (reloj): apuntaba a un §3.7 que no existía en v0.2.0; ahora §3.7 está escrito.

---

## Tabla de contenidos

1. [Introducción](#1-introducción)
2. [Visión del producto](#2-visión-del-producto)
3. [Análisis de tecnologías](#3-análisis-de-tecnologías)
4. [Alcance y planificación](#4-alcance-y-planificación)
5. [Arquitectura del sistema](#5-arquitectura-del-sistema)
6. [Patrones de diseño y principios](#6-patrones-de-diseño-y-principios)
7. [Diseño preliminar de subsistemas](#7-diseño-preliminar-de-subsistemas)
8. [Estrategia de calidad](#8-estrategia-de-calidad)
9. [Decisiones de arquitectura (ADRs)](#9-decisiones-de-arquitectura-adrs)
10. [Glosario](#10-glosario)
11. [Referencias y bibliografía](#11-referencias-y-bibliografía)
12. [Anexos](#12-anexos)

---

## 1. Introducción

### 1.1 Contexto y motivación

Los sistemas distribuidos modernos se construyen, cada vez más, alrededor de una **columna vertebral de mensajería asíncrona**: en lugar de que los servicios se llamen entre sí de forma síncrona y acoplada, publican y consumen eventos a través de un *broker* que los desacopla en el tiempo y en el espacio. Apache Kafka popularizó el modelo del **log distribuido append-only particionado** como sustrato común para *event streaming*, *event sourcing*, integración de datos y comunicación entre microservicios.

Construir un broker de esta clase **desde cero** obliga a enfrentarse, sin atajos, a los problemas centrales de la ingeniería de sistemas: durabilidad ante fallos, consenso y replicación, control de concurrencia de alto rendimiento, gestión explícita de memoria, I/O asíncrona eficiente y diseño de protocolos de red. Es, por tanto, un vehículo idóneo para demostrar dominio de C++ moderno y de los fundamentos de los sistemas distribuidos.

NexusMQ no se posiciona en la frontera de 2011 (broker con *thread-pool* + ISR clásico), sino en la **frontera actual**: una arquitectura ***shared-nothing thread-per-core*** sobre **io_uring**, con **Raft por partición** como mecanismo único de replicación y consenso. Es el diseño que usa Redpanda para batir a Kafka en latencia, y es a la vez el más ambicioso, el más coherente y el más demostrable como correcto (ver §5 y ADR-0005).

### 1.2 Propósito del documento

Este documento es el **anteproyecto** de NexusMQ. Su función es:

- Dejar por escrito **qué** se va a construir y, sobre todo, **qué no** (alcance).
- Justificar las **decisiones tecnológicas** con criterio de ingeniería, no por moda.
- Establecer la **arquitectura** y los **patrones** que vertebran el sistema.
- Registrar las **decisiones de arquitectura (ADRs)** y dejar abiertas las que aún se debaten.
- Servir de **mapa compartido** y de pieza de portfolio que evidencia el proceso de diseño, no solo el resultado.

### 1.3 Objetivos

**Objetivo general.** Diseñar e implementar, por fases, un broker de mensajería distribuido de alto rendimiento en C++20, con arquitectura *shared-nothing thread-per-core*, persistencia durable, replicación con consenso (Raft por partición) y una capa de *ingress* (proxy/gateway) inteligente, acompañado de herramientas de operación y observabilidad.

**Objetivos específicos.**

- **OE-1.** Construir un *storage engine* de log append-only con segmentación, índice disperso, *checksums* (CRC32C) y política de durabilidad configurable.
- **OE-2.** Definir e implementar un **protocolo binario propio** con *framing*, *multiplexing* y versionado, una librería cliente nativa en C++ y un **gateway REST** para interoperabilidad.
- **OE-3.** Implementar el núcleo del broker sobre un **reactor *thread-per-core* propio**: *topics*, particiones (cada una anclada a un núcleo), grupos de consumidores y semánticas de entrega.
- **OE-4.** Dotar al sistema de **replicación con consenso mediante Raft por partición** (elección de líder y *failover* automático) para tolerancia a fallos.
- **OE-5.** Construir la **capa de *ingress*** en dos modos (cliente nativo directo al líder + proxy/REST), con terminación TLS, *rate limiting*, *circuit breaker*, *health checks* y *load balancing*.
- **OE-6.** Proporcionar **observabilidad** de grado producción (métricas Prometheus, logs estructurados, *tracing*) y herramientas de administración (API REST + CLI).
- **OE-7.** Demostrar, con **benchmarks reproducibles y metodológicamente rigurosos**, el impacto de las técnicas de alto rendimiento aplicadas (throughput y latencia con percentiles, sin *coordinated omission*).

### 1.4 Audiencia y propósito de portfolio

El documento se dirige, en primer término, al propio autor como guía de construcción; y, en segundo término, a **revisores técnicos** (entrevistadores, colaboradores) que evalúen el proyecto como muestra de competencia en sistemas e infraestructura, C++ de bajo nivel y desarrollo *backend*. El proyecto busca evidenciar **profundidad técnica en un sistema coherente** (correctitud distribuida + latencia + funcionamiento *end-to-end*) y **criterio** (decisiones justificadas y medidas), por encima de la mera acumulación de funcionalidades. La ambición se persigue como **profundidad dentro de una sola tesis arquitectónica**, nunca como amplitud (dos arquitecturas en paralelo sería *scope sprawl*, no ambición).

### 1.5 Cómo leer este documento

Las secciones §2–§6 son **estables** y deberían leerse en orden: establecen el *qué*, el *con qué* y el *cómo* a alto nivel. La §7 es **preliminar** y crecerá por fases. La §9 (ADRs) puede consultarse de forma independiente y es donde viven las decisiones vinculantes. El §10 (glosario) define los términos de dominio que se usan sin más aclaración en el resto del texto.

---

## 2. Visión del producto

### 2.1 ¿Qué es un *message broker* y qué problema resuelve?

Un *message broker* es un intermediario que recibe mensajes de **productores** y los pone a disposición de **consumidores**, desacoplándolos. NexusMQ adopta el modelo de **log particionado** (estilo Kafka):

- Un **topic** es un flujo lógico de mensajes, dividido en **particiones**.
- Cada partición es un **log append-only**: una secuencia ordenada e inmutable de registros, cada uno identificado por un **offset** monótono.
- Los productores **añaden** al final del log; los consumidores **leen** desde el offset que elijan y avanzan a su ritmo (el broker no borra al entregar: retiene por tiempo/tamaño).

Este modelo resuelve el desacoplo temporal (productor y consumidor no necesitan estar vivos a la vez), permite **re-lectura** (varios consumidores independientes, *replay* histórico) y escala horizontalmente por particiones.

### 2.2 Propuesta de valor y diferenciadores

NexusMQ no pretende competir con Kafka en producción, sino **demostrar**, en una base de código propia y comprensible, las técnicas que hacen viable un sistema así. Sus diferenciadores como proyecto:

- **Arquitectura *shared-nothing thread-per-core* con reactor propio.** No hay *thread-pool* con estado compartido bajo *locks*: cada núcleo es un reactor independiente (su *event loop* io_uring, su *allocator*, su subconjunto de particiones) y se comunican por **paso de mensajes** explícito. Esto elimina la clase de bug más cara (carreras sobre estado mutable compartido) y es la frontera actual del diseño de brokers, no la de hace 14 años.
- **Capa de *ingress* integrada**, no como pieza externa (HAProxy/Nginx) sino como parte del producto, y con **dos modos** explícitos (cliente nativo directo al líder + proxy/REST), demostrando criterio en vez de "un *ingress* que lo hace todo".
- **C++ de alto rendimiento con criterio**: cada técnica avanzada (lock-free, SIMD, *allocators* por núcleo, io_uring) se introduce donde un *profiling* lo justifica y se acompaña de su *benchmark* antes/después, con metodología anti-*coordinated-omission*.
- **Portabilidad por diseño**: Linux primero, con una abstracción de I/O (*proactor*) preparada para un backend Windows (IOCP) posterior (ver ADR-0001 y ADR-0002).

### 2.3 Casos de uso objetivo

- **Streaming de eventos** entre microservicios (publicación/suscripción durable).
- **Cola de trabajo** con grupos de consumidores y reparto de carga.
- **Event sourcing / auditoría**: log inmutable re-leíble desde cualquier offset.
- **Buffer de absorción de picos** (*backpressure*) entre sistemas de ritmos distintos.

### 2.4 Análisis comparativo de referentes

| Sistema       | Lenguaje | Modelo                          | Qué tomamos como referencia                                              |
| ------------- | -------- | ------------------------------- | ----------------------------------------------------------------------- |
| **Apache Kafka** | Scala/Java | Log particionado + ISR; KRaft para metadatos | Modelo de log, particiones, grupos de consumidores, *high-watermark*, retención, formato *record batch*. |
| **Redpanda**  | **C++** (Seastar) | Kafka-compatible; **Raft por partición**; ***thread-per-core* / shared-nothing** | **Validación directa del diseño adoptado**: que un broker *thread-per-core* + Raft por partición es viable y *state-of-the-art* en C++. |
| **RabbitMQ**  | Erlang   | Colas AMQP, *routing* por *exchange* | Semánticas de entrega, *dead letter queues*, ACK/NACK. |
| **NATS / JetStream** | Go | Mensajería ligera + *streams* durables | Simplicidad del protocolo, baja latencia, *at-least-once*. |
| **Apache Pulsar** | Java | *Compute/storage* separados (BookKeeper) | Separación de capas, *tiered storage* (idea para *stretch*). |

> **Conclusión del análisis.** El modelo de **log particionado** (Kafka) es la base, incluido su **formato de *record batch*** (§5.4). **Redpanda** demuestra que la arquitectura que NexusMQ adopta —***thread-per-core* shared-nothing** + **Raft por partición**— es construible en C++ con rendimiento de primera línea; se toma como **validación de diseño**, no como dependencia (NexusMQ construye su propio reactor mínimo, no usa Seastar; ver ADR-0005). De RabbitMQ y NATS se toman semánticas de entrega y DLQ.

---

## 3. Análisis de tecnologías

### 3.1 Lenguaje: C++20/23

C++ es la elección natural para un sistema donde el control sobre **memoria, concurrencia e I/O** es el factor dominante de rendimiento. Se adopta el estándar **C++23** (`std::expected` del modelo de errores lo exige; ver **ADR-0011**). Características que el proyecto explota:

| Característica C++20            | Uso en NexusMQ                                                            |
| ------------------------------ | ------------------------------------------------------------------------ |
| **Coroutines**                 | Manejo de conexiones y de I/O de disco asíncronas sobre el *proactor* (`co_await` de operaciones de I/O); base del reactor *per-core*. |
| **Concepts**                   | Restringir serializadores, políticas de compresión y *callbacks* genéricos con contratos legibles. |
| **`std::atomic` + memory ordering** | Contadores de offset, *high-watermark*, estado del *circuit breaker*, colas SPSC *cross-core*. |
| **Ranges**                     | Iteración y transformación de *batches* de registros e índices. |
| **`std::span` / `std::byte`**  | Vistas no propietarias sobre *buffers* de red y segmentos sin copia. |
| **`constexpr` / templates**    | *Framing* del protocolo y *checksums* resueltos en tiempo de compilación donde aplique. |

### 3.2 Plataforma y *toolchain*

- **Entorno de desarrollo:** **VS Code** conectado a **WSL2** (extensiones *Remote-WSL* + *CMake Tools*), de modo que se programa y compila directamente sobre Linux. Ver **ADR-0010** (reemplaza la elección de IDE de **ADR-0001**).
- **Sistema de construcción:** **CMake** (con *presets*) como fuente de verdad — lo consumen CMake Tools, el CI y la línea de comandos sin reescribir nada; compila con GCC y Clang (MSVC en el *target* Windows posterior).
- **Gestión de dependencias:** **vcpkg** (modo *manifest*), multiplataforma; se integra vía el *toolchain* de CMake.
- **Objetivo primario:** Linux x86-64, desplegable como imagen **Docker**. Objetivo secundario posterior: Windows nativo (ver ADR-0001/0002).
- **Coste:** todo el *toolchain* y todas las dependencias son **gratuitos y open source**; el desarrollo, testing, *chaos* y *benchmarking* se realizan **íntegramente sin coste** en una máquina de desarrollo. Ver **§4.8** y **ADR-0008**.
- **Calidad de compilación:** avisos como errores (**`-Wall -Wextra -Wpedantic -Werror`** en GCC/Clang, `/W4 /WX` en MSVC); análisis estático **`clang-tidy`** (chequeos de las Core Guidelines) + **`cppcheck`**; formato con **`clang-format`** (`.clang-format` versionado); *sanitizers* (ASan/UBSan/TSan) en la build de pruebas (§8.4).

### 3.3 Modelo de I/O asíncrona

El plano de datos de un broker es, en esencia, **I/O de red y disco a gran escala**. Se adopta una abstracción de **proactor** (modelo de *completions*: se envía una operación asíncrona y se recibe su finalización), porque es la forma común a los dos backends objetivo:

- **Linux:** **io_uring** (Jens Axboe) — *completions* de alto rendimiento para red y disco. Bajo *thread-per-core*, **un anillo io_uring por núcleo**, sin compartición entre reactores.
- **Windows (posterior):** **IOCP** — *completion ports*, igualmente basado en *completions*.

Las **coroutines de C++20** se asientan de forma natural sobre un proactor (`co_await` de una operación reanuda en su *completion*). Ver **ADR-0002**.

**I/O de storage (R6).** Decisión deliberada y **escalonada**:

1. **Fase 1:** el *storage engine* arranca con **I/O de fichero bloqueante** (`pread`/`pwrite` + `fsync`) detrás de una abstracción `File`. Cero reactor, cero io_uring: se valida correctitud y durabilidad primero.
2. **Fase 1b en adelante:** se introduce io_uring **como optimización medida** (*benchmark* antes/después). Bajo el reactor *thread-per-core*, **nada bloqueante puede vivir en el reactor**: un solo `fsync` síncrono congela el núcleo entero. Por eso el disco pasa a **io_uring asíncrono** (incluido `fsync`/`fdatasync` vía `IORING_OP_FSYNC`).
3. **Profundidad opcional (Fase 4):** ***direct I/O*** (`O_DIRECT`, alineado a sector) con **caché y *readahead* propios** en lugar de apoyarse en el *page cache* del SO. Da control total sobre la memoria (coherente con *shared-nothing*) a cambio de más trabajo. Solo si el *profiling* lo justifica.

### 3.4 Dependencias

| Dependencia        | Propósito                                              |
| ------------------ | ----------------------------------------------------- |
| **liburing**       | Backend de I/O asíncrona en Linux (io_uring); un anillo por núcleo. |
| **OpenSSL**        | TLS 1.3 en la capa de *ingress* y mTLS intra-cluster (portable; evita SChannel vs OpenSSL). |
| **LZ4**            | Compresión rápida de *batches* (baja latencia).       |
| **Zstd**           | Compresión de mayor ratio (retención/almacenamiento). |
| **fmt**            | Formateo y logging estructurado.                      |
| **GoogleTest**     | Pruebas unitarias y de integración.                   |
| **Google Benchmark** | *Microbenchmarks* reproducibles.                    |

> **Política de dependencias.** Se restringen a piezas **transversales y maduras**. Lo que es **núcleo del aprendizaje** se implementa **a mano**, no se delega en librerías: *storage engine*, **protocolo**, **consenso (Raft)**, estructuras lock-free, *allocators* por núcleo y, de forma destacada, **el propio reactor *thread-per-core* sobre io_uring + corutinas** (Seastar queda como referencia conceptual, no como dependencia; ver ADR-0005).

### 3.5 Técnicas de C++ avanzado y dónde aplican (revisada para *shared-nothing*) — R9, R11

Criterio rector: **"medido, no *checklist*"**. Cada técnica se introduce donde un *profiling* la justifica y se valida con su *benchmark*.

| Técnica                                   | Dónde aparece                                                      | Justificación                                                      |
| ----------------------------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------- |
| **Reactor *thread-per-core* con afinidad**| Modelo de ejecución base: un reactor por núcleo, *pinned*.        | Elimina estado mutable compartido; cachés y NUMA locales; sin *locks* en el camino caliente. **Sustituye al *thread-pool* como modelo primario.** |
| **Colas SPSC *cross-core***               | *Handoff* entre reactores (petición llega a núcleo A, partición vive en B). | Paso de mensajes explícito entre núcleos (estilo `submit_to`) sin estado compartido. |
| **Colas lock-free SPSC**                  | *Handoff* del *pipeline* productor → broker dentro de un núcleo.  | Evitar contención en el camino caliente.                          |
| ***Allocator* de arena / *pool* por núcleo** | *Buffers* de red y mensajes en vuelo, locales a cada reactor.   | Eliminar `malloc`/`free` por mensaje (un `malloc` que entra al kernel congela el reactor); localidad; cero contención entre núcleos. |
| **NUMA-awareness + hugepages**            | *Allocators* y *buffers* por núcleo.                              | Memoria local al nodo NUMA del núcleo; menos *TLB misses* con *hugepages*. (Profundidad opcional.) |
| **io_uring (registered buffers / fixed files)** | I/O de disco y red, un anillo por núcleo.                   | *Batching* de syscalls; menos cambios de contexto; *buffers*/descriptores pre-registrados eliminan trabajo por operación. |
| **Coroutines C++20**                      | Conexiones y I/O de disco asíncronas.                            | Código secuencial legible sobre I/O por *completions*.            |
| **Templates + concepts**                  | Serialización genérica; política de compresión.                  | Abstracción sin coste en *runtime*.                               |
| **`std::atomic` + memory ordering**       | Offsets, *high-watermark*, *circuit breaker*, colas SPSC.        | Sincronización fina sin *locks* donde es seguro.                  |
| **SIMD (SSE4.2 / AVX2)**                  | **CRC32C** de *checksums*; búsqueda en índices.                  | *Checksum* por hardware (CRC32C, como Kafka), no escalar.         |

> **Nota.** Se prefiere **CRC32C** (instrucción `crc32` de SSE4.2) frente a CRC32 genérico, por ser el estándar de facto en sistemas de log y estar acelerado por hardware, con *fallback* portable por software. **El *work-stealing* del thread-pool deja de ser el modelo primario**: bajo *thread-per-core*, el reparto de carga se hace por **asignación de particiones a núcleos** (y, si hace falta, *rebalanceo* de esa asignación), no por robo de tareas sobre una cola compartida.

### 3.6 Resumen de decisiones tecnológicas

| Dimensión             | Decisión                                                | ADR        |
| --------------------- | ------------------------------------------------------- | ---------- |
| Lenguaje              | **C++23** (libc++ en Clang)                             | ADR-0011   |
| Plataforma/IDE        | Linux primero (WSL2); IDE **VS Code**; Windows después  | ADR-0001/0010 |
| Build/deps            | CMake + vcpkg                                            | ADR-0001   |
| I/O asíncrona         | Proactor; io_uring → IOCP                                | ADR-0002   |
| **Replicación**       | **Raft por partición** (un grupo Raft por partición)    | **ADR-0003** |
| **Protocolo**         | **Binario propio + gateway REST**; Kafka-subset en Fase 4 | **ADR-0004** |
| **Concurrencia**      | ***Shared-nothing thread-per-core*** (reactor propio)   | **ADR-0005** |
| **Rol del *ingress***   | **Dos modos**: nativo directo (primario) + proxy/REST   | **ADR-0006** |
| **Consistencia**      | **CP / PACELC PC-EC** (consistencia sobre disponibilidad) | **ADR-0007** |
| Coste de desarrollo   | **Cero** (toolchain y test 100% gratuitos)              | ADR-0008   |

### 3.7 Reloj y tiempo (R8)

Un broker maneja dos nociones de tiempo que **no deben mezclarse**:

- **Reloj monotónico** (`std::chrono::steady_clock`): para todo lo que mide **intervalos** y exige no retroceder — *timeouts* de petición, *heartbeats* y **el *election timeout* de Raft**, *backoff*, *health checks*. Nunca se usa el *wall-clock* para esto: un salto de NTP hacia atrás podría disparar elecciones espurias o falsos *timeouts*.
- **Reloj de pared** (`std::chrono::system_clock`, sincronizado por NTP): **solo** para *timestamps* de `Record` (visibles al usuario) y para la **retención por tiempo**. No interviene en la correctitud del consenso ni en la ordenación: el orden lo da el **offset** (monótono por construcción), no la marca de tiempo.

La ordenación dentro de una partición es por **offset**, no por *timestamp* (que puede venir sesgado del productor). Los **relojes lógicos híbridos (HLC)** y la ordenación causal entre particiones quedan **fuera de alcance**. El *skew* de reloj entre nodos se tolera porque ninguna decisión de correctitud depende del *wall-clock*.

### 3.8 Convenciones de documentación de código (Doxygen) (M8)

Todo el código se comenta en formato **Doxygen**. **Identificadores en inglés; mensajes de cara al usuario y comentarios explicativos en español.**

- **Cabecera de archivo:** `@file`, `@brief`, `@ingroup` (subsistema: `storage`, `reactor`, `protocol`, `consensus`, `broker`, `ingress`).
- **Clase/struct:** descripción, **invariantes**, y —específico de *shared-nothing*— una anotación de **afinidad** obligatoria: `REACTOR-LOCAL` (pertenece a un reactor; no es *thread-safe*), `INMUTABLE/COMPARTIBLE` (solo lectura tras construir), o `CROSS-CORE` (solo se comunica por paso de mensajes). Esta anotación documenta la invariante de concurrencia que sostiene la arquitectura.
- **Método:** `@param`, `@return`, `@pre`/`@post` (precondiciones/postcondiciones), `@throws`. En el camino caliente, anotar si es `noexcept` y si puede `co_await` (suspender).
- **Inline:** solo para decisiones no obvias; nunca comentar lo evidente. Notas especiales `// TODO:` y `// PERF:` (justificación de una optimización medida).

```cpp
/// @file partition_log.hpp
/// @brief Log append-only de una partición (secuencia de segmentos).
/// @ingroup storage
///
/// Afinidad: REACTOR-LOCAL. Pertenece al reactor dueño de la partición;
/// NO es thread-safe y nunca se accede desde otro núcleo salvo por paso
/// de mensajes cross-core. Invariante: log_end_offset es monótono.
class PartitionLog { /* ... */ };
```

---

## 4. Alcance y planificación

### 4.1 Alcance funcional por módulos

1. **Ingress Layer** — proxy/gateway en dos modos: TLS 1.3, *rate limiting* (*token bucket*), *circuit breaker*, *health checks*, *load balancing*, *retry* con *backoff* + *jitter*, y **gateway REST**.
2. **Core Broker** — *topics*, particiones (ancladas a núcleo), grupos de consumidores, gestión de offsets, semánticas de entrega, DLQ, TTL, *batching*/compresión, **backpressure por créditos**.
3. **Consenso y replicación** — **Raft por partición**: elección de líder, replicación de log, *failover* automático, *high-watermark* derivado del *commit index* de Raft.
4. **Protocolo de red** — *framing*, *multiplexing*, versionado; **librería cliente C++ nativa** + **gateway REST**.
5. **Storage engine** — WAL, segmentos, índice disperso, *mmap*/io_uring, retención y compactación.
6. **Admin & observabilidad** — API REST, CLI (`nexus-cli`), métricas Prometheus, logs JSON, *tracing*.

### 4.2 Requisitos funcionales (selección)

- **RF-01.** Un productor puede publicar mensajes en un *topic*/partición y recibir confirmación con el offset asignado y la política de *ack* solicitada.
- **RF-02.** Un consumidor puede leer desde un offset arbitrario y avanzar (commit manual y automático).
- **RF-03.** Los mensajes persisten de forma durable y sobreviven a un reinicio del broker.
- **RF-04.** Grupos de consumidores reparten particiones entre miembros con rebalanceo.
- **RF-05.** El sistema replica cada partición (grupo Raft) y elige un nuevo líder automáticamente ante la caída del actual, sin pérdida de datos *committed*.
- **RF-06.** La capa de *ingress* aplica *rate limiting* y *circuit breaking* por cliente/*topic*, y ofrece un *gateway* REST.
- **RF-07.** Operación vía API REST y CLI; métricas expuestas en formato Prometheus.

### 4.3 Requisitos no funcionales

**Cifras cuantificadas (M6).** Orientativas; se fijan definitivamente tras la Fase 1 con la metodología de §8.2 y dependen del *hardware*. Se dan en dos columnas: objetivo en el **entorno de desarrollo/portfolio** vs objetivo de **producción**.

| Requisito                                   | Desarrollo (portfolio)                          | Producción            | Notas |
| ------------------------------------------- | ----------------------------------------------- | --------------------- | ----- |
| Throughput por nodo                         | ≥ 100k–500k msg/s (batch ~16 KiB, `acks=1`)     | según SLA             | tras Fase 1 |
| Latencia *produce* p99 `acks=1`             | < 5 ms                                           | < 1 ms                | orientativo |
| Latencia *produce* p99 `acks=quorum` (3 nodos local) | < 15 ms                                  | < 5 ms                | incluye *round-trip* de quórum |
| Latencia *fetch* p99                        | < 5 ms                                           | < 2 ms                | desde *page cache*/mmap |
| Tamaño máx. de mensaje                      | 1 MiB (`max.message.bytes`)                      | configurable          | rechazo `MESSAGE_TOO_LARGE` |
| Tamaño máx. de *batch*                      | 4 MiB (`max.batch.bytes`)                        | configurable          | |
| Retención por defecto                       | 7 días / 1 GiB por partición                     | configurable          | `retention.ms` / `retention.bytes` |
| Durabilidad                                 | 0 pérdida con `acks=quorum` tras *crash* de minoría | ídem               | ver RNF-Durabilidad |
| Disponibilidad                              | tolera 1 nodo caído (cluster 3, quórum 2)        | N+1 según RF          | postura CP (§5.7) |
| Recuperación tras *crash*                   | listo en pocos s (valida CRC + trunca cola *torn*) | ídem                | §7.11 |
| Conexiones concurrentes                     | cientos                                          | miles (`max.connections`) | |

Requisitos cualitativos:

- **RNF-Durabilidad / ACK (R2).** Modelo de *ack* configurable por petición: **`acks=0`** (sin esperar, *at-most-once*), **`acks=1`** (escrito en el líder), **`acks=quorum`** (replicado y *committed* por la mayoría del grupo Raft — **por defecto**). Ningún mensaje confirmado con `acks=quorum` se pierde mientras sobreviva la mayoría del grupo. La política de **`fsync`** (cada N mensajes / N ms / por *commit*) es ortogonal y combinable.
- **RNF-Consistencia / Disponibilidad (R3).** Postura **CP** (ver ADR-0007): ante partición de red, una partición sin quórum **deja de aceptar escrituras** antes que arriesgar divergencia. *Failover* automático sin pérdida de datos *committed* por el quórum.
- **RNF-Seguridad (R7).** TLS 1.3 en el borde (terminación en *ingress*) y opción de **mTLS** intra-cluster; autenticación de cliente (*token*/SASL-like) y autorización básica por *topic*. Cifrado **en reposo** queda fuera de alcance (ver §4.4).
- **RNF-Observabilidad.** Métricas, logs estructurados y *tracing* con *correlation IDs*.
- **RNF-Portabilidad.** Núcleo independiente de plataforma tras la abstracción de I/O (*proactor*); Linux primero, Windows después.
- **RNF-Mantenibilidad.** Separación estricta plano de datos / plano de control; cobertura de pruebas en la lógica de dominio.

### 4.4 Fuera de alcance (explícito)

Para acotar el proyecto y evitar la dispersión, **quedan fuera** (al menos hasta superar las fases nucleares):

- Compatibilidad **total** con el protocolo de Kafka (se aborda solo un **subconjunto** —`ApiVersions`/`Metadata`/`Produce`/`Fetch`— como *stretch* de Fase 4; ver ADR-0004).
- *Exactly-once* transaccional entre particiones (sí se contempla **productor idempotente** = *effectively-once* por partición).
- *Tiered storage* a almacenamiento de objetos (idea futura, inspirada en Pulsar).
- Multi-tenancy y ACLs avanzadas; **cifrado en reposo**.
- Dashboard gráfico (la observabilidad se expone vía Prometheus/CLI; un panel Grafana es opcional y *self-hosted*).
- Backend de I/O para Windows (IOCP) **en las fases iniciales** (es objetivo posterior, ver ADR-0001).
- Despliegue **de pago** en cloud: el proyecto se **orienta** a Azure/AWS por diseño, pero su desarrollo, prueba y *benchmark* **no requieren** gasto en cloud (ver §4.8).

### 4.5 Roadmap por fases

| Fase    | Nombre                         | Contenido                                                                                  | Entregable demoable |
| ------- | ------------------------------ | ------------------------------------------------------------------------------------------ | ------------------- |
| **1**   | Storage engine                 | Record + CRC32C, segmento, índice disperso, log de partición, recuperación, durabilidad, retención + **benchmarks**. **I/O bloqueante, sin reactor.** | Motor de log monopartición con cifras de rendimiento. |
| **1b**  | Reactor + broker monolítico    | **Reactor *thread-per-core* propio (io_uring + corutinas)**, protocolo binario, cliente C++, *produce*/*fetch*, gestión de offsets, **backpressure por créditos**. | Broker de un nodo *thread-per-core*: publicar y consumir con cliente nativo. |
| **2**   | Distribución                   | **Raft por partición** (ADR-0003), elección de líder, *failover*, *cross-core message passing*, grupos de consumidores y rebalanceo. | Cluster tolerante a fallos. |
| **3**   | Ingress + operación            | *Ingress* en dos modos (gateway TLS, *rate limit*, *circuit breaker*, *health checks*, **gateway REST**); API REST de admin, CLI, métricas Prometheus, logs JSON. | Plataforma operable y observable. |
| **4**   | *Stretch*                      | Productor idempotente, DLQ, compactación, LZ4/Zstd, ***direct I/O***, **subconjunto Kafka-compatible** (kcat habla con el broker), *binding* Python, *tracing*, backend IOCP (Windows). | Funcionalidades avanzadas, interoperabilidad y portabilidad. |

> Tras la **Fase 1** ya existe un artefacto demoable y *benchmarkable*. La ambición se construye **por capas**: el reactor *shared-nothing* no se construye el día 1 (Fase 1 es monohilo/bloqueante), aterriza en Fase 1b; Raft aterriza en Fase 2. Cada fase es autocontenida y demoable.

### 4.6 Hitos de la Fase 1 (detalle)

- **M1 — Esqueleto:** CMake + vcpkg + CI + primer test + *harness* de *bench* vacío.
- **M2 — Record + CRC32C:** formato en disco; CRC32C por hardware con *fallback* portable; *property tests*.
- **M3 — Segment:** fichero append-only (`.log` + `.index`) con índice disperso; `append`/`read`.
- **M4 — Log de partición:** secuencia de segmentos, *rolling*, lectura cruzando segmentos, **recuperación** al arrancar (validar CRC, truncar cola *torn*).
- **M5 — Durabilidad:** política de *fsync* (cada N mensajes / N ms); test de *crash* (`kill -9` a mitad de escritura).
- **M6 — Retención + benchmarks:** retención por tiempo/tamaño; *benchmarks* (throughput, *p50/p99/p999*, impacto de *fsync*, lectura *mmap*) con la metodología de §8.2.

### 4.7 Riesgos y mitigaciones

| Riesgo                                          | Impacto | Mitigación                                                                 |
| ----------------------------------------------- | ------- | ------------------------------------------------------------------------- |
| **Alcance excesivo** (proyecto "infinito").     | Alto    | Fases autocontenidas y demoables; *Fuera de alcance* explícito (§4.4); **una sola arquitectura**, no dos. |
| **Disciplina asíncrona** del reactor (nada bloqueante). | Alto | Fase 1 bloqueante aislada; en Fase 1b, **todo** I/O (incluido `fsync`) pasa por io_uring; *allocators* por núcleo para no entrar al kernel en el camino caliente. |
| **Complejidad del consenso** (Raft correcto).   | Alto    | Aislar el módulo; pruebas deterministas y de *chaos*; apoyarse en el *paper* de Raft y en DDIA; un grupo Raft por partición (mecánica uniforme). |
| ***Over-engineering*** prematuro (técnicas C++). | Medio   | Regla "medido, no *checklist*": *profiling* antes de optimizar.            |
| **Doble plataforma** desde el inicio.           | Medio   | Linux primero; Windows como fase posterior tras una abstracción ya pensada (ADR-0001/0002). |
| **Corrupción de datos** ante *crash*.           | Alto    | WAL + CRC32C + recuperación con truncado de cola; tests de *crash* dedicados. |

### 4.8 Estudio de viabilidad de coste cero (R-coste) — desarrollo y testing gratuitos

**Conclusión:** **el 100% del desarrollo, el testing (unitario, integración, *crash*/chaos) y el *benchmarking* se realizan sin coste alguno** en la máquina de desarrollo. La orientación a Azure/AWS es un **objetivo de diseño de despliegue**, no un requisito de gasto: nada del ciclo de construcción y validación depende de pagar cloud.

| Actividad                       | Cómo se hace gratis                                                                                 |
| ------------------------------- | -------------------------------------------------------------------------------------------------- |
| **Toolchain**                   | WSL2 (incluido en Windows), **VS Code** (Remote-WSL), GCC/Clang, CMake, Ninja, vcpkg — todos gratuitos. |
| **Dependencias**                | liburing, OpenSSL, LZ4, Zstd, fmt, GoogleTest, Google Benchmark — todas open source.               |
| **io_uring**                    | El kernel de WSL2 reciente lo soporta; si faltara, el *fallback* bloqueante de Fase 1 cubre el desarrollo. |
| **Cluster multi-nodo**          | **Docker Compose** levanta 3 nodos en una sola máquina (suficiente para probar Raft y *failover*). |
| **Crash / chaos testing**       | Local: `kill -9`, `tc netem` (latencia/pérdida/partición de red), `cgroups` (limitar CPU/mem), *namespaces*. |
| **Sanitizers / profiling**      | ASan/UBSan/TSan, `perf`, *flamegraphs*, `bpftrace` — gratuitos en Linux.                            |
| **Benchmarks**                  | *Hardware* local con núcleos aislados (`isolcpus`/`taskset`); el coste es metodológico, no monetario. |
| **CI/CD**                       | **GitHub Actions**: gratis e ilimitado en repos públicos; *free tier* (≈2000 min/mes) en privados. |
| **Observabilidad**              | Prometheus + Grafana *self-hosted* vía Docker — gratuitos.                                          |

**Cloud (Azure/AWS), opcional y solo para una demo de despliegue real:**

- **No es necesario** para desarrollar ni validar. El diseño es *cloud-ready* (imagen Docker, configuración por entorno, 12-factor), de modo que el mismo binario corre en local o en cloud sin cambios.
- Si se quiere una demo desplegada, hay ***free tiers*** suficientes para un cluster pequeño: AWS *Free Tier* (12 meses, instancias `t-micro`), Azure (crédito inicial + servicios *always-free*), **Oracle Cloud Always Free** (4 vCPU ARM Ampere — holgado para 3 nodos modestos), y **GitHub Student Pack** si aplica.
- **Aviso de coste real:** lo que sí genera gasto en cloud es el *egress* de red, el almacenamiento persistente por encima del *free tier* y dejar instancias encendidas. Mitigación: **infraestructura como código** (Terraform) para crear/destruir bajo demanda y apagar tras la demo.
- **Aviso metodológico:** los *free tiers* son recursos modestos → valen para una **demo funcional**, **no** para *benchmarks* serios. Las cifras de rendimiento se toman **en local**, con *hardware* y parámetros documentados (§8.2).

Ver **ADR-0008**.

### 4.9 Objetivo de producción vs entorno de desarrollo (por dimensión) (M7)

El proyecto se **diseña para producción** pero se **construye y valida en local**. Esta tabla hace explícita la diferencia por dimensión y por qué la versión de desarrollo es suficiente para el propósito de portfolio (mismo código, distinto entorno):

| Dimensión          | Producción (objetivo de diseño)               | Desarrollo (portfolio)                         | Justificación                                          |
| ------------------ | --------------------------------------------- | ---------------------------------------------- | ------------------------------------------------------ |
| Cluster            | 3+ nodos en máquinas/VMs separadas            | 3 nodos en **Docker Compose** (1 máquina)      | Suficiente para Raft, quórum y *failover*              |
| Despliegue         | Azure/AWS + orquestador                       | Docker local                                   | El binario es idéntico (12-factor, *cloud-ready*)      |
| NUMA / hugepages   | *hardware* multi-socket real                  | WSL2 (puede no exponer NUMA)                   | La lógica NUMA-aware se diseña; se valida donde haya HW |
| I/O de storage     | *direct I/O* (`O_DIRECT`) + caché propia      | *buffered* / io_uring                          | *Direct I/O* es profundidad de Fase 4 (§3.3)           |
| Núcleos            | muchos *cores* aislados                       | pocos *cores*; `isolcpus` para *bench*         | *Thread-per-core* escala con el nº de *cores*          |
| TLS / mTLS         | certificados de CA real                       | certificados autofirmados locales              | Mismo código de terminación TLS                        |
| Observabilidad     | Prometheus/Grafana gestionados                | Prometheus + Grafana *self-hosted* (Docker)    | Idéntico *scraping* de `/metrics`                      |
| Benchmarks         | *hardware* dedicado                           | local controlado (**no** *free tier* cloud)    | Reproducibilidad (§8.2)                                |

---

## 5. Arquitectura del sistema

### 5.1 Vista de alto nivel y modelo de ejecución *shared-nothing* (R10, R11)

NexusMQ ejecuta, en cada nodo, **un reactor por núcleo físico** (*thread-per-core*), cada uno *pinned* a su CPU. Un reactor es dueño exclusivo de: su **anillo io_uring**, su ***allocator*** (arena/pool local, NUMA-aware), y un **subconjunto de réplicas de partición**. **No hay estado mutable compartido entre reactores**; toda interacción entre núcleos es **paso de mensajes** explícito sobre colas **SPSC** (estilo `submit_to`), con despertar del destino.

```
            Clientes (Producers / Consumers)
                         │
        ┌────────────────┴───── modo nativo (primario) ──────► directo al líder de partición
        ▼  modo proxy/REST (secundario)
   ┌─────────────────────────────┐
   │        Ingress Layer        │   Proxy/Gateway:
   │  TLS · routing · rate limit │   TLS 1.3, load balancing, rate limiting,
   │  · circuit breaker · REST   │   circuit breaker, health checks, retry+backoff, REST
   └──────────────┬──────────────┘
                  ▼
   ┌──────────────────────────────────────────────────────────┐
   │                       Broker Cluster                      │
   │   Nodo 1                Nodo 2                Nodo 3       │
   │   ┌───────────┐         ┌───────────┐         ┌─────────┐ │
   │   │ core0 │core1│       │ core0│core1│        │core0│..  │ │   cada core = 1 reactor:
   │   │ [p0L]│[p3F]│        │ [p1L]│[p0F]│        │[p2L]│..  │ │   io_uring propio, allocator
   │   └───────────┘         └───────────┘         └─────────┘ │   propio, particiones propias
   │   pX L = líder Raft de la partición X ; pX F = follower   │
   │   Topics → Particiones (grupo Raft c/u) → Segmentos       │
   │   (log append-only en disco + consenso Raft)              │
   └──────────────────────┬───────────────────────────────────┘
                          ▼
            ┌─────────────────────────────┐
            │     Admin & Observabilidad  │   REST API · nexus-cli ·
            │  (REST · CLI · /metrics)    │   /metrics (Prometheus) · logs JSON
            └─────────────────────────────┘
```

### 5.2 Descomposición en módulos (estructura de repositorio prevista)

```
nexusmq/
├── CMakeLists.txt · CMakePresets.json · vcpkg.json
├── docs/                  # este anteproyecto, protocolo, ADRs
│   ├── anteproyecto.md
│   ├── protocol.md
│   └── adr/               # adr-NNNN-*.md (extraídos de §9 al crecer)
├── src/
│   ├── common/            # logging JSON, config, Buffer/Slice, crc32c, time (reloj)
│   ├── reactor/           # reactor per-core: event loop io_uring, scheduler de corutinas,
│   │                      #   cross-core SPSC (submit_to), allocator por núcleo
│   ├── io/                # abstracción proactor: io_uring (ahora), iocp (después)
│   ├── storage/           # record, segment, index, log, wal, retention
│   ├── protocol/          # framing, codec, tipos de mensaje, versioning
│   ├── consensus/         # raft: log replicado, elección, snapshots
│   ├── broker/            # topics, partitions, handlers produce/fetch, backpressure
│   ├── ingress/           # tls, rate-limit, circuit-breaker, REST gateway
│   └── server/            # listener, conexiones
├── client/cpp/            # librería cliente C++ nativa (smart-client)
├── tools/                 # nexus-cli, bench (load generator open-loop)
├── tests/                 # unit, integration, crash, chaos
└── deploy/docker/         # Dockerfile + docker-compose (cluster de 3 nodos)
```

> **Una sola solución, multiplataforma (no bloquea Windows).** Todo el árbol es **un único proyecto CMake** —con CMake no hay `.sln` ni "solución" tradicional—. Esto **no** condiciona el soporte de plataforma: "nº de soluciones" y "nº de plataformas" son ejes **independientes**. El mismo árbol compila a **Linux** (GCC/Clang) y, más adelante, a **Windows nativo** (MSVC) seleccionando *preset* en `CMakePresets.json`, **sin reestructurar**. El soporte Windows vive **solo** en `src/io/` (un adaptador **IOCP** junto al de io_uring, tras el mismo puerto *proactor*; ver ADR-0002); el resto (`reactor`, `storage`, `protocol`, `consensus`, `broker`) es **agnóstico de plataforma**. Las dependencias *platform-specific* (p.ej. `liburing`, solo Linux) se condicionan en `vcpkg.json`/CMake. *Separar* en varias soluciones **dificultaría** la portabilidad (duplicaría la costura de plataforma); por eso la **solución única es la opción correcta también para el objetivo Windows**.

### 5.3 Vista de despliegue

Cada **nodo** del broker es un proceso (imagen Docker) que ejecuta N reactores (uno por núcleo) y mantiene un subconjunto de **réplicas de partición** (líder de unas, seguidor de otras), cada partición siendo su propio **grupo Raft**. La capa de *ingress* puede desplegarse como proceso aparte o embebida. Un cluster de **3 nodos** es el objetivo de referencia (tolera la caída de uno con quórum de 2). En desarrollo, los 3 nodos se levantan con **Docker Compose** en una sola máquina (§4.8).

**Convenciones de imagen:** build **multi-stage** (una etapa compila con el *toolchain* C++ → otra ejecuta sobre base **mínima/distroless**, copiando solo el binario); **usuario no-root** (`useradd` + `USER`); **tags de versión explícitos** (no `latest` en producción); **escaneo** de la imagen (Trivy / `docker scout`); **`HEALTHCHECK`** cableado a `/readyz` (§7.6) y `probes` *liveness*/*readiness* en Kubernetes; **secretos por variables de entorno** (§7.10), nunca horneados; `.dockerignore` para no copiar artefactos de build.

### 5.4 Vista de datos y formato de *record batch* (R12)

```
Topic "pedidos"
 ├── Partición 0 (grupo Raft) ──► [Seg 0][Seg 1][Seg 2 (activo)]  cada Seg: .log + .index
 ├── Partición 1 (grupo Raft) ──► [Seg 0][Seg 1 (activo)]
 └── Partición 2 (grupo Raft) ──► [Seg 0 (activo)]
```

La unidad de escritura y replicación no es el registro suelto sino el ***record batch*** (estilo Kafka v2), que amortiza cabeceras, CRC y compresión sobre muchos registros:

```
RecordBatch:
 ┌──────────────┬─────────┬──────────┬───────┬────────────┬──────────────┬───────┬───────────┐
 │ baseOffset   │ length  │  CRC32C  │ attrs │ producerId │ baseSequence │ count │  records… │
 └──────────────┴─────────┴──────────┴───────┴────────────┴──────────────┴───────┴───────────┘
   attrs: codec de compresión (none/LZ4/Zstd), bit de transacción/idempotencia
   records[]: cada uno con deltas (offsetDelta, timestampDelta en varint/zigzag), key, value, headers
```

El **offset** es el identificador lógico y monótono dentro de la partición; el primero del batch es `baseOffset` y cada registro añade su `offsetDelta`. El **CRC32C** cubre el batch completo (detección de corrupción en recuperación). El **índice disperso** mapea, cada N bytes, `offset → posición en fichero`, permitiendo *seek* eficiente sin recorrer todo el segmento. `producerId` + `baseSequence` habilitan el **productor idempotente** (§6.6).

### 5.5 Vistas dinámicas (flujos)

**Publicación (produce), camino feliz con Raft y `acks=quorum`:**

```
Productor ──produce(topic,part,batch,acks)──► Líder Raft de la partición
                                          │ 1. append del batch a su log (= log de Raft)
                                          │ 2. replica la entrada a los followers (AppendEntries)
                                          │ 3. al alcanzar mayoría → avanza commitIndex
                                          │ 4. high-watermark = commitIndex (visible a consumidores)
                  ◄────── ack(baseOffset) ─┘    (cuando el quórum confirmó; según acks=0/1/quorum)
```

> Con **Raft por partición**, el *high-watermark* **es** el `commitIndex` del grupo Raft: no hay un mecanismo de ISR separado. `acks=0` responde sin esperar (riesgo *at-most-once*); `acks=1`, tras el append local del líder; `acks=quorum` (por defecto), tras el *commit* de Raft.

**Consumo (fetch):**

```
Consumidor ──fetch(topic,part,offset)──► Líder de partición
                                          │ localiza segmento vía índice disperso
                                          │ lee batch (mmap / io_uring) hasta el high-watermark
                  ◄──── batch + nextOffset ┘
Consumidor procesa y hace commit del offset (manual o automático).
```

**Failover (caída del líder):** al expirar el *election timeout* sin *heartbeat* del líder, un *follower* del grupo Raft de esa partición se convierte en *candidate*, solicita votos (con *pre-vote* opcional, §6.6) y, si obtiene mayoría, se promueve a líder; sólo puede ganar quien tenga el log al menos tan actualizado como la mayoría, así que **no se pierden datos *committed***. Los clientes redescubren el líder vía *metadata*. (Mecánica según ADR-0003.)

### 5.6 Modelo C4 (resumen)

- **Contexto:** productores y consumidores ↔ NexusMQ ↔ operadores (CLI/REST) y Prometheus.
- **Contenedores:** Ingress (dos modos), Nodos de broker (N reactores *per-core* c/u), Almacén en disco, Plano de administración.
- **Componentes** (por reactor): *event loop* io_uring → *handlers* de protocolo (corutinas) → broker (particiones del núcleo) → storage engine → módulo Raft de esas particiones; *cross-core* SPSC hacia otros reactores.

### 5.7 Postura de consistencia: CAP y PACELC (R3)

NexusMQ es, en el camino de escritura, un sistema **CP**: ante una **partición de red**, una partición de datos que **pierda el quórum** de su grupo Raft **deja de aceptar escrituras** (prioriza **consistencia** sobre **disponibilidad**) antes que arriesgar divergencia o pérdida de datos *committed*. En el marco **PACELC**: ante **P**artición, elige **C**; y en operación normal (**E**lse), elige **C**onsistencia sobre **L**atencia (las escrituras esperan al quórum con `acks=quorum`). Lectura: por defecto desde el **líder** hasta el *high-watermark*; lecturas desde *followers* (potencialmente *stale*) quedan como opción explícita y documentada, no por defecto. Esta postura es coherente con Raft y con el objetivo de **correctitud demostrable** del proyecto (ver ADR-0007).

### 5.8 Modelo de datos interno (estructuras clave) (M3)

Resumen de las estructuras nucleares con sus campos; alimenta directamente el desglose de la solución y la implementación por fases. (En disco, los enteros van en *little-endian*; las longitudes y *deltas* en `varint`/`zigzag` donde se indica.)

**Almacenamiento (`storage`)**

- `RecordBatch` (ver §5.4) — `baseOffset:i64`, `length:i32`, `crc32c:u32`, `attrs:u16` `{codec:none|lz4|zstd, txn, idempotent}`, `producerId:i64`, `producerEpoch:i16`, `baseSequence:i32`, `recordCount:i32`, `records[]`.
- `Record` — `lengthDelta:varint`, `attrs:i8`, `timestampDelta:varint`, `offsetDelta:varint`, `key:bytes?`, `value:bytes?`, `headers[]`.
- `IndexEntry` (fichero `.index`, índice disperso) — `relativeOffset:u32`, `filePosition:u32`.
- `Segment` — `baseOffset:i64`, `logPath`, `indexPath`, `sizeBytes`, `maxTimestamp`, `state:{active|closed}`.
- `PartitionLog` — `topic`, `partitionId`, `segments[]`, `activeSegment`, `logStartOffset`, `logEndOffset`, `recoveryPoint` (último *fsync*).

**Consenso (`consensus`)**

- `RaftPersistentState` (en disco) — `currentTerm:i64`, `votedFor:nodeId?`, `log[] (RaftLogEntry)`.
- `RaftLogEntry` — `term:i64`, `index:i64`, `type:{data|config}`, `payload` (un `RecordBatch` o un cambio de configuración).
- `RaftVolatileState` — `commitIndex:i64`, `lastApplied:i64`; en líder: `nextIndex[peer]`, `matchIndex[peer]`.
- `Snapshot` — `lastIncludedIndex`, `lastIncludedTerm`, `state`.

**Broker / plano de control (`broker`)**

- `TopicMetadata` — `name`, `partitionCount`, `replicationFactor`, `config:{retentionMs, retentionBytes, segmentBytes, compaction, compression}`, `createdAt`.
- `PartitionState` — `topic`, `partitionId`, `leaderEpoch:i32`, `leaderNodeId`, `replicaNodeIds[]`, `highWatermark`, `logStartOffset`, `logEndOffset`, `ownerReactorId`.
- `ConsumerGroup` — `groupId`, `generationId:i32`, `state:{empty|preparingRebalance|stable|dead}`, `leaderMemberId`, `members[]{memberId, clientId, assignment[]}`.
- `OffsetCommit` — `groupId`, `topic`, `partition`, `committedOffset:i64`, `metadata?`, `commitTimestamp`.
- `ProducerSession` (idempotencia) — `producerId:i64`, `epoch:i16`, `lastSequence[partition]:i32`.

**Red (`protocol` / `server`)**

- `FrameHeader` — `length:u32`, `apiKey:u16`, `apiVersion:u16`, `correlationId:u32`, `flags:u16`.
- `ConnectionState` — `connId`, `negotiatedVersions`, `authPrincipal?`, `credits:i32` (*backpressure*, §6.3), `inflight[correlationId]`.

### 5.9 Máquinas de estado y ciclos de vida (M9)

- **Rol de Raft (por partición):** `follower → candidate → leader`. Transiciones: *election timeout* sin *heartbeat* → `candidate` (con *pre-vote*); mayoría de votos → `leader`; descubrir `term` mayor → `follower`. (Detalle en §5.5 y §7.8.)
- **Ciclo de vida de un segmento:** `active` (recibe *appends*) → `closed` (alcanza `segment.bytes`/`segment.ms`; se sella y se finaliza su `.index`) → `eligible` (supera la retención) → `deleted` (borrado del fichero completo). La retención **nunca** borra el segmento activo.
- **Liderazgo de partición:** `follower` (replica) → `candidate` → `leader` (sirve *produce*/*fetch*) → *step-down* (por *leadership transfer* ordenado o por `term` mayor). El `leaderEpoch` se incrementa en cada cambio y permite descartar peticiones de líderes obsoletos (`NOT_LEADER_FOR_PARTITION`).
- **Conexión de cliente:** `accept` → *TLS handshake* → `ApiVersions`/negociación → `auth` → `ready` (multiplexa *requests* con *correlation IDs* y créditos) → `draining` → `closed`.
- **Durabilidad de un registro:** `received` → `appended` (log local del líder) → `replicated` (a *followers*) → `committed` (`commitIndex ≥ index`) → `visible` (`offset ≤ high-watermark`) → `retained` → `deleted` (retención).
- **Secuencia idempotente (por `producerId`, partición):** `expected` → **aceptada**; `< expected` → **duplicado** (se reconoce sin re-*append*); `> expected` → **hueco** → `OUT_OF_ORDER_SEQUENCE`.

---

## 6. Patrones de diseño y principios

> Esta sección **especializa** los principios y patrones de diseño al dominio de sistemas; no los duplica.

### 6.1 Principios rectores

- **Separación plano de datos / plano de control.** El camino caliente (produce/fetch) se mantiene libre de lógica de administración; la gestión (crear *topics*, rebalanceos) vive en un plano aparte.
- **Shared-nothing.** El estado mutable es **propiedad de un único reactor**; nada se comparte entre núcleos salvo por paso de mensajes. Es el principio que hace el sistema razonable y testeable.
- **SOLID y arquitectura hexagonal aplicada a sistemas.** El núcleo (storage, broker, consenso) no depende de detalles de I/O ni de protocolo: estos entran por *puertos* (interfaces) y *adaptadores* (p.ej. el backend io_uring frente a IOCP es un adaptador del puerto de I/O).
- **Mecanismo vs política.** El storage ofrece *mecanismo* (append, read, fsync); la *política* (retención, durabilidad, `acks`) es configurable y externa.

### 6.2 Patrones arquitectónicos

- **Reactor / Proactor.** El servidor de red se modela como *proactor* (I/O por *completions*) — base de la abstracción de §3.3; **un reactor por núcleo**.
- **Leader–Follower (vía Raft).** Cada partición tiene un líder (el líder Raft del grupo) que ordena las escrituras y *followers* que replican.
- **Pipeline.** Recepción → decodificación → *append* → replicación (Raft) → *ack*, como etapas conectadas por colas.
- **Event-driven.** El flujo se gobierna por eventos de I/O y de *timers* (heartbeats Raft, *health checks*).

### 6.3 Patrones de concurrencia (R5, R10, R11)

- **Thread-per-core con afinidad** como modelo **primario** (estado por núcleo, sin *locks* en el camino caliente). El *thread-pool con work-stealing* queda **descartado como modelo primario** (ver §3.5 y ADR-0005).
- **Paso de mensajes *cross-core*** sobre colas **SPSC** (estilo `submit_to`): una petición que llega al núcleo A pero cuya partición vive en B se *reenvía* a B; no hay acceso compartido.
- **Colas lock-free SPSC/MPMC** en el *handoff* del *pipeline* dentro de un núcleo. **Lock-free solo con motivo y medición:** CAS con el `memory_order` mínimo (`acquire`/`release` en productor-consumidor); en las colas **MPMC**, resolver el problema **ABA** (contadores de versión / *hazard pointers* / *epoch-based reclamation*) y validar con **ThreadSanitizer + estrés aleatorio** en CI.
- **Evitar *false sharing*:** separar a líneas de caché distintas los campos escritos por hilos distintos (p.ej. *head*/*tail* de cada cola) con **`alignas(std::hardware_destructive_interference_size)`**.
- **Modelo por-partición** (cada partición como unidad de serialización, estilo *actor*, anclada a un núcleo) para evitar contención entre particiones.
- **Backpressure por créditos (R5):** colas **acotadas** *end-to-end*; el consumidor/escritor concede **créditos** al productor; cuando se agotan, el productor se **frena** (no se descartan mensajes ni crecen los *buffers* sin límite). Evita el colapso bajo sobrecarga y mantiene latencias acotadas.

### 6.4 Patrones de resiliencia (capa de *ingress*)

- **Circuit Breaker** (Nygard) con estados *closed/open/half-open* y ventana deslizante de errores.
- **Bulkhead**: aislar *pools* de conexiones por destino para que un fallo no agote todo.
- **Retry con *backoff* exponencial + *jitter*** para evitar tormentas de reintentos.
- **Rate limiting** con *token bucket* por cliente/*topic*.
- **Backpressure** propagado hasta el cliente (créditos) en lugar de descartar silenciosamente.
- **Health checks** activos (ping periódico) y pasivos (detección por errores en tráfico real).

### 6.5 Patrones de almacenamiento

- **Write-Ahead Log (WAL)** para durabilidad ante *crash*. Bajo Raft, **el log de Raft *es* el WAL** de la partición.
- **Log-structured storage**: escritura secuencial append-only (amiga del disco).
- **Segmentación** + **índice disperso** para *seek* eficiente y retención por borrado de segmentos completos.
- **Compactación por clave** (retener el último valor por clave) como política alternativa a la retención por tiempo/tamaño.

### 6.6 Patrones distribuidos

- **Consenso (Raft) por partición** para elección de líder y replicación de log; *high-watermark* = `commitIndex`. Extensiones contempladas: ***pre-vote*** (evita *disruption* por nodos reincorporándose), ***leadership transfer*** (rebalanceo ordenado de liderazgo) y réplicas ***learner*** (se ponen al día sin votar).
- **Productor idempotente** (*producer-id* + *sequence*) para *effectively-once* por partición (descarta duplicados por reintento).
- **Dead Letter Queue (DLQ)** para mensajes no procesables tras N intentos.

### 6.7 Catálogo de ADRs

Ver §9. Índice rápido: ADR-0001 (plataforma), ADR-0002 (I/O), ADR-0003 (replicación: **Raft por partición**, *aceptado*), ADR-0004 (protocolo: **binario propio + REST**, *aceptado*), ADR-0005 (concurrencia: ***shared-nothing thread-per-core***, *aceptado*), ADR-0006 (*ingress* dos modos, *aceptado*), ADR-0007 (consistencia CP/PACELC, *aceptado*), ADR-0008 (coste cero, *aceptado*), ADR-0009 (manejo de errores por capa, *aceptado*), ADR-0010 (IDE: migración a **VS Code** sobre WSL, *aceptado*), ADR-0011 (estándar **C++23** + libc++ en Clang, *aceptado*), ADR-0012 (backend io_uring directo sobre el uapi, sin liburing, *aceptado*), ADR-0013 (capa **`nexus-wire`** para el framing sobre conexión; `nexus-protocol` queda puro, *aceptado*), ADR-0014 (modelo del **log de Raft**: índice = ordinal de entrada, término en sidecar; `RecordBatch` intacto, *aceptado*), ADR-0015 (`RaftNode` como **máquina de estados síncrona sin E/S** —entradas→cola de salidas—, para simulación determinista, *aceptado*), ADR-0016 (`ReplicatedPartition` como **tipo paralelo** a `Partition`, *aceptado*), ADR-0017 (target **`nexus-telemetry`** para observabilidad bajo el broker, *aceptado*), ADR-0018 (REST admin por **puerto/adaptador**: `AdminService` en ingress, `AdminApi` en server, *aceptado*).

---

## 7. Diseño preliminar de subsistemas

> **Preliminar.** Cada subsistema se detallará en su fase. Aquí se fija solo la intención de diseño.

### 7.1 Storage Engine (Fase 1)

`Record`/`RecordBatch` (serialización + CRC32C) → `Segment` (`.log` + `.index`, append/read) → `Log` (secuencia de segmentos, *rolling*, recuperación) → políticas de **durabilidad** (*fsync*) y **retención**. Lecturas de segmentos antiguos vía **mmap** (Fase 1) y, tras el reactor, vía **io_uring**; índice **disperso** para *seek*. Profundidad opcional (Fase 4): ***direct I/O*** con caché y *readahead* propios (§3.3).

### 7.2 Protocolo de red (Fase 1b)

**Protocolo binario propio** (ADR-0004): *framing* de longitud fija en cabecera + *payload* variable; **multiplexing** de *requests* sobre una conexión TCP con *correlation IDs*; **versionado/negociación**; control de flujo por **créditos** (§6.3); compresión opcional por *batch*. Operaciones iniciales: `metadata`, `produce`, `fetch`, `commit`. Especificación en `docs/protocol.md`. **Gateway REST** encima para interoperabilidad HTTP. *Stretch* (Fase 4): **subconjunto Kafka-compatible** (`ApiVersions`/`Metadata`/`Produce`/`Fetch`) para que `kcat` y consolas Kafka hablen con el broker.

**Cabecera de *frame*:** `length:u32 | apiKey:u16 | apiVersion:u16 | correlationId:u32 | flags:u16 | payload`. El `correlationId` permite **multiplexar** varias peticiones en vuelo sobre una conexión y casar cada respuesta con su petición.

#### 7.2.1 Catálogo de operaciones del protocolo (M4)

| Operación        | Request (campos clave)                                  | Response (campos clave)                                        | Fase | Notas |
| ---------------- | ------------------------------------------------------- | ------------------------------------------------------------- | ---- | ----- |
| `ApiVersions`    | `clientVersion`                                         | `{apiKey, minVer, maxVer}[]`                                   | 1b   | Negociación de versiones |
| `Metadata`       | `topics[]?`                                             | `brokers[{nodeId,host,port}]`, `topics[{name, partitions[{id, leaderNodeId, replicas[], leaderEpoch}]}]` | 1b | Descubrimiento de líder |
| `Produce`        | `topic, partition, acks, batch`                         | `baseOffset, errorCode, throttleMs`                           | 1b   | *Hot path* |
| `Fetch`          | `topic, partition, fetchOffset, maxBytes, minBytes, maxWaitMs` | `batches, highWatermark, logStartOffset, errorCode`    | 1b   | *Long-poll* (espera `minBytes`) |
| `OffsetCommit`   | `groupId, topic, partition, offset, metadata`           | `errorCode`                                                   | 1b–2 | Commit de consumidor |
| `OffsetFetch`    | `groupId, topic, partition`                             | `offset, metadata, errorCode`                                 | 1b–2 | |
| `JoinGroup` / `SyncGroup` / `Heartbeat` / `LeaveGroup` | protocolo de *consumer group* | asignación de particiones, `generationId`               | 2    | Rebalanceo |
| `CreateTopic` / `DeleteTopic` | `name, partitions, replicationFactor, config`  | `errorCode`                                                  | 2–3  | También vía REST (§7.6) |

> El **control de flujo por créditos** (§6.3) viaja en `flags`/campos de la cabecera: el receptor anuncia créditos disponibles y el emisor no excede la ventana.

#### 7.2.2 Taxonomía de errores del protocolo (M2)

Toda respuesta lleva un `errorCode:i16` (+ `errorMessage?` opcional). El cliente decide reintentar según la columna *¿reintentable?*; los códigos *reintentables* exigen **refrescar metadata** y/o **backoff**.

| Código                          | Significado                                              | ¿Reintentable? | Acción del cliente |
| ------------------------------- | ------------------------------------------------------- | -------------- | ------------------ |
| `NONE` (0)                      | Éxito                                                    | —              | — |
| `NOT_LEADER_FOR_PARTITION`      | Este nodo ya no es líder de la partición                | Sí             | Refrescar `Metadata`, reintentar contra el nuevo líder |
| `LEADER_NOT_AVAILABLE`          | Elección en curso; sin líder estable                    | Sí (*backoff*) | Esperar y reintentar |
| `UNKNOWN_TOPIC_OR_PARTITION`    | El *topic*/partición no existe                           | No             | Error al usuario |
| `OFFSET_OUT_OF_RANGE`           | El offset pedido está fuera de `[logStart, HW]`         | No             | Reposicionar (*earliest*/*latest*) |
| `NOT_ENOUGH_REPLICAS`           | Quórum insuficiente para `acks=quorum`                  | Sí (*backoff*) | Reintentar; alerta de operación |
| `REQUEST_TIMED_OUT`             | Expiró el plazo de la petición                          | Sí             | Reintentar |
| `CORRUPT_MESSAGE`               | El CRC32C no cuadra                                     | No             | No reenviar igual; investigar |
| `MESSAGE_TOO_LARGE`             | Excede `max.message.bytes`/`max.batch.bytes`            | No             | Trocear/comprimir |
| `OUT_OF_ORDER_SEQUENCE`         | Hueco en la secuencia idempotente                       | Especial       | Reiniciar sesión de productor |
| `DUPLICATE_SEQUENCE`            | Secuencia ya vista (duplicado por reintento)            | No (idempotente)| Tratar como éxito |
| `THROTTLED` / `QUOTA_VIOLATED`  | Cuota superada                                          | Sí (tras `throttleMs`) | Respetar el *throttle* |
| `REBALANCE_IN_PROGRESS`         | El grupo de consumidores se está rebalanceando          | Sí             | Re-`JoinGroup` |
| `UNSUPPORTED_VERSION`           | Versión de API no soportada                             | No             | Renegociar con `ApiVersions` |
| `UNAUTHORIZED` / `SASL_AUTH_FAILED` | Fallo de autenticación/autorización                 | No             | Revisar credenciales |
| `INVALID_REQUEST`               | Petición malformada                                     | No             | Bug del cliente |

### 7.3 Core Broker y semánticas de entrega (Fase 1b–2) — R1

Gestión de *topics*/particiones (ancladas a núcleo), grupos de consumidores y rebalanceo, gestión de offsets (commit manual/automático, *replay*), TTL y DLQ. **Semánticas de entrega precisas:**

- ***At-most-once***: el consumidor hace *commit* del offset **antes** de procesar (`acks=0` del lado productor). Se puede perder, nunca duplicar.
- ***At-least-once*** (por defecto): *commit* **después** de procesar; ante fallo, se reprocesa. Nunca se pierde, puede duplicar.
- ***Effectively-once* por partición**: *at-least-once* + **productor idempotente** (*producer-id* + *sequence*), que descarta duplicados por reintento dentro de una partición. **No** es *exactly-once* transaccional entre particiones (fuera de alcance, §4.4).

### 7.4 Reactor y coordinación entre núcleos (Fase 1b) — R10

Implementación del **reactor *thread-per-core* propio**: *event loop* sobre **io_uring** (un anillo por núcleo), *scheduler* de **corutinas** C++20, *allocator* por núcleo y **colas SPSC *cross-core*** con despertar del destino (estilo `submit_to` de Seastar, pero a mano). El *router* de conexiones dirige cada petición al reactor **dueño** de la partición destino; si llega al núcleo equivocado, se reenvía por la cola *cross-core*. Es la pieza que materializa el principio *shared-nothing* (ADR-0005). El reactor puede usar ***busy-poll*** del anillo io_uring por **latencia crítica** (justificado y **medido**, con *backoff* si la carga baja). **Apagado limpio:** ante `SIGTERM`/`SIGINT` el nodo deja de aceptar conexiones, **drena** el trabajo en curso (vacía colas, `fsync` final, transfiere liderazgos) y cierra; en el *signal handler*, solo trabajo *async-signal-safe* (un `eventfd`/self-pipe que despierta al reactor).

### 7.5 Ingress Layer — dos modos (Fase 3) — R5, R-ingress

Dos modos explícitos con *trade-offs* documentados (ADR-0006):

- **Modo nativo (primario, alto rendimiento):** *smart-client* que consulta *metadata* y va **directo al líder** de la partición. Cero salto de proxy, *zero-copy* posible. Es el camino recomendado.
- **Modo proxy (secundario, conveniencia, *opt-in*):** el *ingress* acepta clientes "tontos" y enruta por él (*consistent hashing* por *partition key*), asumiendo el salto extra a sabiendas. El **gateway REST** es una variante de este modo para HTTP.

En ambos modos, el *ingress* aporta: terminación **TLS 1.3**, *circuit breaker*, *rate limiting* (*token bucket*), *health checks*, *connection pooling*, *retry* con *backoff*+*jitter* y propagación de **backpressure**. *Load balancing*: round-robin, *least-connections*, *consistent hashing*.

### 7.6 Admin & Observabilidad (Fase 3)

Plano de **control** (separado del plano de datos), expuesto por HTTP: API REST de administración, CLI `nexus-cli`, `/metrics` (Prometheus), logs JSON con niveles y *tracing* con *correlation IDs*.

**Catálogo REST de administración (M4)** — rutas versionadas en `/api/v1`:

| Método | Ruta                                  | Descripción                                         |
| ------ | ------------------------------------- | --------------------------------------------------- |
| GET    | `/api/v1/topics`                      | Listar *topics* (**paginado** `page`/`size`)        |
| POST   | `/api/v1/topics`                      | Crear *topic* `{name, partitions, replicationFactor, config}` (`201` + `Location`) |
| GET    | `/api/v1/topics/{name}`               | Describir (particiones, líderes, *high-watermark*, *lag*) |
| DELETE | `/api/v1/topics/{name}`               | Borrar *topic*                                      |
| GET    | `/api/v1/groups`                      | Listar grupos de consumidores (**paginado**)        |
| GET    | `/api/v1/groups/{id}`                 | Describir grupo (miembros, asignación, *lag*)       |
| POST   | `/api/v1/partitions/reassign`         | Reasignar réplicas / transferir liderazgo (acción; **clave de idempotencia**, pues `POST` no lo es) |
| GET    | `/metrics`                            | Métricas en formato Prometheus                      |
| GET    | `/healthz`                            | *Liveness* (ver abajo)                              |
| GET    | `/readyz`                             | *Readiness* (ver abajo)                             |

**Convenciones REST:** recursos en plural y **versión en la ruta** (`/api/v1`); **semántica HTTP** respetada (`GET`/`HEAD` *safe*; `GET`/`PUT`/`DELETE` idempotentes); **formato de error `ProblemDetail` (RFC 7807)** con política central — **distinto** de los `errorCode` numéricos del protocolo binario (§7.2.2): el REST **traduce al modelo RFC 7807 en el borde**; **paginación** obligatoria en colecciones; contrato en **OpenAPI/Swagger** (ver Anexo A); **Bearer JWT** + HTTPS + CORS con orígenes explícitos. Los **DTOs** de entrada/salida no exponen las estructuras internas (§5.8); enums **serializados como texto** para estabilidad del contrato.

**Semántica de *health* y *readiness* (M10)** — distinción inspirada en un `/health` enriquecido:

- **`/healthz` (*liveness*):** ¿está vivo el proceso? `200` si los *event loops* de los reactores laten. Un fallo aquí justifica **reiniciar** el proceso.
- **`/readyz` (*readiness*):** ¿puede recibir tráfico? Devuelve JSON con *checks* en estado `ok|degraded|fail`:
  - `disk`: `data.dir` escribible y `diskFreePct` por encima del umbral.
  - `raft`: este nodo es líder o *follower* **al día** de sus particiones (no en recuperación).
  - `replicationLag`: *lag* de réplica acotado.
  - `startup`: ha terminado el arranque/recuperación.
  
  Un `fail` retira el nodo del balanceo sin matarlo. El *ingress* y los *health checks* (§7.5) consumen `/readyz`.

### 7.7 Cliente nativo (Fase 1b)

Librería C++ separada (*smart-client*) que habla el protocolo binario; gestión de conexión, *metadata* (descubrimiento de líder), productor (con créditos y, *stretch*, idempotencia) y consumidor. *Stretch*: *binding* Python con pybind11.

### 7.8 Replicación y consenso — Raft por partición (Fase 2) — R2

Cada partición es un **grupo Raft** independiente: su log replicado *es* el log de la partición. Componentes: máquina de estados de Raft (*follower*/*candidate*/*leader*), `RequestVote` y `AppendEntries` (con *heartbeats*), *commit index* → *high-watermark*, persistencia de estado (`currentTerm`, `votedFor`, log) y **snapshots/compactación de log** para acotar el tamaño. El **modelo de ACK** (§4.3) se deriva del *commit* de Raft. Extensiones (§6.6): *pre-vote*, *leadership transfer*, *learners*. Anclaje: el líder de cada partición vive en un reactor concreto; el *heartbeat* y la replicación viajan por la red entre nodos y, dentro de un nodo, por colas *cross-core* hacia el reactor dueño.

### 7.9 Seguridad (Fase 3) — R7

- **En tránsito:** **TLS 1.3** terminado en el *ingress*; opción de **mTLS** intra-cluster (entre nodos del broker) para autenticar réplicas.
- **Autenticación de cliente:** *tokens* / mecanismo tipo SASL (esquema simple, *pluggable*).
- **Autorización:** control básico por *topic* (quién produce/consume), como *stretch*.
- **Fuera de alcance:** cifrado **en reposo**, multi-tenancy y ACLs avanzadas (§4.4).

**Endurecimiento de recursos (*hardening*) (M10):**

- **Límites de tamaño:** `max.message.bytes`/`max.batch.bytes`; se rechaza con `MESSAGE_TOO_LARGE` **antes** de materializar el *payload*.
- **Protección anti *decompression bomb*:** límite de **ratio** y de **tamaño descomprimido** de *batches* comprimidos (LZ4/Zstd) antes de expandirlos; un *batch* hostil no puede agotar la memoria del reactor.
- **Límite de conexiones** por IP y total (`max.connections`); *timeouts* de *handshake* e *idle* contra *slowloris*.
- **Cuotas por cliente/principal** (bytes/s de *produce*/*fetch*) → `THROTTLED` en vez de tumbar el broker.
- **Backpressure por créditos** (§6.3) como defensa primaria ante sobrecarga.
- **Decodificador defensivo:** nunca confiar en longitudes del *wire* sin acotarlas; *fuzzing* del *parser* (§8.1).
- **TLS 1.3** únicamente, *ciphers* modernos; **mTLS** intra-cluster.

### 7.10 Catálogo de configuración (M1)

Configuración por fichero con *override* por **variables de entorno** (12-factor, coherente con ADR-0008/*cloud-ready*). Claves principales y sus valores por defecto:

| Clave                          | Por defecto              | Descripción |
| ------------------------------ | ------------------------ | ----------- |
| `broker.id`                    | *(requerido)*            | Identificador único de nodo |
| `listeners`                    | `0.0.0.0:9092`           | *Endpoints* del protocolo binario |
| `advertised.listeners`         | *(= listeners)*          | `host:port` anunciado en `Metadata` |
| `data.dir`                     | `/var/lib/nexusmq`       | Raíz de los logs de partición |
| `num.reactors`                 | `0` (= nº de *cores*)    | Reactores *thread-per-core* |
| `segment.bytes`                | `1 GiB`                  | *Rolling* de segmento por tamaño |
| `segment.ms`                   | `7d`                     | *Rolling* de segmento por tiempo |
| `retention.ms`                 | `7d`                     | Retención temporal |
| `retention.bytes`              | `-1` (∞)                 | Retención por tamaño y partición |
| `max.message.bytes`            | `1 MiB`                  | Tamaño máximo de mensaje |
| `max.batch.bytes`              | `4 MiB`                  | Tamaño máximo de *batch* |
| `compression.type`             | `producer`              | `none`/`lz4`/`zstd`/`producer` |
| `default.acks`                 | `quorum`                | `0`/`1`/`quorum` |
| `fsync.policy`                 | `interval`              | `none`/`interval`/`commit` |
| `fsync.interval.ms`            | `1000`                  | Si `fsync.policy=interval` |
| `replication.factor.default`   | `3`                     | Réplicas por partición |
| `raft.election.timeout.ms`     | `1000–1500` (aleatorio) | *Election timeout* (reloj monotónico) |
| `raft.heartbeat.interval.ms`   | `150`                   | *Heartbeats* del líder |
| `raft.snapshot.threshold`      | `10000` entradas         | Umbral de *snapshot*/compactación |
| `max.connections`              | `10000`                 | Conexiones concurrentes |
| `request.timeout.ms`           | `30000`                 | Plazo de petición |
| `tls.enabled`                  | `false` (dev)            | TLS 1.3 (en prod: `true`) |
| `metrics.port`                 | `9644`                  | Endpoint Prometheus + REST admin |

### 7.11 Pseudocódigo crítico (M5)

**1) Camino caliente de *produce* (en el líder de la partición):**

```text
on Produce(topic, part, acks, batch):
    p = partition(topic, part)
    if not reactor_owns(p): return forward_cross_core(p.reactor, request)   # shared-nothing
    if size(batch) > max_batch_bytes: return MESSAGE_TOO_LARGE
    if crc32c(batch) != batch.crc: return CORRUPT_MESSAGE
    if p.idempotent:
        case check_sequence(batch.producerId, batch.baseSequence):
            duplicate -> return ack(prev_offset)        # ya visto, no re-append
            gap       -> return OUT_OF_ORDER_SEQUENCE
    entry = raft_log_append(p.raft, batch)              # = WAL append (io_uring, async)
    if acks == 0: return ack(entry.baseOffset)          # at-most-once, sin esperar
    if acks == 1:
        co_await fsync_policy(p); return ack(entry.baseOffset)
    co_await raft_replicate(p.raft, entry)              # acks == quorum: AppendEntries
    advance_commit_index(p.raft)
    p.high_watermark = p.raft.commitIndex
    return ack(entry.baseOffset)
```

**2) Recuperación al arrancar (truncado de cola *torn*):**

```text
for each segment in partition (orden por baseOffset):
    pos = 0; last_valid = 0
    while pos < segment.size:
        hdr = read_batch_header(pos)
        if hdr incompleto or pos + hdr.length > segment.size: break    # torn tail
        if crc32c(bytes[pos : pos+hdr.length]) != hdr.crc: break       # corrupción
        last_valid = pos + hdr.length
        pos = last_valid
    truncate(segment, last_valid)        # descarta cola incompleta/corrupta
log_end_offset = mayor offset válido;  recovery_point = log_end_offset
```

**3) *Seek* por índice disperso (en *fetch*):**

```text
on Fetch(part, fetchOffset):
    seg = segment_containing(fetchOffset)         # búsqueda binaria por baseOffset
    e   = sparse_index.floor(fetchOffset)         # mayor IndexEntry <= fetchOffset
    pos = e.filePosition
    while last_offset(batch_at(pos)) < fetchOffset:   # escaneo corto desde el ancla
        pos += batch_at(pos).length
    return read_batches(seg, from=pos, maxBytes, upTo=part.high_watermark)
```

**4) *Backpressure* por créditos (emisor):**

```text
credits = initial_window
on send(msg):
    while credits == 0: co_await credits_available    # se FRENA (no descarta, no crece)
    credits -= cost(msg); write(msg)
on credit_update(n):                                 # el receptor concede al drenar su cola
    credits += n; signal(credits_available)
```

**5) `AppendEntries` (en el *follower* de Raft):**

```text
on AppendEntries(term, prevLogIndex, prevLogTerm, entries[], leaderCommit):
    if term < currentTerm: return {currentTerm, success=false}
    reset_election_timer()                                   # reloj monotónico
    if log[prevLogIndex].term != prevLogTerm: return {currentTerm, success=false}  # hueco
    append/overwrite log desde prevLogIndex+1 con entries
    if leaderCommit > commitIndex: commitIndex = min(leaderCommit, last_index())
    return {currentTerm, success=true}
```

---

## 8. Estrategia de calidad

### 8.1 Testing

- **Unitarias** (GoogleTest) sobre la lógica de dominio (record, índice, offsets, *circuit breaker*, máquina de estados de Raft).
- **Property-based** para serialización (round-trip de `RecordBatch`) y para invariantes del log.
- **Integración** *end-to-end*: productor → broker → consumidor.
- **Crash / chaos**: matar el proceso a mitad de escritura y verificar recuperación; **particionar la red** (`tc netem`) y verificar *failover* y la postura **CP** (Fase 2); inyección de retardos.
- **Tests deterministas de Raft**: simulación con reloj y red virtuales para reproducir elecciones y *splits* de forma repetible.
- **Fuzzing** del decodificador del protocolo.

**Matriz de testing por subsistema (M10)** (✓ = aplica):

| Subsistema        | Unit | Property | Integración | Crash/Chaos | Determinista | Fuzz |
| ----------------- | :--: | :------: | :---------: | :---------: | :----------: | :--: |
| `storage`         | ✓    | ✓        | ✓           | ✓ (*torn tail*) | –        | –    |
| `protocol`        | ✓    | ✓ (*round-trip*) | ✓   | –           | –            | ✓    |
| `reactor`         | ✓    | –        | ✓           | –           | ✓ (sim)      | –    |
| `consensus` (Raft)| ✓    | ✓ (invariantes) | ✓    | ✓ (partición red) | ✓ (reloj/red virtual) | – |
| `broker`          | ✓    | –        | ✓ (*e2e*)   | ✓           | –            | –    |
| `ingress`         | ✓    | –        | ✓           | ✓           | –            | –    |
| `client`          | ✓    | –        | ✓ (*e2e*)   | ✓ (reconexión) | –         | –    |

> **Política de fase:** una fase no se cierra sin su columna de pruebas en verde; los *sanitizers* (ASan/UBSan/TSan) corren en CI sobre la lógica de dominio.
>
> **TDD y FIRST:** se escribe la prueba **antes** que el código (ciclo rojo→verde→refactor) en la lógica de dominio, y las pruebas cumplen **FIRST** — el no-determinismo distribuido se **controla inyectando** reloj/red virtuales (no se tolera una prueba *flaky*). Nombres de test `Metodo_Escenario_ResultadoEsperado`. **Dobles:** *stubs* para consultas, *mocks* solo para interacciones con dependencias **fuera de proceso**.

### 8.2 Benchmarking (R4) — metodología rigurosa

- **Generador de carga *open-loop*** (`tools/bench`): emite a tasa fija **independiente de las respuestas**, para **evitar *coordinated omission*** (el sesgo por el que un cliente *closed-loop* deja de medir la latencia real cuando el sistema se atasca).
- **Latencia con histograma de alta resolución** (estilo **HdrHistogram**): se reportan **p50/p99/p999/max**, no solo medias.
- **Disciplina experimental:** *hardware* y kernel documentados, **núcleos aislados** (`isolcpus`/`taskset`), *governor* de CPU fijo, varias ejecuciones, **descartar *warm-up***, y registro de parámetros (tamaño de batch, `acks`, `fsync`, compresión).
- **Métricas:** throughput (msg/s, MB/s) y latencia por percentiles; impacto de *fsync*, compresión, *batching* y de `acks=1` vs `acks=quorum`.
- **Cultura de "antes/después":** cada optimización (p.ej. introducir io_uring, *registered buffers*, *direct I/O*) se justifica con su ***delta* medido**.
- **Aviso:** las cifras absolutas dependen del *hardware*; se miden **en local** con entorno controlado, no en *free tiers* de cloud (§4.8).

### 8.3 Observabilidad

Métricas Prometheus (throughput, *lag* de consumidor, estado de Raft —*term*, líder, `commitIndex`—, latencias), logs estructurados en JSON con niveles, y *tracing* básico con *correlation IDs* propagados entre componentes.

### 8.4 CI/CD

CI (GitHub Actions) que compila en Linux (GCC/Clang) con **avisos como errores** (`-Werror`), pasa `clang-tidy` y `clang-format --dry-run`, ejecuta las pruebas y los *sanitizers* (ASan/UBSan/TSan), y **construye la imagen Docker una sola vez** para **promover el mismo binario** por los entornos (*build-once, promote the same binary*). **Flujo git:** ramas `feature/`/`fix/` cortas, **Conventional Commits** (`feat:`/`fix:`/`docs:`/`refactor:`/`test:`/`chore:`), PR con revisión y CI en verde antes de fusionar; cada **ADR** se commitea con `docs:`. *Releases* etiquetados por fase. **Coste cero** (repos públicos o *free tier*, §4.8).

### 8.5 Runbook operativo (esbozo) (M10)

Procedimientos de operación que el diseño debe soportar (se concretan en Fase 3):

- **Añadir un nodo:** arranca con `broker.id` nuevo + *seeds*; se une como **learner** (no vota), se pone al día por Raft, pasa a *voter*; después se **reasignan** particiones hacia él.
- **Rebalanceo / transferencia de liderazgo:** `nexus-cli partitions reassign` y *leadership transfer* (cambio de líder **sin** *downtime*).
- **Disco lleno:** `/readyz` lo detecta (`diskFreePct`); aplicar retención más agresiva o añadir disco. El broker **rechaza** *produce* antes que corromper.
- **Caída de un nodo (cluster de 3):** el quórum (2/3) mantiene el servicio; al volver, el nodo se re-sincroniza por Raft.
- **Pérdida de quórum (2 de 3 caídos):** por la postura **CP**, las particiones afectadas quedan **no disponibles para escritura**; se recuperan al restaurar nodos. Forzar progreso sin quórum solo mediante un procedimiento manual y documentado de *unsafe-recovery* (asume riesgo de pérdida).
- **Backups:** *snapshot* de `data.dir` por nodo; *restore* = arrancar desde ese `data.dir`.
- **Rotación de certificados TLS:** recarga en caliente si se implementa, o *rolling restart*.

---

## 9. Decisiones de arquitectura (ADRs)

> Formato según `BibliotecaDocumentacion/patrones/plantilla-adr.md`. **En este documento** los ADR van en forma condensada (negrita inline); al extraerlos a `docs/adr/adr-NNNN-titulo-corto.md` adoptan las secciones **`## Contexto` · `## Decisión` · `## Consecuencias` · `## Alternativas consideradas`** (H2) y el estado canónico (`propuesto` | `aceptado` | `reemplazado por adr-NNNN` | `deprecado`). Una decisión `aceptado` **no se edita**: si cambia, se **reemplaza** por un ADR nuevo.

### ADR-0001: Plataforma de desarrollo y *target* (Linux primero, Windows después)

- **Estado:** aceptado
- **Fecha:** 2026-06-07

**Contexto.** El autor desarrolla en Windows con Visual Studio, pero el dominio (broker de mensajería) se despliega de forma natural en Linux/contenedores, y el ecosistema de *tooling* de alto rendimiento (perf, flamegraphs, io_uring, hugepages, NUMA) es Linux. Se desea no renunciar a Visual Studio.

**Decisión.** *Target* primario **Linux** (x86-64, Docker), desarrollado **desde Visual Studio vía WSL2** (workload "Linux development with C++"), con **CMake + vcpkg** como build/deps portables. El *target* **Windows nativo** es un **objetivo posterior comprometido**, no descartado: se aborda tras consolidar Linux.

**Consecuencias.** (+) Encaje con el dominio y con el *tooling* esperado por perfiles de sistemas/HFT; se conserva el IDE preferido. (+) La portabilidad se diseña desde el inicio sin pagar su coste completo de entrada. (+) La estructura de **solución única** (árbol CMake) **no bloquea** el *target* Windows posterior: el mismo árbol cambia de *preset* a MSVC y solo añade el adaptador **IOCP** en `src/io/`, sin reestructurar (ver §5.2). (−) Se asume la fricción de WSL2 y de un *toolchain* multiplataforma.

**Alternativas consideradas.**
- **Windows nativo (IOCP/RIO) como primario:** descartado como primario por encaje de dominio y *tooling*, aunque sería un diferenciador; se mantiene como fase posterior.
- **Cross-platform total desde el día 1:** descartado por el coste de mantener dos backends de I/O y de fichero antes de tener producto.

### ADR-0002: Modelo de I/O asíncrona (proactor; io_uring primero, IOCP después)

- **Estado:** aceptado
- **Fecha:** 2026-06-07

**Contexto.** El plano de datos es I/O intensiva. Los dos *targets* (Linux, Windows) ofrecen modelos asíncronos: io_uring e IOCP, **ambos basados en *completions***; epoll, en cambio, es *readiness-based*.

**Decisión.** Modelar la capa de I/O como **proactor** (interfaz: enviar operación asíncrona → recibir *completion*), con **backend io_uring ahora** y **IOCP después**. Las **coroutines de C++20** se asientan sobre el proactor. El storage engine arranca con I/O **bloqueante** (Fase 1) y adopta io_uring como **optimización medida**; bajo el reactor *thread-per-core*, **todo** I/O (incluido `fsync`) pasa por io_uring porque nada bloqueante puede vivir en el reactor (matiz R6). *Direct I/O* con caché propia queda como profundidad opcional de Fase 4.

**Consecuencias.** (+) Una sola forma de I/O para ambos *targets*; coroutines naturales; un anillo por núcleo encaja con *shared-nothing*. (+) Portabilidad como capacidad documentada sin doble implementación inicial. (−) Un proactor es más complejo que un *reactor* epoll directo; si hiciera falta epoll, se emularía como *completion*. (−) Prohibir bloqueos en el reactor exige disciplina asíncrona total (allocators por núcleo, fsync asíncrono).

**Alternativas consideradas.**
- **Reactor epoll (readiness):** descartado como forma canónica por no mapear bien a IOCP (Windows).
- **Librería asíncrona externa (ASIO/Seastar):** descartada para el núcleo por ser una pieza de aprendizaje central; se valora Seastar solo como referencia conceptual.

### ADR-0003: Modelo de replicación (Raft por partición)

- **Estado:** **aceptado**
- **Fecha:** 2026-06-07

> **Provisional revisable:** se reconfirma con los *benchmarks* de la Fase 1; si cambia, se **reemplaza** por un ADR nuevo (no se edita).

**Contexto.** La Fase 2 requiere replicar particiones y tolerar caídas de líder. Dos arquitecturas de referencia: **Raft por partición** (cada partición es su propio grupo Raft; el log de Raft *es* el log de la partición — modelo Redpanda) y **ISR estilo Kafka** (consenso solo para metadatos/controller; los datos se replican por primario-backup con *high-watermark*).

**Decisión.** Adoptar **Raft por partición**: un **único mecanismo** de ordenación, replicación y elección por partición. El *high-watermark* se deriva del `commitIndex` de Raft; el modelo de `acks` (§4.3) se asienta sobre su *commit*. Se prefiere por ser **conceptualmente uniforme**, **formalmente especificado** y **testeable de forma determinista** —lo que sirve al objetivo de *correctitud demostrable*— y por componer con la arquitectura *shared-nothing* (un grupo Raft anclado a un núcleo).

**Consecuencias.** (+) Un solo mecanismo que razonar y probar; *failover* sin pérdida de datos *committed*; encaje con *thread-per-core*. (+) Base natural para el modelo de ACK y la postura CP (ADR-0007). (−) Coste de **quórum en cada escritura** (latencia de mayoría) en el camino caliente. (−) Muchas particiones = muchos grupos Raft = mucho *heartbeat* (no problemático a escala de portfolio; relevante a escala de miles de particiones). 

**Alternativas consideradas.**
- **ISR estilo Kafka:** mayor throughput potencial, pero más piezas (controller, gestión de ISR, *high-watermark* separado) y más superficie de error; descartado por complejidad y por ser el diseño "de hace 14 años".
- **Raft por partición:** elegido (ver Decisión).

### ADR-0004: Protocolo del plano de datos (binario propio + gateway REST)

- **Estado:** **aceptado**
- **Fecha:** 2026-06-07

**Contexto.** El plano de datos necesita un protocolo. Un **binario propio** maximiza el aprendizaje y el control (framing, varint, *multiplexing*/*correlation IDs*, versionado, control de flujo) y compone sin *impedance mismatch* con la arquitectura *shared-nothing*. La **compatibilidad con Kafka** daría ecosistema (kcat, consolas, clientes) pero obliga a implementar una especificación ajena y enorme (~70+ *API keys* versionadas, *record batch* v2, *consumer group protocol*…), atando el diseño y *front-loadeando* un esfuerzo ingrato.

**Decisión.** Implementar un **protocolo binario propio** para las fases nucleares, con un **gateway REST** para interoperabilidad temprana (cubre el "¿funciona con herramientas reales?" vía HTTP/curl/Postman). Diferir un **subconjunto Kafka-compatible** (`ApiVersions`/`Metadata`/`Produce`/`Fetch`) a la **Fase 4** como *stretch*: basta para la demo "kcat habla con mi broker" sin apostar el proyecto a la spec completa. El titular "Kafka-compatible" es así **recuperable más tarde** sin pagar su coste por adelantado.

**Consecuencias.** (+) Control total del protocolo; aprendizaje de diseño de protocolos (lo que el proyecto quiere demostrar); libertad para ajustarlo a *shared-nothing* y a créditos. (+) Interoperabilidad inmediata vía REST. (−) Sin ecosistema Kafka "gratis" hasta la Fase 4. (−) El cliente nativo hay que escribirlo (ya estaba en alcance, OE-2).

**Alternativas consideradas.**
- **Compat Kafka desde el inicio:** ecosistema instantáneo, pero esfuerzo enorme e ingrato, riesgo de *uncanny valley* (compat parcial que casi funciona) y diseño atado antes de existir; descartado para el núcleo, diferido como subconjunto de Fase 4.
- **Binario propio + REST:** elegido (ver Decisión).

### ADR-0005: Arquitectura de concurrencia (*shared-nothing thread-per-core* con reactor propio)

- **Estado:** **aceptado**
- **Fecha:** 2026-06-07

> **Provisional revisable** tras los *benchmarks* de la Fase 1; si cambia, se **reemplaza** por un ADR nuevo.

**Contexto.** El plano de datos exige un modelo de concurrencia. Dos familias: **thread-pool con estado compartido** (modelo Kafka clásico, *locks* sobre estructuras compartidas) frente a ***shared-nothing thread-per-core*** (un reactor por núcleo, estado por núcleo, paso de mensajes — modelo Redpanda/Seastar). El proyecto prioriza profundidad de sistemas, *correctitud demostrable* y estar en el estado del arte.

**Decisión.** Adoptar ***shared-nothing thread-per-core***: **un reactor por núcleo** (*pinned*), cada uno dueño de su anillo **io_uring**, su ***allocator*** y su subconjunto de particiones; comunicación entre reactores **solo por colas SPSC *cross-core*** (estilo `submit_to`). **Construir un reactor mínimo propio** sobre io_uring + corutinas C++20 — **no** usar Seastar (dependencia pesada, Linux-only; "construir" demuestra más que "usar", y encaja con la política §3.4 de hacer el núcleo a mano). Seastar queda como **referencia conceptual**. Decisión **provisional revisable** tras los *benchmarks* de Fase 1.

**Consecuencias.** (+) Elimina la clase de bug más cara (carreras sobre estado compartido); cachés/NUMA locales; sin *locks* en el camino caliente; estado del arte. (+) Compone con Raft por partición (un grupo anclado a un núcleo) y con io_uring (un anillo por núcleo). (−) **Disciplina asíncrona total**: nada bloqueante en el reactor (un `fsync`, `malloc` o *lock* que entre al kernel congela el núcleo) → exige fsync asíncrono y *allocators* por núcleo. (−) Coordinación *cross-core* explícita (SPSC + *wakeup*). (−) Si se adopta *direct I/O*, hay que implementar caché/*readahead* propios (no se usa el *page cache*).

**Alternativas consideradas.**
- **Thread-pool con work-stealing / estado compartido:** más simple de arrancar pero demuestra el diseño de hace 14 años, introduce contención y la clase de bug que queremos eliminar; descartado como primario.
- **Usar Seastar:** ahorra construir el reactor, pero demuestra menos, es Linux-only y pesado; descartado como dependencia, retenido como referencia.

### ADR-0006: Rol del *ingress* (dos modos: nativo directo + proxy/REST)

- **Estado:** **aceptado**
- **Fecha:** 2026-06-07

**Contexto.** En un plano de datos de alto rendimiento, interponer un proxy en el camino caliente añade un salto y rompe el *zero-copy*. Pero exigir siempre un *smart-client* excluye a clientes simples y a HTTP. Hay tensión entre rendimiento y conveniencia.

**Decisión.** Soportar **dos modos** con jerarquía explícita: (1) **nativo directo** (primario): *smart-client* que va al **líder** de la partición vía *metadata*, sin proxy; (2) **proxy/REST** (secundario, *opt-in*): el *ingress* enruta clientes "tontos" (*consistent hashing*) y expone un **gateway REST**, asumiendo el salto extra a sabiendas. Se documenta como **dos modos con *trade-offs***, no como "un *ingress* que lo hace todo".

**Consecuencias.** (+) Máximo rendimiento por defecto y conveniencia/interoperabilidad bajo demanda; demuestra **criterio** en vez de incoherencia. (−) Dos caminos que mantener y documentar; el cliente nativo debe gestionar *metadata* y descubrimiento de líder.

**Alternativas consideradas.**
- **Solo proxy:** simple para el cliente, pero penaliza el camino caliente; descartado como único.
- **Solo smart-client:** óptimo en rendimiento, pero excluye clientes simples y HTTP; descartado como único.

### ADR-0007: Postura de consistencia (CP / PACELC PC-EC)

- **Estado:** **aceptado**
- **Fecha:** 2026-06-07

**Contexto.** Todo sistema replicado elige, ante una partición de red, entre **consistencia** y **disponibilidad** (CAP); y, en operación normal, entre **latencia** y **consistencia** (PACELC). El proyecto prioriza correctitud y se apoya en Raft, que requiere quórum.

**Decisión.** Ser **CP**: ante partición, una partición de datos sin **quórum** de su grupo Raft **deja de aceptar escrituras** (no diverge). En PACELC: **PC/EC** — ante **P**artición elige **C**onsistencia, y en condiciones normales (**E**lse) también prioriza **C**onsistencia sobre **L**atencia (escrituras esperan al quórum con `acks=quorum`). Lectura por defecto desde el **líder** hasta el *high-watermark*; lecturas *stale* desde *followers* son *opt-in* y documentadas.

**Consecuencias.** (+) Sin divergencia ni pérdida de datos *committed*; coherente con Raft y con *correctitud demostrable*. (+) Modelo mental simple para el usuario. (−) Una partición minoritaria queda **no disponible** para escritura durante un *split* (se acepta como precio de la consistencia).

**Alternativas consideradas.**
- **AP (alta disponibilidad, escrituras en minoría con reconciliación posterior):** mayor disponibilidad pero abre divergencia y conflictos; incoherente con el modelo de log ordenado y con Raft; descartado.

### ADR-0008: Viabilidad de coste cero (desarrollo y test gratuitos; cloud como *target* opcional)

- **Estado:** **aceptado**
- **Fecha:** 2026-06-07

**Contexto.** El proyecto se **orienta** a despliegue en Azure/AWS, pero su valor formativo y de portfolio no debe depender de gasto recurrente en cloud. Conviene fijar que todo el ciclo se puede hacer gratis.

**Decisión.** Realizar **todo** el desarrollo, testing (unitario, integración, *crash*/chaos) y *benchmarking* **en local, sin coste**: *toolchain* y dependencias open source; cluster de 3 nodos vía **Docker Compose**; *chaos* con `tc netem`/`cgroups`; CI en **GitHub Actions** (gratis en público / *free tier* en privado); observabilidad **Prometheus+Grafana** *self-hosted*. El despliegue en **cloud** es un **objetivo de diseño** (imagen Docker, 12-factor) y, como mucho, una **demo opcional** cubrible con *free tiers* (AWS/Azure/Oracle Always Free), nunca un requisito de gasto. Los *benchmarks* se miden en local (los *free tiers* no sirven para cifras serias).

**Consecuencias.** (+) Barrera de entrada económica nula; reproducibilidad. (+) El diseño *cloud-ready* permite una demo desplegada si se desea. (−) Una demo en cloud real con *hardware* serio sí costaría dinero (egress, almacenamiento, instancias encendidas); se mitiga con IaC bajo demanda. (−) Los *free tiers* no permiten *benchmarks* representativos (se asume; se miden en local).

**Alternativas consideradas.**
- **Depender de cloud para test/bench:** descartado por coste y por peor reproducibilidad de los *benchmarks*.

### ADR-0009: Política de manejo de errores por capa (excepciones / `std::expected` / códigos de wire)

- **Estado:** **aceptado**
- **Fecha:** 2026-06-10

**Contexto.** `principios/manejo-errores.md` prescribe **excepciones, no códigos de error** para la **lógica de negocio de aplicación**, mientras que `stacks/cpp` avala **`std::expected`/`error_code`** en **código de sistemas y baja latencia**. El broker tiene **tres superficies de error** distintas: el **protocolo de red** (contrato), el **núcleo de sistemas** (camino caliente) y el **plano de control** (admin/REST). Hay que fijar qué mecanismo rige en cada una para no contradecir la normativa ni pagar el coste de excepciones en el *hot path*.

**Decisión.** Política por capa, alineada con la biblioteca:
- **Protocolo / *wire* (§7.2.2):** **códigos de error numéricos** (`errorCode:i16`) como **contrato**; se **traducen al modelo interno en el borde**, no se propagan crudos por el núcleo (`manejo-errores`: *"los códigos de error de protocolo… son legítimos"*).
- **Núcleo de sistemas / camino caliente** (storage, reactor, consensus): **`std::expected<T,E>`** (o `std::error_code`/`Result`) — el error es un **valor**, de coste predecible; **sin excepciones en el *hot path***. Se evaluará **`-fno-exceptions`** solo con **justificación medida** (coste de *unwinding* / latencia de cola).
- **Plano de control / aplicación** (admin, configuración): **excepciones para lo excepcional** + `std::optional` para ausencia; en el **borde REST**, traducción a **`ProblemDetail` (RFC 7807)** (§7.6).
- **Invariantes transversales (no cambian):** aportar **contexto** en el error; **validar en el borde** (*fail-fast*, §7.9); **una sola política central**; **nunca tragar** un error; **envolver** los errores de librerías de terceros (liburing/OpenSSL) en tipos propios.

**Consecuencias.** (+) Coherente con `principios/manejo-errores.md` **y** con `stacks/cpp`; sin coste de excepciones en el camino caliente; modelo de error de cara al cliente estándar (códigos en binario, RFC 7807 en REST). (−) Hay que **propagar `expected` a mano** (mitigado con los monádicos `and_then`/`or_else`) y mantener la **traducción en los bordes** entre los tres modelos.

**Alternativas consideradas.**
- **Excepciones en todo (incluido el *hot path*):** contradice la baja latencia (*unwinding* no determinista) y la guía de sistemas de `stacks/cpp`; descartada para el núcleo.
- **Códigos de error en todo (incluida la aplicación):** contradice `manejo-errores.md` para la lógica de aplicación; descartada fuera del núcleo/contrato.

### ADR-0010: Entorno de desarrollo (VS Code sobre WSL; reemplaza el IDE de ADR-0001)

- **Estado:** aceptado
- **Fecha:** 2026-06-11

> **Reemplaza la elección de IDE de ADR-0001.** El resto de ADR-0001 (*target* Linux primero, WSL2, CMake + vcpkg, Windows después) **sigue vigente**; ADR-0001 no se edita (un ADR aceptado se reemplaza, no se modifica).

**Contexto.** ADR-0001 fijó desarrollar **desde Visual Studio** (workload "Linux development with C++") sobre WSL2. El código del proyecto vive **dentro del sistema de ficheros de WSL** (`/home/…`, visible desde Windows como `\\wsl.localhost\…`). El modelo CMake+WSL de Visual Studio **asume que el código reside en el FS de Windows** y lo sincroniza a WSL con `rsync`; al abrir una carpeta que ya vive en WSL sobre el *share* `\\wsl.localhost`, VS **no activa la integración CMake** (no genera caché, no aparecen *targets* ni "Elemento de inicio"), con cuelgues al cerrar. Se verificó con **VS 2026 (18.7)**: presets válidos (`cmake --list-presets` OK), `enableCMake: true` y componentes Linux/WSL instalados — el bloqueo es de **diseño de VS**, no de configuración.

**Decisión.** Migrar el IDE a **VS Code conectado a WSL** (extensiones **Remote-WSL** + **CMake Tools** + **C/C++**), abriendo el proyecto en su ruta Linux real (`code .` desde WSL). CMake Tools consume el **mismo `CMakePresets.json`** y permite configurar/compilar/ejecutar/**depurar (gdb)** *nativo* en WSL, sin copias ni `rsync`. El código permanece en WSL (mejor I/O que `/mnt/c`). Se **estandariza** en los presets `linux-gcc/clang/asan` (Ninja, los mismos que el CI) y se eliminan los presets `wsl-ubuntu*` (Makefiles) y los *vendor maps* `microsoft.com/VisualStudioSettings` introducidos para VS.

**Consecuencias.** (+) El IDE trabaja donde vive el código: sin `rsync`, sin cuelgues, **un único `CMakePresets.json`** para IDE, CI y terminal, con idéntico *toolchain* (gcc/gdb/cmake). (+) Experiencia gráfica equivalente (configurar/compilar/ejecutar/depurar). (+) Menos superficie de configuración (un solo conjunto de presets). (−) Se renuncia a Visual Studio como IDE (preferencia previa del autor) para este proyecto. (−) Recuperar VS exigiría mover el repo al FS de Windows — descartado por el flujo *Linux-first*.

**Alternativas consideradas.**
- **Mover el repo a `C:\` y seguir en Visual Studio (modelo `rsync` de VS):** funcional, pero el origen pasa a Windows, con peor rendimiento de I/O en WSL2 y fricción de *line endings*/permisos; contradice *Linux-first*. Descartado.
- **Forzar VS sobre `\\wsl.localhost`:** no es flujo soportado; es la causa de la no-activación y de los cuelgues. Descartado.
- **VS Code + Remote-WSL:** elegido (ver Decisión).

### ADR-0011: Estándar C++23 + libc++ en Clang (para `std::expected`)

- **Estado:** aceptado
- **Fecha:** 2026-06-11

> **Refina el mecanismo de ADR-0009** (no lo reemplaza). ADR-0009 fija `expected<T>` = `std::expected<T, Error>` en el núcleo; este ADR registra las **consecuencias de toolchain** de esa decisión, descubiertas al implementar el modelo de errores en M2.

**Contexto.** `std::expected` es **C++23**, pero el proyecto se fijó en C++20 (`cxx_std_20`). Al implementar `error.hpp` se constató, además, que **Clang 18 + libstdc++ no compila `<expected>`** ni en C++23: el `<expected>` de libstdc++ exige `__cpp_concepts >= 202002L` y Clang 18 reporta `201907L`. GCC (libstdc++) sí lo compila (`__cpp_concepts = 202002L`). Sin solución, el *lane* de Clang del CI —y **clang-tidy**, que parsea con el frontend de Clang— quedarían rotos en todo fichero que use el modelo de errores.

**Decisión.** Subir el estándar a **`cxx_std_23`**. **GCC** compila con **libstdc++** (por defecto). **Clang** compila con **libc++** (`-stdlib=libc++`), fijado **globalmente** en el preset `linux-clang` y en el CI (build + lint), de modo que también las dependencias traídas por FetchContent (GoogleTest/Benchmark) usen libc++ y no haya choque de ABI. `clang-tidy` consume el `compile_commands.json` de ese preset, así que parsea con libc++. Se documenta en `CLAUDE.md` (hechos del proyecto) y se requiere `libc++-dev`/`libc++abi-dev` donde se use Clang.

**Consecuencias.** (+) `std::expected` disponible y **probado en ambos compiladores** (GCC/libstdc++ y Clang/libc++); se conserva la cobertura de Clang y de clang-tidy. (+) Modelo de error de cara al cliente estándar (sin coste de excepciones en el camino caliente, ADR-0009). (−) Conviven **dos** librerías estándar (libstdc++ con GCC, libc++ con Clang): hay que instalar libc++ donde se compile con Clang; algo más de superficie de toolchain. (−) El estándar mínimo sube de C++20 a C++23 (el toolchain objetivo lo soporta de sobra).

**Alternativas consideradas.**
- **`expected` propio (C++20) sobre `std::variant`:** portable sin libc++ y sin subir de estándar, pero se aparta del mecanismo elegido en ADR-0009 (`std::expected`); descartado por preferencia explícita de mantener el tipo estándar.
- **`std::expected` solo con GCC (sin lane Clang):** simple, pero pierde la cobertura de Clang y deja **clang-tidy** sin poder parsear el modelo de errores; descartado.

### ADR-0012: Backend io_uring directo sobre el uapi del kernel (sin liburing)

- **Estado:** aceptado
- **Fecha:** 2026-06-12

> **Precisa el desglose de `nexus-io`** (no cambia el diseño): el desglose anotaba el backend del `Proactor` como *«io_uring (liburing)»*. Este ADR registra que la **implementación** habla con io_uring **directamente** por el uapi del kernel, sin la biblioteca liburing. El **puerto `Proactor` y sus contratos no cambian**; solo el backend.

**Contexto.** El backend io_uring (R5) debe cumplir la **puerta de calidad**: compilar y testear **en local y en CI**, en GCC y Clang, bajo sanitizers. En el entorno de desarrollo no hay `sudo` para instalar `liburing-dev` y vcpkg no está *bootstrapeado* (se cae a FetchContent); liburing usa *autotools* (no CMake), así que vendorizarla reproduciblemente es frágil. En cambio, el **uapi del kernel** (`<linux/io_uring.h>`) está presente tanto en local (WSL2, kernel 6.18) como en el *runner* de CI (ubuntu-24.04), y io_uring funciona en tiempo de ejecución en local (probado: `io_uring_setup` OK, `IORING_FEAT_SINGLE_MMAP`).

**Decisión.** Implementar `IoUringBackend` **directamente sobre el uapi**: `io_uring_setup`/`io_uring_enter` vía `syscall`, anillos SQ/CQ mapeados con `mmap` (se exige `IORING_FEAT_SINGLE_MMAP`), barreras *acquire/release* sobre los índices compartidos con el kernel y SQEs gestionadas a mano. **Cero dependencias externas**: el mismo binario compila en todas partes con solo cabeceras del kernel. CMake compila el backend solo donde existe `<linux/io_uring.h>` (define `NEXUS_HAVE_IOURING`); el *smoke-test* hace E/S real y se **omite en ejecución** (`GTEST_SKIP`) si io_uring no está disponible (kernel viejo, *seccomp* del CI). Toda la gestión cruda (fd, mmap, ops en vuelo) queda confinada en un *pimpl* RAII, sin filtrar el uapi al resto del árbol.

**Consecuencias.** (+) Sin dependencias ni instalación: build idéntica en local y CI, sin `sudo`/vcpkg/autotools. (+) Control total del anillo (afín al *shared-nothing* y al objetivo de aprender sistemas) y *hot-path* sin biblioteca intermedia. (+) Validado en local en los cuatro *lanes* (GCC/Clang/ASan/TSan) porque io_uring corre aquí. (−) Reimplementamos lo que liburing ofrece hecho (setup de anillos, *helpers* de SQE): más código propio que mantener y más superficie de error de bajo nivel. (−) Acceso a uniones del `io_uring_sqe` (ABI del kernel): obliga a desactivar `cppcoreguidelines-pro-type-union-access` (igual criterio que `pro-bounds-*`/`pro-type-vararg` para código de sistemas). (−) `wake()` queda como *no-op* hasta R6 (requiere registrar un `eventfd` en el anillo).

**Alternativas consideradas.**
- **liburing vía `liburing-dev` (apt):** lo estándar, pero requiere `sudo` (no disponible) y añade dependencia de sistema a instalar en cada entorno; descartado por la restricción del entorno.
- **liburing vendorizada (FetchContent/ExternalProject):** reproducible sin `sudo`, pero liburing usa *autotools* (genera cabeceras con `./configure`): wrapper frágil en CMake y más lento en CI; descartado por complejidad frente a la opción sin dependencias.

### ADR-0013: Capa `nexus-wire` para el framing sobre conexión (`FrameReader`/`FrameWriter`)

- **Estado:** aceptado
- **Fecha:** 2026-06-13

> **Refina el desglose** (no cambia el diseño del protocolo): el desglose detallado ubicaba `FrameReader`/`FrameWriter` en `protocol/frame.hpp`, pero el grafo de dependencias fija `nexus-protocol → common` y «protocolo puro (encode/decode, sin E/S ni async)». Este ADR resuelve ese conflicto entre dos fuentes de verdad.

**Contexto.** `FrameReader`/`FrameWriter` leen y escriben tramas longitud-prefijo (§7.2) sobre un `Socket` mediante el `Proactor`: necesitan `nexus-io` (Socket, Proactor) y corrutinas (`task<expected<T>>`). Colocarlos en `nexus-protocol` obligaría a que el protocolo —hoy puro encode/decode, testable y fuzzeable sin E/S— dependiera de `nexus-io` y de la maquinaria async, contradiciendo el grafo (protocol→common) y el principio de capas limpias. Los consumidores del framing-sobre-conexión (broker, client, ingress, server) ya dependen tanto de `io` como de `protocol`.

**Decisión.** Crear un target nuevo **`nexus-wire`** (`src/wire/`), que depende de **common + io + protocol** y aloja `Frame`, `FrameReader` y `FrameWriter`. `nexus-protocol` se mantiene **puro** (codec, `FrameHeader`, mensajes, códigos de error: solo→common, sin E/S ni async, independientemente testeable). broker/client/ingress/server dependen de `nexus-wire` para el transporte de tramas. El `Buffer` de `nexus-common` gana `extend`/`truncate` (cola mutable para `recv` sin copia intermedia), que usa `FrameReader`.

**Consecuencias.** (+) `nexus-protocol` queda independiente de E/S: la (de)serialización se prueba y fuzzea sin tocar sockets ni io_uring. (+) Separación de responsabilidades clara (protocolo = bytes; wire = tramas sobre conexión) y reutilizable por todos los consumidores. (+) Grafo acíclico y descendente (wire → {common, io, protocol}). (−) Un target más en el árbol (15 en total) y una desviación respecto a la ubicación del desglose detallado (anotada en la hoja de ruta). (−) `read_frame` expone el payload como vista **dentro del búfer del lector** (válida hasta la siguiente lectura): zero-copy a cambio de una invariante de vida documentada.

**Alternativas consideradas.**
- **`nexus-protocol` depende de `io` (FrameReader en `protocol/frame_io.hpp`):** sigue el desglose detallado y evita un target nuevo, pero rompe la pureza del protocolo (lo acopla a io_uring/async) y el grafo protocol→common; descartado por el autor a favor de capas limpias.
- **FrameReader genérico en `nexus-io` (sin conocer `FrameHeader`):** mantiene io→common, pero parte el concepto de «trama» en dos capas y se aparta del `read_frame()→Frame` del desglose; descartado.

### ADR-0014: Modelo del log de Raft (índice = ordinal de entrada; término en sidecar)

- **Estado:** aceptado
- **Fecha:** 2026-06-14

> **Refina el desglose** (no cambia ADR-0003): concreta *cómo* `RaftLog` es «una vista `(term,index)` sobre el `PartitionLog`» cuando el formato de `RecordBatch` (§5.4) no lleva campo de término y los offsets del log son **por record** (un batch abarca varios offsets).

**Contexto.** ADR-0003 fija que «el log de Raft **es** el log de la partición». Al implementarlo surgen dos fricciones: (1) el `RecordBatch` (la unidad de replicación, §5.4) **no tiene** campo de término, y añadírselo cambiaría el formato en disco y su CRC (toca todo `nexus-storage`); (2) el offset de partición es **por record** (un batch de N records ocupa N offsets), mientras que Raft razona con un **índice por entrada**. Igualar «índice = offset» obligaría a entradas de un solo record (mata el *batching*, contrario al §5.4) o a partir batches al truncar.

**Decisión.** Una **entrada de Raft ↔ un `RecordBatch`**. El **índice de Raft es el ordinal de la entrada** (1-based, +1 por entrada, contiguo), **espacio distinto** del offset de partición (por record). `RaftLog` envuelve un `PartitionLog` (que almacena los bytes de cada entrada y asigna sus offsets) y **posee el mapeo** `índice → (term, base_offset, last_offset, type)`. El **término** (y el resto de metadatos por entrada) se persiste en un **sidecar** de registros de tamaño fijo (`raft-meta`, 25 B/entrada: `term:i64 | base_offset:i64 | last_offset:i64 | type:u8`), append-only y truncable; así el log de partición conserva `RecordBatch` **intactos** (el *fetch* del consumidor los lee sin desenvolver) y la recuperación del término no exige tocar el formato del batch. El *high-watermark* visible (espacio de offsets) = `last_offset` de la entrada en `commit_index`. Resolución de conflictos (`truncate_from(index)`) → `PartitionLog::truncate_to(base_offset(index))` (frontera de batch, ADR/C3) + truncado del sidecar.

**Consecuencias.** (+) `RecordBatch` y `nexus-storage` quedan **sin cambios** (formato y CRC estables); el *fetch* sirve batches tal cual. (+) El algoritmo de Raft opera sobre índices contiguos +1 (formulación estándar del *paper*; sin aritmética de offsets). (+) El sidecar de tamaño fijo hace `term_at`/`last_term`/recuperación O(1)/lineal triviales. (−) Dos espacios de coordenadas (índice de entrada vs offset de record) que la capa de partición reconcilia (el *high-watermark* traduce de uno a otro). (−) Un fichero sidecar por partición además del `.log`/`.index`. (−) El mapeo se persiste por duplicado parcial (base/last son derivables del log escaneándolo), a cambio de un `open` simple sin re-escanear el log.

**Alternativas consideradas.**
- **Añadir `leader_epoch`/term a la cabecera del `RecordBatch`** (estilo Kafka v2): fiel a «el log es el log», pero cambia el formato en disco y el CRC, e impacta todo `nexus-storage` y sus tests; descartado para no reabrir un formato ya estabilizado en Fase 1.
- **Índice = offset+1 con entradas de un solo record:** iguala ambos espacios, pero elimina el *record batch* como unidad (contradice §5.4 y el modelo Kafka); descartado.
- **Anidar el `RecordBatch` del cliente dentro de un marco Raft** (term/type + payload como records del batch del log): término durable sin sidecar, pero cambia la semántica del offset a *por batch* (rompe el modelo por record de la Fase 1b) y obliga al *fetch* a desenvolver; descartado.

### ADR-0015: RaftNode como máquina de estados síncrona sin E/S (entradas→salidas)

- **Estado:** aceptado
- **Fecha:** 2026-06-14

> **Refina el desglose** (no cambia ADR-0003 ni la mecánica de Raft): el desglose detallado modela `RaftNode` con `propose`/`replicate_to` como **corrutinas** y un puerto `RaftTransport` (`task<expected<…>> send_append/send_vote`). Este ADR reorganiza esa frontera sin alterar el algoritmo.

**Contexto.** El objetivo nº1 de Fase 2 es la **correctitud demostrable** de Raft mediante **simulación determinista** con reloj y red virtuales (§8.1). Si `RaftNode` hace E/S por dentro (corrutinas que `co_await` un transporte), el reloj y la red quedan **dentro** del nodo y la prueba determinista exige inyectar un proactor/transport asíncrono y conducir corrutinas — friccionando con FIRST y con la reproducibilidad. El patrón estándar de implementaciones de Raft probadas (etcd/raft, TigerBeetle, ongaro) es un **núcleo sin E/S**: el nodo consume *entradas* (ticks de reloj, RPC recibidos) y produce *salidas* (mensajes a enviar, entradas a aplicar); el tiempo, la red y el disco viven **fuera**.

**Decisión.** `RaftNode` (`src/consensus/raft_node.hpp/.cpp`) es una **máquina de estados síncrona y sin E/S**: sin `RaftTransport`, sin corrutinas, sin reloj propio. Entradas: `tick(now)`, `on_request_vote(now, args) → reply`, `on_append_entries(now, args) → reply`, `on_request_vote_reply(now, from, reply)`, `on_append_entries_reply(now, from, reply)`, `propose(batch)` (C6). Salidas: una cola de mensajes salientes proactivos (`RaftMessage{from,to,payload}` con `payload` = `variant` de los RPC) drenable con `take_messages()`; las entradas confirmadas se exponen vía `commit_index()` (el broker lee el log hasta ahí). El **reloj se inyecta** como `MonoTime now` en cada entrada (los *timeouts* de elección/heartbeat son aritmética pura sobre `now`); la **aleatorización** del *election timeout* usa un RNG sembrado (`random_seed + self`), reproducible. En producción, un adaptador del reactor traduce: temporizador→`tick`, RPC recibido→`on_*`, mensajes de `take_messages()`→envíos por `nexus-wire` (se cablea en la integración con el broker, C9). Respecto al desglose: se **añade** `now` a las firmas de los manejadores (determinismo) y se **sustituye** el par `propose: task<…>` + `RaftTransport` por `propose` síncrono + cola de salida (el adaptador asíncrono vive fuera del núcleo).

**Consecuencias.** (+) Simulación **determinista** trivial: el arnés (C8) tiene un reloj virtual y una red virtual (cola de mensajes con retardos/particiones programables) y dirige N `RaftNode` reproduciblemente; cero hilos, cero E/S real en el test del algoritmo. (+) El núcleo de Raft se razona y prueba aislado de io_uring/corrutinas. (+) Encaja con *shared-nothing*: un `RaftNode` por partición, dirigido por su reactor. (−) Hace falta un **adaptador** (reactor↔nodo) que el desglose no nombraba (se añade en C9). (−) El emisor debe drenar `take_messages()` tras cada entrada (contrato explícito).

**Alternativas consideradas.**
- **`RaftNode` con corrutinas + `RaftTransport` (desglose literal):** menos piezas en producción, pero mete reloj/red dentro del nodo y complica la prueba determinista (el objetivo nº1); descartado.
- **Núcleo sin E/S con *callbacks* de envío** (en vez de cola drenable): equivalente, pero los *callbacks* reintroducen acoplamiento e dificultan inspeccionar las salidas en el test; se prefiere la cola explícita.

### ADR-0016: `ReplicatedPartition` como tipo paralelo a `Partition` (composición de la pila Raft)

- **Estado:** aceptado
- **Fecha:** 2026-06-15

> **Refina el desglose** (no cambia ADR-0003/0015): el desglose detallado preveía **mutar** `Partition` para intercalar `RaftNode` y convertir `produce`/`fetch` en `task<expected<…>>`. Este ADR mantiene `Partition` intacta y añade un tipo nuevo, sin alterar el algoritmo ni la frontera de E/S de ADR-0015.

**Contexto.** En C9 hay que dar a la partición respaldo Raft (`acks=quorum`, *high-watermark* = `commit_index`, ADR-0003). La `Partition` de la Fase 1b funciona con *ack* local y sirve al broker e2e que ya pasa. Dos fricciones al mutarla in situ: (1) la pila de consenso es **autorreferencial** —`PartitionLog` ← `RaftLog`(ref) ← `RaftNode`(ref)—, así que un tipo con esos miembros por valor no sería **movible** sin invalidar referencias internas; (2) `RaftNode` es una **FSM sin E/S** (ADR-0015) que un portador externo debe conducir (`tick`/`on_*`/`take_messages`), contrato que aún no existe en el broker (llega con el *routing* multi-reactor, C11). Mutar `Partition` ahora mezclaría ambos modelos (ack local vs quorum) en un tipo en uso y arriesgaría el camino e2e verde.

**Decisión.** Se añade **`ReplicatedPartition`** (`src/broker/replicated_partition.{hpp,cpp}`) como **tipo paralelo** a `Partition`, sin tocar esta última. Compone la pila por **`unique_ptr`** (`PartitionLog` + `RaftLog` + `RaftNode`): las direcciones de heap son estables, por lo que las referencias internas siguen válidas y el objeto es **movible** (move por defecto, copia borrada). `produce` (solo líder; `Unsupported` si no) aplica la idempotencia por productor (§5.9, reutilizada de `Partition`) y **propone** la entrada al `RaftNode`, traduciendo el índice de Raft asignado a su último offset de partición vía `RaftLog::offsets_at`; `high_watermark()` = offset (exclusivo) de la entrada en `commit_index` (0 ⇒ `log_start_offset`). La FSM **no se conduce sola**: `raft()` expone la superficie para que el portador (reactor o arnés) la dirija; en C9 se valida con **enrutado directo/simulado** (test con reloj y red virtuales que comprueba que una escritura no es visible hasta el quorum). El **cambio en caliente** del broker a `ReplicatedPartition` (con transporte real) se difiere a **C11**.

**Consecuencias.** (+) `Partition` y el broker e2e quedan **intactos** (sin riesgo en el camino verde); la unidad de partición replicada queda lista y probada aislada. (+) Tipo **movible** pese a la pila autorreferencial (los `unique_ptr` fijan las direcciones). (+) Reutiliza idempotencia y `PartitionLog`/`RaftLog` sin duplicar lógica. (+) `produce` síncrono (la FSM no hace E/S, ADR-0015); cuando llegue el reactor, el portador drena `take_messages()` y espera a `commit_index`. (−) **Dos** tipos de partición conviviendo hasta C11 (deuda temporal explícita). (−) El cliente de `ReplicatedPartition` debe conducir `raft()` (contrato externo, como en ADR-0015).

**Alternativas consideradas.**
- **Mutar `Partition` (desglose literal):** un solo tipo, pero rompe su movilidad por la pila autorreferencial, mezcla ack local/quorum en un tipo en uso y arriesga el e2e verde antes de tener el portador (C11); descartado.
- **`RaftNode` por valor dentro de la partición:** evita una indirección, pero el tipo deja de ser movible (referencias internas colgantes al mover) y complica almacenarlo en contenedores del broker; descartado a favor de los `unique_ptr`.
- **Fusionar ambos tras C11** (un tipo con modo local/quorum): posible evolución futura; hoy se prefiere separar para no acoplar la Fase 1b a la 2.

### ADR-0017: Target `nexus-telemetry` para observabilidad (métricas/logs) bajo el broker

- **Estado:** aceptado
- **Fecha:** 2026-06-17

> **Refina el desglose** (no cambia ninguna decisión de arquitectura previa): el desglose detallado (§4.9) ubicaba `metrics.{hpp,cpp}` dentro de `nexus-server`, que es el **ejecutable** del broker. Este ADR mueve la observabilidad a una **biblioteca** propia para respetar el grafo de dependencias.

**Contexto.** La `MetricsRegistry` es **THREAD-SAFE** y la **alimentan** capas del plano de datos (broker/reactor: tasa de produce/fetch, *lag*, `commit_index`/term de Raft, latencias) mientras que la **exponen** el gateway REST (`nexus-ingress`, que tiene un `MetricsRegistry&`) y el servidor (`/metrics`). Si viviera en `nexus-server` (un *exe*, no enlazable como dependencia) ninguna biblioteca inferior podría registrar métricas; y colocarla en `nexus-ingress` crearía una dependencia ascendente `broker → ingress` (ciclo de capas). Lo mismo aplica al logger JSON estructurado (lo usa todo el árbol). `nexus-common` se reserva para vocabulario mínimo (tipos, errores, bytes); una registry con mutex, familias y render Prometheus no encaja ahí.

**Decisión.** Crear el target **`nexus-telemetry`** (`src/telemetry/`), que **depende solo de `nexus-common`** y se sitúa **bajo** broker/ingress/server en el grafo. Aloja `MetricsRegistry` (contadores/gauges/histogramas + exposición Prometheus) y, más adelante, el logger JSON estructurado. La registry es THREAD-SAFE con un **mutex que protege solo la estructura** (alta de series y recorrido en `render`); las **actualizaciones de valor** (`inc`/`set`/`observe`) son **atómicas y sin candado** (lock-free en el camino caliente). Cualquier capa (broker, reactor, ingress, server) enlaza `nexus-telemetry` para registrar o exponer.

**Consecuencias.** (+) Grafo de dependencias **acíclico**: el plano de datos registra sin depender de capas superiores. (+) Observabilidad **testeable en aislamiento** (sin red ni servidor). (+) `nexus-common` se mantiene mínimo. (+) Registro de valores **lock-free**; solo el alta de una serie nueva toma el mutex. (−) Un target más que mantener. (−) El *sharding* por núcleo de los contadores (cero contención total) queda como optimización futura medida; hoy basta con atómicos.

**Alternativas consideradas.**
- **`metrics` en `nexus-server` (desglose literal):** imposible que las bibliotecas inferiores registren métricas (un *exe* no es dependencia); descartado.
- **`metrics` en `nexus-ingress`:** crea el ciclo de capas `broker → ingress`; descartado.
- **`metrics` en `nexus-common`:** mete infraestructura con estado (mutex, familias, render) en la capa de vocabulario mínimo; descartado por cohesión.

---

### ADR-0018: REST admin por **puerto/adaptador** (`AdminService` en ingress, `AdminApi` en server)

- **Estado:** aceptado
- **Fecha:** 2026-06-18

> **Refina el desglose** (no cambia ninguna decisión de arquitectura previa): el desglose detallado (§4.9) ubicaba `admin_api.{hpp,cpp}` dentro de `nexus-server` y, a la vez (§4.8), hacía que `RestGateway` (en `nexus-ingress`) tuviera un `AdminApi&`. Eso crea un **ciclo de capas** `ingress → server` (la pasarela usa la API) y `server → ingress` (el `Server` posee la `Ingress`). Este ADR rompe el ciclo con **inversión de dependencias**, igual que ADR-0017 hizo con la telemetría.

**Contexto.** El `RestGateway` (plano de ingress) traduce HTTP↔dominio y necesita ejecutar operaciones de administración (crear/borrar/describir/listar topics y grupos). La lógica de esas operaciones vive sobre `TopicManager`/`GroupCoordinator`, que son del **broker** (capa inferior a server, pero `ingress` **no** depende de broker en el grafo: `ingress → common, io, protocol, wire`). Si el gateway dependiera de un `AdminApi` concreto alojado en `nexus-server`, `ingress` dependería de `server` (que ya depende de `ingress`): ciclo.

**Decisión.** Definir el **puerto** `AdminService` (interfaz abstracta) y sus **DTOs** (`CreateTopicSpec`, `TopicSummary`, `PartitionInfo`, `TopicDescription`, `GroupSummary`) en **`nexus-ingress`** (`ingress/admin_service.hpp`), como tipos de datos planos sin dependencia del broker. El `RestGateway` depende solo de `AdminService&`. El **adaptador** concreto `AdminApi` (en **`nexus-server`**, `server/admin_api.{hpp,cpp}`) **implementa** `AdminService` sobre `TopicManager&` y un *group lister* inyectado (`std::function`), traduciendo los tipos del broker a los DTOs del puerto. La enumeración de grupos es **reactor-local** (cada `GroupCoordinator` vive en su reactor), así que se inyecta como función desde el cableado del server (I14), no se acopla al puerto. Se añade a `TopicManager` un accesor de observabilidad `list_metadata()` (control-plane).

**Consecuencias.** (+) Grafo **acíclico**: `ingress` define el puerto (sin nuevas dependencias), `server` implementa el adaptador (ya depende de `ingress` y `broker`). (+) `RestGateway` es **testeable** con un doble de `AdminService` sin levantar broker. (+) Los DTOs del REST quedan **desacoplados** de los tipos internos (ADR-0009: traducción en el borde). (−) Una capa de traducción DTO↔dominio en el adaptador. (−) El listado de grupos cross-core real se materializa en el cableado (I14), no en el puerto.

**Alternativas consideradas.**
- **`AdminApi` concreto en `nexus-server`, referenciado por `ingress` (desglose literal):** crea el ciclo `ingress ↔ server`; descartado.
- **`AdminApi` en `nexus-broker`:** tendría que implementar la interfaz `AdminService` de `ingress` ⇒ `broker → ingress` (dependencia **ascendente**, prohibida); descartado.
- **`RestGateway` como router genérico de *handlers* (`std::function`):** rompe el ciclo, pero diluye el contrato de administración en *callbacks* sin tipado; el puerto explícito documenta mejor la API y es más fiel al desglose.

---

## 10. Glosario

| Término | Definición |
| ------- | ---------- |
| **Topic** | Flujo lógico de mensajes, dividido en particiones. |
| **Partición** | Log append-only ordenado; unidad de paralelismo y de replicación; en NexusMQ, **un grupo Raft** anclado a un núcleo. |
| **Offset** | Identificador lógico monótono de un registro dentro de una partición. |
| **Segmento** | Fichero que materializa un tramo del log de una partición (`.log` + `.index`). |
| **Índice disperso** | Mapa `offset → posición` cada N bytes, para *seek* eficiente. |
| **Record batch** | Unidad de escritura/replicación que agrupa muchos registros bajo una cabecera, un CRC32C y compresión común (estilo Kafka v2). |
| **WAL** | *Write-Ahead Log*: se escribe el cambio antes de aplicarlo, para recuperar tras *crash*. Bajo Raft, el log de Raft *es* el WAL. |
| **Raft** | Algoritmo de consenso (elección de líder + replicación de log) formalmente especificado. En NexusMQ, **uno por partición**. |
| **High-watermark** | Mayor offset *committed* y, por tanto, visible a consumidores. Bajo Raft, el `commitIndex`. |
| **commitIndex** | En Raft, el índice de log replicado por la mayoría y considerado *committed*. |
| **acks** | Política de confirmación de escritura: `0` (sin esperar), `1` (líder), `quorum` (mayoría Raft; por defecto). |
| **Shared-nothing** | Modelo en el que cada unidad (núcleo) posee su estado y no comparte memoria mutable; se coordina por mensajes. |
| **Thread-per-core** | Un hilo/reactor por núcleo físico, *pinned*, dueño de su I/O, memoria y particiones. |
| **Reactor / Proactor** | Modelos de I/O asíncrona: *reactor* (basado en *readiness*, epoll) vs *proactor* (basado en *completions*, io_uring/IOCP). |
| **Cross-core message passing** | Comunicación entre reactores por colas SPSC con despertar del destino (estilo `submit_to`). |
| **Backpressure (por créditos)** | Control de flujo en el que el receptor concede *créditos*; sin créditos, el emisor se frena (colas acotadas). |
| **Direct I/O** | I/O que evita el *page cache* del SO (`O_DIRECT`), a cambio de gestionar caché/alineación propias. |
| **NUMA** | *Non-Uniform Memory Access*: la memoria local al nodo del núcleo es más rápida; los *allocators* por núcleo la explotan. |
| **Reloj monotónico** | Reloj que nunca retrocede; se usa para *timeouts*/heartbeats/elección Raft (no el *wall-clock*). |
| **Circuit breaker** | Patrón que "abre" el circuito ante fallos para no insistir contra un destino caído. |
| **Token bucket** | Algoritmo de *rate limiting* basado en una cubeta de *tokens* que se rellena a tasa fija. |
| **CRC32C** | Variante de CRC de 32 bits con instrucción hardware (SSE4.2), usada como *checksum*. |
| **Productor idempotente** | Productor con *producer-id* + *sequence* que evita duplicados por reintento (*effectively-once* por partición). |
| **DLQ** | *Dead Letter Queue*: destino de mensajes no procesables tras N intentos. |
| **PACELC** | Marco que extiende CAP: ante **P**artición, **A** o **C**; **E**lse (normal), **L**atencia o **C**onsistencia. |
| **Coordinated omission** | Sesgo de medición de latencia en clientes *closed-loop*; se evita con un generador *open-loop*. |
| **Quórum** | Mayoría de un grupo Raft (⌊n/2⌋+1) necesaria para *commit* y para elegir líder. |
| **Snapshot (Raft)** | Imagen compactada del estado que permite truncar el log replicado y acotar su tamaño. |
| **Leader epoch** | Número que identifica el mandato de un líder de partición; detecta y descarta líderes obsoletos. |
| **Decompression bomb** | *Batch* comprimido que se expande desmesuradamente al descomprimir; se mitiga con límites de ratio/tamaño. |
| **Liveness / Readiness** | Estar vivo (el proceso responde) vs estar listo para tráfico (*checks* de disco/Raft/*lag*). |

---

## 11. Referencias y bibliografía

- Kleppmann, M. *Designing Data-Intensive Applications*. O'Reilly, 2017. (DDIA)
- Ongaro, D.; Ousterhout, J. *In Search of an Understandable Consensus Algorithm (Raft)*. USENIX ATC, 2014.
- Ongaro, D. *Consensus: Bridging Theory and Practice* (tesis; *pre-vote*, *leadership transfer*, *learners*, snapshots).
- Apache Kafka — *Documentation*, diseño del *log* particionado, *high-watermark* y formato *record batch* v2.
- Redpanda — arquitectura *thread-per-core* / shared-nothing (Seastar) y Raft por partición.
- Seastar — *framework* (referencia conceptual del reactor *per-core*: `submit_to`, *futures*, shared-nothing).
- Herlihy, M.; Shavit, N. *The Art of Multiprocessor Programming*. (Concurrencia, lock-free, consenso.)
- Gregg, B. *Systems Performance*. (Método USE, *profiling*, latencia.)
- Tene, G. *How NOT to Measure Latency* / HdrHistogram. (*Coordinated omission*, percentiles.)
- Drepper, U. *What Every Programmer Should Know About Memory*. (Caché, NUMA, localidad.)
- Petrov, A. *Database Internals*. (Storage engines, replicación, consenso.)
- Bryant, R.; O'Hallaron, D. *Computer Systems: A Programmer's Perspective* (CS:APP).
- Kerrisk, M. *The Linux Programming Interface* (TLPI). (syscalls, fsync, mmap.)
- Nygard, M. *Release It!*. (Circuit Breaker, Bulkhead, patrones de estabilidad.)
- Axboe, J. *Efficient IO with io_uring*. (Documento de diseño de io_uring.)

> Estas referencias cubren los fundamentos de datos distribuidos, concurrencia, rendimiento, memoria y sistemas/SO.

---

## 12. Anexos

- **Anexo A — `docs/protocol.md`** (pendiente): especificación del protocolo binario propio (framing, *correlation IDs*, versionado, créditos).
- **Anexo B — Diagramas** (pendiente): casos de uso, componentes, secuencia y modelo de datos en formato vectorial.
- **Anexo C — Resultados de benchmarks** (pendiente, Fase 1): tablas y gráficas de throughput y latencia (metodología §8.2).

---

**Autor.** Andrés Ojeda Rodríguez · [andresojedarodriguez@gmail.com](mailto:andresojedarodriguez@gmail.com)

**Licencia.** Documentación bajo **PolyForm Noncommercial License 1.0.0** (coherente con el resto del portfolio). © 2026 Andrés Ojeda Rodríguez.
