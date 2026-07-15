# 28. Registro de decisiones (ADR)

Este capítulo es la puerta de entrada al **registro de decisiones de arquitectura** (ADR) de NexusMQ: explica qué es un ADR, la regla de inmutabilidad que rige su ciclo de vida, y ofrece el **índice completo** de los 38 ADR del proyecto, agrupados por tema. Las decisiones íntegras viven en el directorio [`../adr/`](../adr/), una por fichero.

## 28.1 Qué es un ADR

Un *Architecture Decision Record* (ADR) documenta **una** decisión de arquitectura: la fuerza o problema que la motiva (*Contexto*), lo que se decide (*Decisión*), los efectos positivos y negativos que se aceptan (*Consecuencias*) y las opciones descartadas con su porqué (*Alternativas consideradas*). El valor de un ADR no es la decisión en sí, sino el **rastro del razonamiento**: por qué se eligió esto y no aquello, qué se sacrificó y bajo qué supuestos. Es la pieza de portfolio que evidencia el **criterio de ingeniería**, no solo el resultado.

Cada ADR de NexusMQ sigue la plantilla común (*Contexto · Decisión · Consecuencias · Alternativas consideradas*) y conserva su **estado** y su **fecha**. Cada decisión vive en un fichero individual `adr-NNNN-titulo-corto.md` bajo [`../adr/`](../adr/).

## 28.2 La regla de inmutabilidad

**Una decisión `aceptado` no se edita.** Si la realidad obliga a cambiarla, no se modifica el ADR existente: se **escribe uno nuevo** que la reemplaza, y el antiguo pasa al estado *reemplazado por adr-NNNN*. La historia de decisiones es inmutable, igual que la historia de git: poder leer la decisión vieja —y por qué se superó— es tan valioso como leer la nueva.

En NexusMQ esto produce una **cadena de reemplazo** visible en el backend de I/O de Windows (IOCP), que se diseñó y verificó en tres pasos sucesivos:

> **[ADR-0021](../adr/adr-0021-iocp-diseno-diferido.md)** (diseño fijado, implementación diferida)
> → **[ADR-0022](../adr/adr-0022-iocp-implementado-mingw.md)** (implementado, compile-verificado con MinGW)
> → **[ADR-0023](../adr/adr-0023-iocp-runtime-msvc.md)** (verificado en runtime con MSVC).

Cada eslabón cae cuando una premisa se demuestra falsa: ADR-0021 asumía que el backend «no era verificable en este entorno»; resultó que el contenedor **sí** podía instalar el cross-compiler MinGW-w64 (ADR-0022), y finalmente se **ejecutó** en una máquina Windows real (ADR-0023). La cadena documenta esa progresión sin reescribir el pasado.

## 28.3 Índice de los 38 ADR

| ADR | Título | Estado | Fecha |
|-----|--------|--------|-------|
| [0001](../adr/adr-0001-plataforma-y-target.md) | Plataforma de desarrollo y *target* (Linux primero, Windows después) | aceptado | 2026-06-07 |
| [0002](../adr/adr-0002-modelo-io-asincrona-proactor.md) | Modelo de I/O asíncrona (proactor; io_uring primero, IOCP después) | aceptado | 2026-06-07 |
| [0003](../adr/adr-0003-replicacion-raft-por-particion.md) | Modelo de replicación (Raft por partición) | aceptado | 2026-06-07 |
| [0004](../adr/adr-0004-protocolo-binario-propio-gateway-rest.md) | Protocolo del plano de datos (binario propio + gateway REST) | aceptado | 2026-06-07 |
| [0005](../adr/adr-0005-concurrencia-shared-nothing-thread-per-core.md) | Arquitectura de concurrencia (*shared-nothing thread-per-core*) | aceptado | 2026-06-07 |
| [0006](../adr/adr-0006-ingress-dos-modos.md) | Rol del *ingress* (dos modos: nativo directo + proxy/REST) | aceptado | 2026-06-07 |
| [0007](../adr/adr-0007-consistencia-cp-pacelc.md) | Postura de consistencia (CP / PACELC PC-EC) | aceptado | 2026-06-07 |
| [0008](../adr/adr-0008-coste-cero.md) | Viabilidad de coste cero (desarrollo y test gratuitos) | aceptado | 2026-06-07 |
| [0009](../adr/adr-0009-manejo-errores-por-capa.md) | Política de manejo de errores por capa | aceptado | 2026-06-10 |
| [0010](../adr/adr-0010-entorno-vscode-wsl.md) | Entorno de desarrollo (VS Code sobre WSL) | aceptado | 2026-06-11 |
| [0011](../adr/adr-0011-cpp23-libcxx-clang.md) | Estándar C++23 + libc++ en Clang (para `std::expected`) | aceptado | 2026-06-11 |
| [0012](../adr/adr-0012-io_uring-directo-uapi.md) | Backend io_uring directo sobre el uapi del kernel (sin liburing) | aceptado | 2026-06-12 |
| [0013](../adr/adr-0013-capa-nexus-wire.md) | Capa `nexus-wire` para el framing sobre conexión | aceptado | 2026-06-13 |
| [0014](../adr/adr-0014-modelo-log-raft.md) | Modelo del log de Raft (índice = ordinal de entrada; término en sidecar) | aceptado | 2026-06-14 |
| [0015](../adr/adr-0015-raftnode-fsm-sin-io.md) | RaftNode como máquina de estados síncrona sin E/S | aceptado | 2026-06-14 |
| [0016](../adr/adr-0016-replicated-partition.md) | `ReplicatedPartition` como tipo paralelo a `Partition` | aceptado | 2026-06-15 |
| [0017](../adr/adr-0017-nexus-telemetry.md) | Target `nexus-telemetry` para observabilidad | aceptado | 2026-06-17 |
| [0018](../adr/adr-0018-rest-admin-puerto-adaptador.md) | REST admin por puerto/adaptador (`AdminService` / `AdminApi`) | aceptado | 2026-06-18 |
| [0019](../adr/adr-0019-tls-opcional-openssl-bios.md) | TLS opcional vía OpenSSL con puente de BIOs de memoria | aceptado | 2026-06-19 |
| [0020](../adr/adr-0020-binding-python-abi-c.md) | *Binding* de Python vía ABI C estable (`nexus-ffi` + `ctypes`) | aceptado | 2026-06-20 |
| [0021](../adr/adr-0021-iocp-diseno-diferido.md) | Backend IOCP (Windows) — diseño fijado, implementación diferida | reemplazado por adr-0022 | 2026-06-20 |
| [0022](../adr/adr-0022-iocp-implementado-mingw.md) | Backend IOCP (Windows) — implementado, compile-verificado con MinGW | reemplazado por adr-0023 | 2026-06-20 |
| [0023](../adr/adr-0023-iocp-runtime-msvc.md) | Backend IOCP (Windows) — verificado en runtime con MSVC | aceptado | 2026-06-20 |
| [0024](../adr/adr-0024-compactacion-raft-snapshot.md) | Compactación del log de Raft por snapshot | aceptado | 2026-06-20 |
| [0025](../adr/adr-0025-activacion-raft-multireactor-transporte.md) | Activación en producción del stack Raft + multi-reactor (transporte inter-nodo) | aceptado | 2026-06-21 |
| [0026](../adr/adr-0026-sharding-por-nucleo.md) | Sharding por núcleo del plano de datos (partición→núcleo, grupos por hash) | aceptado | 2026-06-21 |
| [0027](../adr/adr-0027-modo-proxy-upstream-pool.md) | Cableado del modo proxy — *pool* de conexiones aguas arriba por reactor | aceptado | 2026-06-25 |
| [0028](../adr/adr-0028-port-completo-nexusd-windows.md) | Port completo de `nexusd` a Windows (backend, afinidad y señales) | aceptado | 2026-06-27 |
| [0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md) | Adaptador Kafka asíncrono cross-core sobre el broker vivo | aceptado | 2026-06-28 |
| [0030](../adr/adr-0030-particion-mono-protocolo.md) | Partición mono-protocolo — guarda cross-protocol nativo/Kafka | aceptado | 2026-07-09 |
| [0031](../adr/adr-0031-cifrado-en-reposo-aes-gcm.md) | Cifrado en reposo del log con AES-256-GCM y framing AEAD por bloque | aceptado | 2026-07-12 |
| [0032](../adr/adr-0032-tiered-storage-puerto-y-tier-local.md) | Almacenamiento por niveles — puerto `StorageTier` y tier local | aceptado | 2026-07-12 |
| [0033](../adr/adr-0033-exactly-once-nativo-transacciones.md) | Exactly-once multi-partición nativo (transacciones, coordinador y `read_committed`) | aceptado | 2026-07-12 |
| [0034](../adr/adr-0034-2pc-logueado-recuperable.md) | 2PC logueado y recuperable (reconciliación con la prohibición del 2PC bloqueante) | aceptado | 2026-07-12 |

## 28.4 ADR agrupados por tema

Los 38 ADR cubren diez áreas. Cada grupo se resume en una frase; los números enlazan al fichero correspondiente en [`../adr/`](../adr/).

### Plataforma e I/O

[0001](../adr/adr-0001-plataforma-y-target.md), [0002](../adr/adr-0002-modelo-io-asincrona-proactor.md), [0008](../adr/adr-0008-coste-cero.md), [0010](../adr/adr-0010-entorno-vscode-wsl.md), [0011](../adr/adr-0011-cpp23-libcxx-clang.md), [0012](../adr/adr-0012-io_uring-directo-uapi.md), [0013](../adr/adr-0013-capa-nexus-wire.md).

Fijan el terreno: Linux primero y Windows después, modelo **proactor** sobre *completions*, C++23 con libc++ en Clang (que `std::expected` exige), `io_uring` directo sobre el uapi del kernel sin `liburing`, y la capa `nexus-wire` que aloja el framing sobre conexión, todo desarrollable y comprobable a coste cero.

### Replicación y consenso

[0003](../adr/adr-0003-replicacion-raft-por-particion.md), [0007](../adr/adr-0007-consistencia-cp-pacelc.md), [0014](../adr/adr-0014-modelo-log-raft.md), [0015](../adr/adr-0015-raftnode-fsm-sin-io.md), [0016](../adr/adr-0016-replicated-partition.md), [0024](../adr/adr-0024-compactacion-raft-snapshot.md), [0025](../adr/adr-0025-activacion-raft-multireactor-transporte.md).

Definen el corazón distribuido: **Raft por partición** con postura **CP** (PACELC PC/EC), el log de Raft como vista `(term, índice)` sobre el `PartitionLog`, una FSM síncrona **sin E/S** que el portador conduce, `ReplicatedPartition` paralelo a `Partition`, la compactación por *snapshot* y la activación del stack con transporte inter-nodo real.

### Protocolo

[0004](../adr/adr-0004-protocolo-binario-propio-gateway-rest.md), [0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md), [0030](../adr/adr-0030-particion-mono-protocolo.md).

Establecen el plano de datos: un **protocolo binario propio** (con *gateway* REST para administración) y, como *stretch*, un **subconjunto Kafka** servido por un adaptador asíncrono que interopera en vivo con `kcat`, con una **guarda mono-protocolo** que impide mezclar records nativos y Kafka en una misma partición.

### Concurrencia

[0005](../adr/adr-0005-concurrencia-shared-nothing-thread-per-core.md), [0026](../adr/adr-0026-sharding-por-nucleo.md).

La tesis arquitectónica: **shared-nothing thread-per-core** (un reactor *pinned* por núcleo) con **sharding** del plano de datos (partición→núcleo por `p % N`, grupos por `hash(id) % N`) para evitar la contención.

### Ingress y seguridad

[0006](../adr/adr-0006-ingress-dos-modos.md), [0018](../adr/adr-0018-rest-admin-puerto-adaptador.md), [0019](../adr/adr-0019-tls-opcional-openssl-bios.md), [0027](../adr/adr-0027-modo-proxy-upstream-pool.md), [0031](../adr/adr-0031-cifrado-en-reposo-aes-gcm.md), [0035](../adr/adr-0035-estado-cluster-raft-rest-admin.md), [0037](../adr/adr-0037-config-topic-mutable-cross-core.md).

El borde del sistema y la protección de datos: *ingress* en dos modos (nativo directo y proxy *opt-in*), REST admin desacoplado por **puerto/adaptador** (DIP), TLS/mTLS opcional con puente de BIOs de memoria sobre el proactor, el *pool* de conexiones aguas arriba por reactor que cablea el modo proxy, y el **cifrado en reposo** del log (AES-256-GCM opcional con DEK por segmento y framing AEAD por bloque), que reutiliza la misma dependencia OpenSSL que TLS. Sobre esa superficie REST se enriquece el **backend de la consola web**: el **estado de clúster/Raft** por partición (`GET /api/v1/cluster`) agregado cross-core sin filtrar los tipos internos, y la **config de *topic* mutable en caliente** (`PATCH`) publicada a todos los núcleos —retención mutable, `segment.bytes` de solo-creación—.

### Almacenamiento

[0032](../adr/adr-0032-tiered-storage-puerto-y-tier-local.md), [0036](../adr/adr-0036-aplicacion-retencion-runtime.md).

La retención larga a bajo coste: el **almacenamiento por niveles** (*tiered storage*) descarga los segmentos sellados fríos a un puerto `StorageTier` (con adaptador local por defecto e interfaz orientada a fichero, lista para un adaptador S3 futuro), reclama el disco local **solo tras confirmar** la subida y **rehidrata** de forma transparente al leer un offset frío, interoperando con el cifrado en reposo (sube el *ciphertext* tal cual). Y la **aplicación de la retención en runtime**: un barrido periódico por núcleo (temporizador holgado en cada reactor) que reclama segmentos sellados enteros de las particiones no replicadas leyendo la config **actual** del *topic* —antes la política existía pero nadie la disparaba—.

### Transacciones

[0033](../adr/adr-0033-exactly-once-nativo-transacciones.md), [0034](../adr/adr-0034-2pc-logueado-recuperable.md).

El **exactly-once multi-partición** (nativo, *effectively-once* honesto): marcadores de control COMMIT/ABORT, un **coordinador de transacciones** como FSM sin E/S con su propio grupo Raft, el **LSO** por partición y el aislamiento `read_committed` (con filtrado de abortados), y el *fencing* por época de productores y coordinadores. El 2PC que lo sostiene es **logueado y recuperable** (la decisión se registra en Raft antes de escribir marcadores; un failover la re-conduce), reconciliando la atomicidad multi-partición con la prohibición del 2PC en memoria bloqueante.

### Observabilidad

[0017](../adr/adr-0017-nexus-telemetry.md), [0038](../adr/adr-0038-streaming-sse-admin-http.md).

Un target dedicado `nexus-telemetry` que concentra métricas Prometheus y logging estructurado, evitando esparcir la instrumentación por todas las capas. Para la consola en tiempo real se suma un **modelo de streaming SSE** en el servidor HTTP admin: un camino de respuesta hermano del *buffered* que emite el snapshot de métricas por `text/event-stream` (sin `Content-Length`, con cadencia) y observa la señal de drenaje para cerrar limpio en el apagado.

### Windows

[0021](../adr/adr-0021-iocp-diseno-diferido.md) → [0022](../adr/adr-0022-iocp-implementado-mingw.md) → [0023](../adr/adr-0023-iocp-runtime-msvc.md), [0028](../adr/adr-0028-port-completo-nexusd-windows.md).

La portabilidad a Windows: la **cadena de reemplazo** del backend IOCP (diseño → MinGW → runtime con MSVC) y, sobre ella, el port completo de `nexusd` (backend, afinidad de hilos y señales).

### Lenguajes y errores

[0009](../adr/adr-0009-manejo-errores-por-capa.md), [0020](../adr/adr-0020-binding-python-abi-c.md).

Dos decisiones transversales: la **política de errores por capa** (`expected` en el hot-path, excepciones en el plano de control, códigos de wire traducidos en el borde) y el *binding* de Python vía una **ABI C estable** consumida con `ctypes`.

## 28.5 Dónde está cada decisión

El registro vivo y completo es el directorio [`../adr/`](../adr/), con su propio [README](../adr/README.md) e índice. Este capítulo lo resume y lo contextualiza; para leer una decisión entera —contexto, consecuencias y alternativas— se va al fichero correspondiente.
