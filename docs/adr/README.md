# Registro de decisiones de arquitectura (ADR)

Cada decisión de arquitectura de NexusMQ vive en un fichero `adr-NNNN-titulo-corto.md`,
con las secciones **Contexto · Decisión · Consecuencias · Alternativas consideradas**
(plantilla en `../../BibliotecaDocumentacion/patrones/plantilla-adr.md`).

> **Una decisión `aceptado` no se edita:** si cambia, se **reemplaza** por un ADR nuevo
> que la marca como *reemplazada por adr-NNNN*. La historia de decisiones es inmutable.
> El recorrido narrado de estas decisiones vive en el capítulo
> [28 de la documentación técnica](../tecnica/28-registro-de-decisiones-adr.md).

| ADR | Título | Estado | Fecha |
|-----|--------|--------|-------|
| [0001](adr-0001-plataforma-y-target.md) | Plataforma de desarrollo y *target* (Linux primero, Windows después) | aceptado | 2026-06-07 |
| [0002](adr-0002-modelo-io-asincrona-proactor.md) | Modelo de I/O asíncrona (proactor; io_uring primero, IOCP después) | aceptado | 2026-06-07 |
| [0003](adr-0003-replicacion-raft-por-particion.md) | Modelo de replicación (Raft por partición) | aceptado | 2026-06-07 |
| [0004](adr-0004-protocolo-binario-propio-gateway-rest.md) | Protocolo del plano de datos (binario propio + gateway REST) | aceptado | 2026-06-07 |
| [0005](adr-0005-concurrencia-shared-nothing-thread-per-core.md) | Arquitectura de concurrencia (*shared-nothing thread-per-core*) | aceptado | 2026-06-07 |
| [0006](adr-0006-ingress-dos-modos.md) | Rol del *ingress* (dos modos: nativo directo + proxy/REST) | aceptado | 2026-06-07 |
| [0007](adr-0007-consistencia-cp-pacelc.md) | Postura de consistencia (CP / PACELC PC-EC) | aceptado | 2026-06-07 |
| [0008](adr-0008-coste-cero.md) | Viabilidad de coste cero (desarrollo y test gratuitos) | aceptado | 2026-06-07 |
| [0009](adr-0009-manejo-errores-por-capa.md) | Política de manejo de errores por capa | aceptado | 2026-06-10 |
| [0010](adr-0010-entorno-vscode-wsl.md) | Entorno de desarrollo (VS Code sobre WSL) | aceptado | 2026-06-11 |
| [0011](adr-0011-cpp23-libcxx-clang.md) | Estándar C++23 + libc++ en Clang (para `std::expected`) | aceptado | 2026-06-11 |
| [0012](adr-0012-io_uring-directo-uapi.md) | Backend io_uring directo sobre el uapi del kernel (sin liburing) | aceptado | 2026-06-12 |
| [0013](adr-0013-capa-nexus-wire.md) | Capa `nexus-wire` para el framing sobre conexión | aceptado | 2026-06-13 |
| [0014](adr-0014-modelo-log-raft.md) | Modelo del log de Raft (índice = ordinal de entrada; término en sidecar) | aceptado | 2026-06-14 |
| [0015](adr-0015-raftnode-fsm-sin-io.md) | RaftNode como máquina de estados síncrona sin E/S | aceptado | 2026-06-14 |
| [0016](adr-0016-replicated-partition.md) | `ReplicatedPartition` como tipo paralelo a `Partition` | aceptado | 2026-06-15 |
| [0017](adr-0017-nexus-telemetry.md) | Target `nexus-telemetry` para observabilidad | aceptado | 2026-06-17 |
| [0018](adr-0018-rest-admin-puerto-adaptador.md) | REST admin por puerto/adaptador (`AdminService` / `AdminApi`) | aceptado | 2026-06-18 |
| [0019](adr-0019-tls-opcional-openssl-bios.md) | TLS opcional vía OpenSSL con puente de BIOs de memoria | aceptado | 2026-06-19 |
| [0020](adr-0020-binding-python-abi-c.md) | *Binding* de Python vía ABI C estable (`nexus-ffi` + `ctypes`) | aceptado | 2026-06-20 |
| [0021](adr-0021-iocp-diseno-diferido.md) | Backend IOCP (Windows) — diseño fijado, implementación diferida | reemplazado por adr-0022 | 2026-06-20 |
| [0022](adr-0022-iocp-implementado-mingw.md) | Backend IOCP (Windows) — implementado, compile-verificado con MinGW | reemplazado por adr-0023 | 2026-06-20 |
| [0023](adr-0023-iocp-runtime-msvc.md) | Backend IOCP (Windows) — verificado en runtime con MSVC | aceptado | 2026-06-20 |
| [0024](adr-0024-compactacion-raft-snapshot.md) | Compactación del log de Raft por snapshot | aceptado | 2026-06-20 |
| [0025](adr-0025-activacion-raft-multireactor-transporte.md) | Activación en producción del stack Raft + multi-reactor (transporte inter-nodo) | aceptado | 2026-06-21 |
| [0026](adr-0026-sharding-por-nucleo.md) | Sharding por núcleo del plano de datos (partición→núcleo, grupos por hash) | aceptado | 2026-06-21 |
| [0027](adr-0027-modo-proxy-upstream-pool.md) | Cableado del modo proxy — *pool* de conexiones aguas arriba por reactor | aceptado | 2026-06-25 |
| [0028](adr-0028-port-completo-nexusd-windows.md) | Port completo de `nexusd` a Windows (backend, afinidad y señales) | aceptado | 2026-06-27 |
| [0029](adr-0029-adaptador-kafka-async-cross-core.md) | Adaptador Kafka asíncrono cross-core sobre el broker vivo | aceptado | 2026-06-28 |
| [0030](adr-0030-particion-mono-protocolo.md) | Partición mono-protocolo — guarda cross-protocol nativo/Kafka | aceptado | 2026-07-09 |
| [0031](adr-0031-cifrado-en-reposo-aes-gcm.md) | Cifrado en reposo del log con AES-256-GCM y framing AEAD por bloque | aceptado | 2026-07-12 |
| [0032](adr-0032-tiered-storage-puerto-y-tier-local.md) | Almacenamiento por niveles — puerto `StorageTier` y tier local | aceptado | 2026-07-12 |
| [0033](adr-0033-exactly-once-nativo-transacciones.md) | Exactly-once multi-partición nativo (transacciones, coordinador y `read_committed`) | aceptado | 2026-07-12 |
| [0034](adr-0034-2pc-logueado-recuperable.md) | 2PC logueado y recuperable (reconciliación con la prohibición del 2PC bloqueante) | aceptado | 2026-07-12 |
| [0035](adr-0035-estado-cluster-raft-rest-admin.md) | Estado de clúster/Raft por la superficie REST admin (`GET /api/v1/cluster`) | aceptado | 2026-07-13 |
| [0036](adr-0036-aplicacion-retencion-runtime.md) | Aplicación de la retención en runtime (barrido periódico por núcleo) | aceptado | 2026-07-13 |
| [0037](adr-0037-config-topic-mutable-cross-core.md) | Config de topic mutable en caliente y publicada cross-core (`PATCH /api/v1/topics/{name}`) | aceptado | 2026-07-13 |
