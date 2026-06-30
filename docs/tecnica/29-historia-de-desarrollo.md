# 29. Historia de desarrollo

> Retrospectiva del proyecto por fases. NexusMQ se construyó **por capas**, cada una
> autocontenida y demoable, registrando las decisiones de arquitectura como ADR a medida
> que surgían. Aquí se resume el recorrido y los aprendizajes; el catálogo de decisiones
> vive en [`docs/adr/`](../adr/) y el capítulo [28](./28-registro-de-decisiones-adr.md).

## 29.1 Filosofía de avance

El principio rector fue **incrementos pequeños y siempre compilables**, con TDD
(rojo→verde→refactor) y una puerta de calidad en local antes de cada *commit* (dos
compiladores, sanitizers, formato, `clang-tidy`). La ambición se persiguió como
**profundidad dentro de una sola tesis arquitectónica** (*thread-per-core* + Raft por
partición), no como amplitud. Cada decisión de calado quedó como ADR; un ADR aceptado no
se edita, se reemplaza (ver [capítulo 28](./28-registro-de-decisiones-adr.md)).

## 29.2 Fase 1 — Storage engine

Motor de log monopartición con **I/O bloqueante y sin reactor** (decisión deliberada:
validar correctitud y durabilidad antes que rendimiento). Hitos: `Record` + **CRC32C**
por hardware con *fallback* software; `Segment` (`.log` + `.index`); `PartitionLog`
(secuencia de segmentos, *rolling*, recuperación con validación de CRC y truncado de la
cola *torn*); política de `fsync`; retención por tiempo/tamaño; primeros *benchmarks*.

**Aprendizajes:** la recuperación ante *crash* y la corrupción silenciosa (*torn writes*)
exigen *checksums* por registro verificados al leer; la durabilidad real depende de
`fsync`/`fdatasync`, no de `write`.

## 29.3 Fase 1b — Reactor + broker monolítico

Aparece el **reactor *thread-per-core* propio** sobre el proactor (io_uring + corutinas
C++23), el **protocolo binario** (capa `nexus-wire` para el framing —
[ADR-0013](../adr/adr-0013-capa-nexus-wire.md)), el cliente nativo y los flujos
*produce*/*fetch* con **backpressure por créditos**. Aquí se consolidan dos decisiones
mayores: **C++23 + libc++ en Clang** por `std::expected`
([ADR-0011](../adr/adr-0011-cpp23-libcxx-clang.md)) y **io_uring directo sobre el uapi**,
sin liburing ([ADR-0012](../adr/adr-0012-io_uring-directo-uapi.md)).

**Aprendizajes:** bajo *thread-per-core*, **nada bloqueante puede vivir en el reactor**
(un `fsync` o un `malloc` que entre al kernel congela el núcleo) → todo I/O pasa por el
proactor y se usan *allocators* por núcleo.

## 29.4 Fase 2 — Distribución (Raft por partición)

Replicación y consenso con **Raft por partición**. Decisiones clave: el **log de Raft *es*
el WAL**, con el **índice de entrada** como espacio distinto del **offset por record**
([ADR-0014](../adr/adr-0014-modelo-log-raft.md)); `RaftNode` como **máquina de estados
síncrona sin E/S** —entradas `tick`/`on_*` con `now` inyectado→ cola de mensajes— que
habilita la **simulación determinista** ([ADR-0015](../adr/adr-0015-raftnode-fsm-sin-io.md));
`ReplicatedPartition` como tipo paralelo a `Partition`
([ADR-0016](../adr/adr-0016-replicated-partition.md)); grupos de consumidores y rebalanceo.

**Aprendizajes:** separar el **núcleo de consenso de la E/S** es lo que hace Raft
*testeable* de forma determinista (reloj/red virtuales), cumpliendo FIRST sin *flakiness*.

## 29.5 Fase 3 — Ingress y operación

Plataforma operable y observable: *ingress* en **dos modos** (nativo/proxy —
[ADR-0006](../adr/adr-0006-ingress-dos-modos.md)), **TLS opcional** vía OpenSSL con puente
de BIOs de memoria sobre el proactor ([ADR-0019](../adr/adr-0019-tls-opcional-openssl-bios.md)),
**API REST de administración** por puerto/adaptador
([ADR-0018](../adr/adr-0018-rest-admin-puerto-adaptador.md)), CLI, y observabilidad con el
target **`nexus-telemetry`** ([ADR-0017](../adr/adr-0017-nexus-telemetry.md)): métricas
Prometheus, logs JSON, *health/readiness*.

**Aprendizajes:** un puente **TLS síncrono sobre I/O asíncrona** se resuelve con *memory
BIOs* (OpenSSL procesa en memoria; el socket asíncrono mueve los bytes cifrados), evitando
acoplar la librería de TLS al modelo de *completions*.

## 29.6 Fase 4 — *Stretch* y producción

Activación del stack distribuido y funcionalidades avanzadas: **transporte inter-nodo
real** con plano separado y portador por partición
([ADR-0025](../adr/adr-0025-activacion-raft-multireactor-transporte.md)); **compactación por
snapshot** del log de Raft ([ADR-0024](../adr/adr-0024-compactacion-raft-snapshot.md));
**sharding por núcleo** del plano de datos
([ADR-0026](../adr/adr-0026-sharding-por-nucleo.md)); **modo proxy** con *upstream pool* por
reactor ([ADR-0027](../adr/adr-0027-modo-proxy-upstream-pool.md)); **subconjunto
Kafka-compatible** asíncrono cross-core con interop `kcat`
([ADR-0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md)); *binding* de Python por
ABI C ([ADR-0020](../adr/adr-0020-binding-python-abi-c.md)); y el **port a Windows**
(IOCP), que recorrió tres estados —diseño, compile-verificado con MinGW y, finalmente,
**verificado en runtime con MSVC**— hasta el **port completo de `nexusd`**
([ADR-0021](../adr/adr-0021-iocp-diseno-diferido.md) →
[0022](../adr/adr-0022-iocp-implementado-mingw.md) →
[0023](../adr/adr-0023-iocp-runtime-msvc.md);
[ADR-0028](../adr/adr-0028-port-completo-nexusd-windows.md)).

**Aprendizajes:** "compila" ≠ "funciona"; el valor real del port a Windows llegó al
**verificarlo en runtime**. El único arreglo de fondo fue portar `crc32c.cpp` a MSVC
(`__cpuid` + intrínsecos SSE4.2), lo que evidencia que las *CPU features* se detectan y
usan distinto según el compilador.

## 29.7 Estado actual

Fases 1→4 implementadas; el árbol compila y pasa la suite con GCC/libstdc++ y Clang/libc++,
y el backend Windows está verificado en runtime con MSVC. Las **GitHub Actions** están
desactivadas temporalmente (cuota) y se reactivan al publicar; la puerta de calidad se
mantiene en local. La documentación se está consolidando en su forma final (esta misma).
