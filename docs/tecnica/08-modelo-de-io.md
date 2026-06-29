# 8. Modelo de I/O

> Cómo NexusMQ habla con disco y red sin bloquear nunca un núcleo. El plano de datos es, en
> esencia, **I/O de red y disco a gran escala**; el modelo elegido es un **proactor**
> (*completions*) sobre el que se asientan las corutinas C++23.

## 8.1 Proactor, no reactor

Se adopta una abstracción de **proactor** (modelo de *completions*: se envía una operación
asíncrona y se recibe su finalización con el resultado), no un *reactor* epoll
(*readiness*). El motivo es la **portabilidad**: *completions* es la forma común a los dos
backends objetivo, **io_uring** (Linux) e **IOCP** (Windows)
([ADR-0002](../adr/adr-0002-modelo-io-asincrona-proactor.md)). El puerto `Proactor` y los
tipos portables `NativeHandle`/`IoResult` viven en `nexus-io`; los backends concretos son
adaptadores (`io_uring_backend`, `iocp_backend`). La secuencia *submit → completion → reanudar
corutina* está en el [diagrama 7](../diagramas/07-secuencia-proactor.md).

## 8.2 io_uring directo, sin liburing

En Linux, el backend usa **io_uring directamente sobre el uapi del kernel**, **sin
`liburing`** ([ADR-0012](../adr/adr-0012-io_uring-directo-uapi.md)): el proyecto gestiona sus
propios anillos de *submission*/*completion*. Bajo *thread-per-core* hay **un anillo por
núcleo**, sin compartición entre reactores, lo que encaja con el modelo shared-nothing y
amortiza syscalls por *batching* (coherente con "no una syscall por byte"). En Windows, el
backend IOCP se construye sobre `GetQueuedCompletionStatusEx`/`AcceptEx` y está verificado en
runtime con MSVC (ver [capítulo 24](./24-portabilidad.md)).

## 8.3 Corutinas sobre el proactor

Las **corutinas de C++23** se asientan de forma natural sobre el proactor: un `co_await` de
una operación de I/O suspende la corutina y la reanuda en su *completion*, con el resultado.
El código del camino caliente queda **secuencial y legible** sobre I/O por *completions*. Las
operaciones del núcleo devuelven `task<expected<T>>`: combinan la asincronía (`task`) con el
modelo de errores como valor (`expected`, ver
[capítulo 16](./16-modelo-errores-wire-codes.md)). Hay que cuidar el *lifetime* de los
argumentos a través de la suspensión: una corutina puede reanudarse en otro momento, así que
no se hace `co_await` sobre temporales colgantes.

## 8.4 Nada bloqueante en el reactor

La regla de oro: **un solo `fsync`, `malloc` que entre al kernel o *lock* contendido congela
el núcleo entero**. Por eso:

- **Todo** I/O de disco —incluido `fsync`/`fdatasync`— pasa por io_uring asíncrono
  (`IORING_OP_FSYNC`), no por llamadas bloqueantes.
- Se usan ***allocators* por núcleo** (arena/pool) para no entrar al kernel a pedir memoria
  en el camino caliente.
- El control de flujo es por **créditos** (colas acotadas), no por *buffers* ilimitados (ver
  [capítulo 7](./07-concurrencia.md)).

## 8.5 Durabilidad y checksums

- **`write` no persiste.** La durabilidad se fuerza con `fsync`/`fdatasync`; como su coste es
  alto, se **agrupa** (por N mensajes / N ms / por *commit*), nunca por escritura. La política
  de `fsync` es configurable y ortogonal al modelo de `acks`.
- **Checksums.** Cada `RecordBatch` lleva un **CRC32C** que cubre el batch completo; al leer y
  al recuperar tras *crash* se verifica, detectando corrupción silenciosa (*bit rot*, *torn
  writes*) que el WAL por sí solo no ve. CRC32C usa la instrucción hardware de SSE4.2 con
  *fallback* software (ver [capítulo 9](./09-almacenamiento.md)).
- **Recuperación.** Al arrancar se valida el CRC de la cola del log y se **trunca la cola
  *torn*** (escritura a medias por un *crash*), dejando el log en un estado consistente.

## 8.6 Evolución escalonada de la I/O de storage

La I/O de disco se introdujo por fases (decisión deliberada de
[ADR-0002](../adr/adr-0002-modelo-io-asincrona-proactor.md)): la **Fase 1** arrancó con I/O
de fichero **bloqueante** (`pread`/`pwrite` + `fsync`) detrás de la abstracción `File`, para
validar correctitud y durabilidad antes que rendimiento; a partir de la **Fase 1b**, bajo el
reactor, el disco pasó a io_uring asíncrono. ***Direct I/O*** (`O_DIRECT`) con caché y
*readahead* propios quedó como **profundidad opcional** de Fase 4, solo si el *profiling* lo
justifica (ver [capítulo 23](./23-rendimiento-y-benchmarks.md)).
