# 4. Glosario

> Define los términos de dominio que el resto de la documentación usa sin más aclaración.
> Identificadores y términos técnicos asentados van en inglés (es la convención del
> ecosistema); la definición, en español.

## Modelo de log y mensajería

- **Topic.** Flujo lógico de mensajes, dividido en una o más **particiones**.
- **Partición.** Unidad de paralelismo y de replicación: un **log append-only** ordenado
  e inmutable. En NexusMQ, además, **un grupo Raft** y el dueño de un núcleo concreto.
- **Offset.** Identificador monótono de un registro dentro de una partición; da el orden
  (no el *timestamp*).
- **Record / RecordBatch.** Un registro es la unidad de mensaje (clave, valor, cabeceras,
  *timestamp*); el **RecordBatch** (formato v2, estilo Kafka) agrupa varios registros con
  un *checksum* y metadatos comunes. Ver [contrato](../protocol.md).
- **Segmento.** Tramo físico del log en disco: un fichero `.log` (registros) + un `.index`
  (índice disperso offset→posición). El log de una partición es una secuencia de segmentos.
- **Índice disperso.** Índice que mapea algunos offsets a posiciones de fichero (no todos),
  para acotar su tamaño; la búsqueda fina se completa con un escaneo corto.
- **WAL (Write-Ahead Log).** Log de escritura previa que garantiza durabilidad. En NexusMQ
  el **log de Raft *es* el WAL** de la partición.
- **High-watermark (HWM).** Offset hasta el cual los datos están *committed* (replicados por
  el quórum) y, por tanto, visibles a los consumidores. Se deriva del `commitIndex` de Raft.
- **acks.** Política de confirmación de una escritura: `acks=0` (sin esperar,
  *at-most-once*), `acks=1` (escrito en el líder), `acks=quorum` (replicado y *committed*
  por la mayoría del grupo Raft; valor por defecto).
- **Grupo de consumidores.** Conjunto de consumidores que se reparten las particiones de un
  *topic* con **rebalanceo**, para escalar el consumo.
- **DLQ (Dead Letter Queue).** Destino de los mensajes no procesables tras N intentos.
- **Productor idempotente.** Productor con *producer-id* + *sequence* que evita duplicados
  por reintento (*effectively-once* por partición).
- **Retención / compactación.** Borrado de datos antiguos por tiempo/tamaño (retención) o
  conservación del último valor por clave (compactación).

## Consenso y distribución

- **Raft.** Algoritmo de consenso (Ongaro & Ousterhout) para replicar un log de forma
  tolerante a fallos con un líder elegido por mayoría.
- **commitIndex.** Índice del log de Raft hasta el cual las entradas están *committed*.
- **Quórum.** Mayoría de un grupo Raft (⌊n/2⌋+1), necesaria para *commit* y para elegir líder.
- **Leader epoch.** Número que identifica el mandato de un líder; detecta y descarta líderes
  obsoletos.
- **Snapshot (Raft).** Imagen/base compactada del estado que permite truncar el prefijo del
  log replicado y acotar su tamaño (ver [ADR-0024](../adr/adr-0024-compactacion-raft-snapshot.md)).
- **PACELC.** Marco que extiende CAP: ante **P**artición se elige **A** o **C**; en
  operación normal (**E**lse), **L**atencia o **C**onsistencia. NexusMQ es **PC/EC**.
- **Failover.** Sustitución automática de un líder caído por un nuevo líder elegido.

## Concurrencia, memoria y sistemas

- **Shared-nothing.** Diseño sin estado mutable compartido entre unidades de ejecución; se
  comunican por **paso de mensajes**.
- **Thread-per-core.** Un hilo (reactor) por núcleo físico, *pinned* a su CPU.
- **Reactor / Proactor.** Modelos de I/O asíncrona: *reactor* basado en *readiness* (epoll);
  *proactor* basado en *completions* (io_uring/IOCP). NexusMQ usa **proactor**.
- **io_uring / IOCP.** Interfaces de I/O por *completions* de Linux y Windows. NexusMQ usa
  io_uring **directo sobre el uapi del kernel**, sin liburing
  ([ADR-0012](../adr/adr-0012-io_uring-directo-uapi.md)).
- **Cross-core message passing.** Comunicación entre reactores por colas **SPSC** con
  despertar del destino (estilo `submit_to`).
- **SPSC.** *Single-producer single-consumer*: cola con un único productor y un único
  consumidor (caso simple y rápido).
- **Backpressure (por créditos).** Control de flujo en el que el receptor concede *créditos*;
  sin créditos, el emisor se frena (colas acotadas).
- **Afinidad (*pinning*).** Fijar un hilo a un núcleo concreto para localidad de caché y NUMA.
- **NUMA.** *Non-Uniform Memory Access*: la memoria local al nodo del núcleo es más rápida.
- **False sharing.** Degradación por compartir una línea de caché entre hilos que escriben;
  se mitiga con `alignas(std::hardware_destructive_interference_size)`.
- **CRC32C.** Variante de CRC de 32 bits con instrucción hardware (SSE4.2), usada como *checksum*.
- **Direct I/O.** I/O que evita el *page cache* del SO (`O_DIRECT`), gestionando
  caché/alineación propias.
- **Reloj monotónico.** Reloj que nunca retrocede; se usa para *timeouts*/heartbeats/elección
  Raft (nunca el *wall-clock*).

## Modelo de errores y observabilidad

- **`expected<T>`.** `std::expected<T, Error>` (C++23): resultado-o-error como valor, usado
  en el núcleo/hot-path ([ADR-0009](../adr/adr-0009-manejo-errores-por-capa.md)).
- **errorCode (wire).** Código de error numérico (`i16`) del protocolo; es **contrato**, se
  traduce al modelo interno en el borde.
- **RFC 7807 (ProblemDetail).** Formato estándar de error para la API REST de administración.
- **Liveness / Readiness.** Estar vivo (el proceso responde) vs estar listo para tráfico
  (*checks* de disco/Raft/*lag*).
- **Coordinated omission.** Sesgo de medición de latencia de los clientes *closed-loop*; se
  evita con un generador *open-loop* (ver [capítulo 23](./23-rendimiento-y-benchmarks.md)).

## Otros patrones

- **Circuit breaker.** Patrón que "abre" el circuito ante fallos para no insistir contra un
  destino caído.
- **Token bucket.** Algoritmo de *rate limiting* basado en una cubeta de *tokens* que se
  rellena a tasa fija.
- **Decompression bomb.** *Batch* comprimido que se expande desmesuradamente al descomprimir;
  se mitiga con límites de ratio/tamaño.
