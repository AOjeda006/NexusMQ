# Protocolo binario de NexusMQ (plano de datos)

Contrato del **plano de datos** de NexusMQ: el protocolo binario propio sobre TCP que hablan los
clientes nativos con el broker (puerto `9092` por defecto). El **plano de administración** es
aparte (REST sobre el puerto de operación; ver [`openapi.yaml`](openapi.yaml)).

> NexusMQ expone **además** un **listener compatible con el protocolo de Apache Kafka** (un
> subconjunto: `Produce`/`Fetch`/`Metadata`/`ListOffsets`/`ApiVersions`) en `--kafka-port`, para
> interoperar con clientes del ecosistema (`kcat`, librdkafka). Ese contrato —big-endian, *framing*
> `Size:INT32`, clásico vs flexible— se documenta aparte en [`kafka.md`](kafka.md) (ADR-0029). Este
> documento describe el **protocolo nativo**.

- **Transporte:** TCP. Una conexión multiplexa muchas peticiones por `correlation_id`.
- **Modelo:** petición/respuesta. El cliente envía una trama de petición; el broker responde con
  una trama cuyo `correlation_id` **espeja** el de la petición.
- **Endianness:** **little-endian** para todos los enteros.
- **Codificación:** los enteros son de ancho fijo; cadenas y bytes van longitud-prefijo; los enteros
  variables usan **varint**. La fuente de verdad del codec es [`src/protocol/codec.hpp`](../src/protocol/codec.hpp)
  y los mensajes [`src/protocol/messages.hpp`](../src/protocol/messages.hpp).

## Trama (framing)

Toda trama es **longitud-prefijo**: una cabecera de 14 bytes seguida del payload.

```
 0               4       6       8                 12      14
 +---------------+-------+-------+-----------------+-------+===============+
 |   length:u32  |apiKey |apiVer | correlationId   | flags |    payload    |
 |               | :u16  | :u16  |     :u32        | :u16  |   (length-10) |
 +---------------+-------+-------+-----------------+-------+===============+
```

| Campo            | Tipo  | Descripción                                                        |
| ---------------- | ----- | ------------------------------------------------------------------ |
| `length`         | u32   | Bytes **tras** este campo (resto de cabecera + payload) = 10 + payload. |
| `apiKey`         | u16   | Operación (ver tabla de ApiKeys).                                  |
| `apiVersion`     | u16   | Versión del esquema de esa operación (negociada, ver más abajo).   |
| `correlationId`  | u32   | Eco petición↔respuesta; lo elige el cliente.                       |
| `flags`          | u16   | Bits de control (ver abajo).                                       |
| `payload`        | bytes | Cuerpo específico de la operación (`length - 10` bytes).            |

**Lectura:** lee 4 bytes (`length`), luego lee exactamente `length` bytes más. La cabecera
codificada ocupa 14 bytes (`kEncodedSize`); el lector valida una cota inferior (debe caber el
resto de cabecera) y una **cota superior** anti-DoS (`max_frame`, por defecto **16 MiB**).

### `flags`

| Bit      | Nombre              | Significado                                            |
| -------- | ------------------- | ------------------------------------------------------ |
| `0x0001` | credit update       | La trama acarrea una actualización de créditos (control de flujo / backpressure). |

## ApiKeys

| Valor | ApiKey          | Descripción                                          |
| ----- | --------------- | ---------------------------------------------------- |
| 0     | `ApiVersions`   | Negociación de versiones soportadas.                 |
| 1     | `Metadata`      | Brokers del clúster y topics/particiones.            |
| 2     | `Produce`       | Publica `RecordBatch` en una partición.              |
| 3     | `Fetch`         | Lee registros desde un offset.                       |
| 4     | `OffsetCommit`  | Confirma el offset consumido de un grupo.            |
| 5     | `OffsetFetch`   | Recupera el offset confirmado de un grupo.           |
| 6     | `JoinGroup`     | Une un miembro a un grupo de consumidores.           |
| 7     | `SyncGroup`     | Distribuye la asignación de particiones del grupo.   |
| 8     | `Heartbeat`     | Mantiene viva la pertenencia al grupo.               |
| 9     | `LeaveGroup`    | Abandona el grupo.                                    |
| 10    | `CreateTopic`   | Crea un topic.                                        |
| 11    | `DeleteTopic`   | Borra un topic.                                       |

## Negociación de versiones

El cliente abre con `ApiVersions` anunciando el máximo que soporta por `ApiKey`. El servidor
publica un rango `[min, max]` por `ApiKey`; la versión efectiva es la **mayor que ambos
soportan**: `min(client_max, server.max)` si alcanza `server.min`, o `0` si la `ApiKey` no es
negociable / no hay solape. A partir de ahí, cada trama lleva su `apiVersion`. Ver
[`src/protocol/versioning.hpp`](../src/protocol/versioning.hpp).

## Modelo de errores

Las respuestas acarrean un `errorCode:i16` (`WireError`) como contrato externo de errores
(ADR-0009): el núcleo trabaja con `Error`/`ErrorCode` y traduce en el **borde** del protocolo.
`None` (0) = éxito. El cliente reintroduce el código en su modelo interno al recibirlo.

| Código | WireError                 | ¿Reintentable? |
| ------ | ------------------------- | -------------- |
| 0      | `None` (éxito)            | —              |
| 1      | `NotLeaderForPartition`   | sí             |
| 2      | `LeaderNotAvailable`      | sí             |
| 3      | `UnknownTopicOrPartition` | no             |
| 4      | `OffsetOutOfRange`        | no             |
| 5      | `NotEnoughReplicas`       | sí             |
| 6      | `RequestTimedOut`         | sí             |
| 7      | `CorruptMessage`          | no             |
| 8      | `MessageTooLarge`         | no             |
| 9      | `OutOfOrderSequence`      | no             |
| 10     | `DuplicateSequence`       | no             |
| 11     | `Throttled`               | sí             |
| 12     | `RebalanceInProgress`     | sí             |
| 13     | `UnsupportedVersion`      | no             |
| 14     | `Unauthorized`            | no             |
| 15     | `InvalidRequest`          | no             |
| 16     | `InvalidProducerEpoch`    | no             |

La política exacta de reintento (`is_retryable`) y la traducción `WireError`↔`Error` viven en
[`src/protocol/error_code.hpp`](../src/protocol/error_code.hpp).

## Registros y batches

`Produce`/`Fetch` transportan **record batches** estilo Kafka v2: una cabecera de batch con un
**CRC32C** que cubre el contenido, compresión común y muchos registros longitud-prefijo bajo un
offset base. El batch viaja **intacto** por el log y la replicación (es la unidad de escritura y
de entrada de Raft, ADR-0014). El layout campo a campo está en
[`src/common/record.hpp`](../src/common/record.hpp) y [`src/protocol/messages.hpp`](../src/protocol/messages.hpp).

El **blob de records** puede ir **comprimido** (F5): los **2 bits bajos** de `attrs` indican el
códec (`0`=None, `1`=LZ4, `2`=Zstd). El broker trata el blob como **opaco** —lo guarda y replica
comprimido—; solo el cliente lo descomprime al consumir. El bloque comprimido lleva su **tamaño
original** como prefijo (u32 LE) para acotar la descompresión (defensa anti *decompression bomb*).
Ver [`src/common/compression.hpp`](../src/common/compression.hpp).

## Transacciones y marcadores de control (exactly-once nativo)

El *exactly-once* multi-partición (ADR-0033 / ADR-0034) se apoya en dos bits altos de `attrs`
—**disjuntos** del códec de compresión (bits bajos)— y en un **batch de control** que el
coordinador de transacciones escribe en cada partición participante para cerrar la transacción.
El formato de estos elementos es contrato *as-built*; la fuente es
[`src/common/control_record.hpp`](../src/common/control_record.hpp).

### Flags transaccionales de `attrs`

| Bit      | Nombre          | Significado                                                       |
| -------- | --------------- | ---------------------------------------------------------------- |
| `0x0010` | transactional   | El batch pertenece a una transacción abierta; su visibilidad depende de su marcador. |
| `0x0020` | control         | Batch de **control**: no lleva datos de usuario, sino un único marcador COMMIT/ABORT. |

Un batch transaccional de datos arrastra además `producer_id` + época en la cabecera del batch,
reutilizando la idempotencia de `ProducerSession` (dedup por secuencia + *fencing* por época).

### Batch de control (marcador de fin de transacción)

Un batch de control lleva `record_count = 1`, sin comprimir, con los bits `control`+`transactional`
fijados. Su único record codifica un `EndTxnMarker` en clave/valor:

| Parte  | Layout                                | Tamaño  |
| ------ | ------------------------------------- | ------- |
| clave  | `version:i16 \| type:i16`             | 4 bytes |
| valor  | `version:i16 \| coordinator_epoch:i32`| 6 bytes |

- **`type`**: `0` = `Abort`, `1` = `Commit` (convención de wire compatible con Kafka).
- **`coordinator_epoch`**: época de liderazgo del coordinador emisor; la partición **descarta**
  marcadores de un coordinador obsoleto (*fencing* en el failover).
- **`version`**: `0` (reservado para evolución del formato); un decodificador defensivo devuelve
  `Corrupt` ante tamaños/tipo inválidos y `Unsupported` ante una versión desconocida.

### Aislamiento de lectura y LSO

En lectura, la superficie nativa distingue un **nivel de aislamiento**: `read_uncommitted` (0)
entrega hasta el *high-watermark*, como siempre; `read_committed` (1) entrega solo hasta el **LSO**
(*last stable offset* = mínimo *first-offset* de transacción abierta, acotado por el HWM) y **filtra**
los records abortados y los propios marcadores de control. El LSO y el conjunto de rangos abortados
los mantiene `PartitionTxnIndex` en el broker (§9.8 de la
[documentación técnica](tecnica/09-almacenamiento.md)).

> **Estado *as-built*.** El formato de batch de control, los flags de `attrs`, el filtrado
> `read_committed`/LSO y el `TransactionCoordinator` (2PC logueado y recuperable) están
> implementados y probados por simulación determinista. La **orquestación** (`InitProducerId`,
> `AddPartitionsToTxn`, `EndTxn`, nivel de aislamiento en `Fetch`) vive hoy en la capa de
> coordinador/broker; **no** hay todavía `ApiKey` de wire dedicadas en el enum de
> [`src/protocol/frame.hpp`](../src/protocol/frame.hpp): cablearlas al framing nativo es el
> paso de exposición pendiente. Este documento no inventa valores de `ApiKey` no implementados.

## Seguridad del transporte

El plano de datos puede terminar **TLS 1.3** (y **mTLS** intra-clúster) por delante del framing
(ADR-0019); el protocolo binario es idéntico, cifrado o en claro. Ver
[`src/ingress/tls.hpp`](../src/ingress/tls.hpp).

El **cifrado en reposo** (log en disco, AES-256-GCM, ADR-0031) es un plano **ortogonal** a este
protocolo: actúa sobre el `.log` del segmento, no sobre la trama de wire, así que el batch viaja
por la red exactamente igual esté el log cifrado o en claro. Su formato on-disk (cabecera de
segmento `NXSEG1` + framing AEAD por bloque) se documenta en la §9.6 de la
[documentación técnica](tecnica/09-almacenamiento.md); el layout vive en
[`src/storage/segment_crypto.hpp`](../src/storage/segment_crypto.hpp).
