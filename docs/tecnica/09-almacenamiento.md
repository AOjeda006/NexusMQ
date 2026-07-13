# 9. Almacenamiento

> El *storage engine*: un log particionado append-only sobre segmentos, con índice disperso,
> *checksums* y retención. Es la Fase 1 del proyecto y la base sobre la que se monta Raft. El
> layout se ilustra en el [diagrama 8](../diagramas/08-layout-log.md).

## 9.1 Log particionado

Cada partición es un **log append-only**: una secuencia ordenada e inmutable de registros,
cada uno con un **offset** monótono. Los productores añaden al final; los consumidores leen
desde el offset que elijan y avanzan a su ritmo (el broker no borra al entregar: retiene por
tiempo/tamaño). Una partición es físicamente una **secuencia de segmentos**:

```
Partición 0 ──► [Seg 0 (closed)][Seg 1 (closed)][Seg 2 (active)]   cada Seg: .log + .index
```

`PartitionLog` reúne los segmentos, el segmento activo, `logStartOffset`/`logEndOffset` y el
`recoveryPoint` (último `fsync`). Es **REACTOR-LOCAL**: pertenece al reactor dueño de la
partición y no es *thread-safe* (su invariante es que `logEndOffset` es monótono).

## 9.2 Segmentos e índice disperso

Cada **segmento** (`Segment`) es un par de ficheros: `.log` (los `RecordBatch`) y `.index`
(el índice disperso). El **índice disperso** mapea, cada N bytes, `relativeOffset → posición
en fichero` (`IndexEntry { relativeOffset:u32, filePosition:u32 }`), lo que permite un *seek*
eficiente sin recorrer todo el segmento: se localiza la entrada de índice más cercana por
debajo del offset buscado y se escanea hacia delante un tramo corto.

Ciclo de vida de un segmento: `active` (recibe *appends*) → `closed` (alcanza
`segment.bytes`/`segment.ms`; se sella y se finaliza su `.index`) → `eligible` (supera la
retención) → `deleted`. La retención **nunca** borra el segmento activo.

## 9.3 RecordBatch v2

La unidad de escritura y replicación **no** es el registro suelto, sino el **RecordBatch**
(estilo Kafka v2), que amortiza cabeceras, CRC y compresión sobre muchos registros. Sus
campos (en disco, *little-endian*; *deltas* en `varint`/`zigzag`):

| Campo | Tipo | Descripción |
| ----- | ---- | ----------- |
| `baseOffset` | i64 | Offset del primer registro del batch. |
| `length` | i32 | Longitud del batch. |
| `crc32c` | u32 | Checksum que cubre el batch completo. |
| `attrs` | u16 | Codec de compresión (none/LZ4/Zstd), bits de txn/idempotencia. |
| `producerId` / `producerEpoch` / `baseSequence` | i64/i16/i32 | Productor idempotente. |
| `recordCount` | i32 | Número de registros. |
| `records[]` | — | Cada uno con `offsetDelta`/`timestampDelta` (varint/zigzag), `key`, `value`, `headers`. |

El **offset** lógico de cada registro es `baseOffset + offsetDelta`. El formato exacto es
contrato: ver [`docs/protocol.md`](../protocol.md). La **compresión** LZ4/Zstd es opcional
(se compila solo si `find_package` la encuentra) y se aplica por batch.

## 9.4 Durabilidad y recuperación

- **`fsync` agrupado** (por N mensajes / N ms / por *commit*): durabilidad real sin pagar un
  `fsync` por escritura (ver [capítulo 8](./08-modelo-de-io.md)).
- **CRC32C por batch**, verificado al leer y al recuperar: detecta corrupción silenciosa.
- **Recuperación al arrancar:** se valida el CRC de la cola del log y se **trunca la cola
  *torn*** (escritura a medias por un *crash*), dejando el log consistente y listo en pocos
  segundos. Es la durabilidad que sostiene la garantía `acks=quorum`.

## 9.5 Retención y compactación

- **Retención por tiempo/tamaño** (`retention.ms` / `retention.bytes`): se borran **segmentos
  sellados enteros** cuando se supera el umbral (nunca registros sueltos ni el segmento
  activo). Ver el [diagrama 9](../diagramas/09-retencion-compactacion.md).
- **Compactación por clave** (`LogCompactor`): política alternativa que conserva el último
  valor por clave, útil para *topics* de estado tipo *changelog*.

El storage ofrece **mecanismo** (append, read, fsync); la **política** (retención,
durabilidad, compresión) es configurable y externa (`LogConfig`). Bajo Raft, este mismo log
es el **WAL** de la partición y lo envuelve `RaftLog` (ver
[capítulo 10](./10-replicacion-y-consenso.md)).

## 9.6 Cifrado en reposo (opcional)

El log puede persistirse **cifrado** con **AEAD AES-256-GCM**, de forma opcional y con
**degradación limpia** ([ADR-0031](../adr/adr-0031-cifrado-en-reposo-aes-gcm.md), vía OpenSSL como
en TLS). Si `LogConfig` no lleva clave, el `.log` se escribe **en claro**, byte a byte como hasta
ahora; si la lleva, cada `RecordBatch` (unidad de escritura, §9.3) se cifra como **un bloque**
independiente —nunca por registro—. El cifrado vive confinado en `segment_crypto` y en el framing
de `Segment`; el resto del motor (índice, retención, recuperación, Raft) no cambia.

- **Jerarquía de claves.** La **KEK** (clave maestra de 256 bits) llega por entorno/config y jamás
  se persiste; deriva por **HKDF-SHA256(KEK, salt aleatoria por segmento)** una **DEK** distinta por
  segmento. Acotar cada clave a un segmento aísla el radio de daño y mantiene el número de cifrados
  por clave muy por debajo del límite de colisión de nonces.
- **Formato on-disk.** El `.log` cifrado empieza por una **cabecera de segmento** (32 bytes: magic
  `NXSEG1` + identificadores de KDF/cipher + *flags* + *salt*), que permite **autodetectar**
  cifrado vs. claro al abrir (logs mixtos admitidos). Cada bloque es
  `version | flags | base_offset | record_count | ct_len | nonce(12) | tag(16) | ciphertext`. Los
  metadatos de traversal (`base_offset`/`record_count`/`ct_len`) viajan **en claro pero
  autenticados** (AAD del GCM): el log se **recorre y localiza por offset sin descifrar** (el índice
  disperso y la lectura por offset siguen funcionando); solo las claves/valores de los registros van
  en el ciphertext.
- **Nonce e integridad.** Cada bloque toma un **nonce aleatorio de 96 bits** —robusto ante el ciclo
  truncar→re-append del WAL de Raft (§9.4, [capítulo 10](./10-replicacion-y-consenso.md)); la
  reutilización de nonce con la misma clave es la **invariante nº 1**—. El **tag GCM** autentica
  cada bloque: cualquier byte alterado (ciphertext o metadatos) produce un fallo **autenticado**
  (`Corrupt`), nunca datos corruptos silenciosos. El CRC32C del batch se conserva **dentro** del
  plaintext como defensa en profundidad. Configuración y operación de la KEK: ver
  [capítulo 26](./26-configuracion-y-operacion.md) y [capítulo 27](./27-seguridad.md).

## 9.7 Almacenamiento por niveles (opcional)

Para retención larga a bajo coste, el log puede **descargar** sus segmentos sellados fríos a un
almacén de objetos barato y **rehidratarlos de forma transparente** al leerlos —el patrón *tiered
storage* de Kafka/Pulsar—, de forma opcional y con **degradación limpia**
([ADR-0032](../adr/adr-0032-tiered-storage-puerto-y-tier-local.md)). Sin `--tier-dir`, el prefijo
frío está siempre vacío y el comportamiento es byte-idéntico al de hoy. El diseño se ilustra en el
[diagrama 24](../diagramas/24-tiered-storage.md).

- **Puerto orientado a fichero.** El motor depende de un puerto `StorageTier` (DIP), no de una nube
  concreta: `put_file` / `fetch_file` / `contains` / `object_size` / `remove` / `list_segment_bases`
  sobre una **clave de objeto** determinista `TierObjectKey` (`<topic>/<partition>/<base:020>.<ext>`).
  Un segmento **es** un fichero, así que descargar y rehidratar son copias, sin cargar segmentos en
  RAM. El adaptador por defecto `LocalStorageTier` copia a un **directorio objeto** local (coste
  cero); un adaptador S3 futuro implementaría el mismo contrato tras `find_package`.
- **Descargar sellados, reclamar solo tras confirmar.** `offload_sealed_to_tier` sube los segmentos
  **sellados** (nunca el activo), del más antiguo al más nuevo, y **solo tras confirmar** la subida
  (atómica e idempotente) borra los ficheros locales y marca el segmento como frío. Un fallo del
  tier deja el segmento local y se reintenta; nunca se pierde un dato por reclamación prematura. Con
  un tier configurado, cada **rotación** de segmento dispara la descarga (best-effort).
- **El tier es la autoridad; sin manifiesto.** Al reabrir, el `PartitionLog` reconstruye su **prefijo
  frío** listando el tier (`list_segment_bases`), sin un fichero manifiesto que pudiera
  desincronizarse. Las bases que también existen localmente (offload confirmado pero sin reclamar por
  un *crash*) se descartan: el segmento local es autoritativo. La **contigüidad** del log (sin huecos)
  permite reconstruir el rango de cada segmento frío solo con las bases ordenadas.
- **Lectura transparente e interoperación.** `read` sirve el *suffix* caliente localmente y, para un
  offset del prefijo frío, **rehidrata** el segmento (baja `.log` + `.index` a un temporal, lo abre
  —descifrándolo si procede, §9.6— y sirve el fragmento) con limpieza **RAII**. El rango del log
  (`logStart`/`logEnd`) **no cambia** al descargar. El tier guarda el **ciphertext tal cual** (bytes
  opacos): la clave nunca sale del broker. Las operaciones de Raft son *tier-conscientes* —el
  truncado de compactación/snapshot borra del tier los segmentos que descarta, el truncado normal
  rechaza reescribir dentro del prefijo frío (historia comprometida) y la retención no toca el *hot
  set* mientras haya prefijo frío—. Configuración: ver
  [capítulo 26](./26-configuracion-y-operacion.md).

## 9.8 Visibilidad transaccional (opcional)

El log soporta **transacciones multi-partición** con visibilidad atómica, de forma **opt-in** y con
degradación limpia ([ADR-0033](../adr/adr-0033-exactly-once-nativo-transacciones.md)). Sin usar la
API transaccional, nada cambia. La coordinación (2PC recuperable) vive en el broker (ver
[capítulo 10](./10-replicacion-y-consenso.md)); lo que toca al **almacenamiento** es cómo se marcan
las transacciones en el log y cómo se acota la lectura. Se ilustra en el
[diagrama 25](../diagramas/25-transacciones-2pc.md).

- **Marcadores de control en el log.** El `RecordBatch` gana dos *flags* en `attrs` —**transaccional**
  y **control**— disjuntos de los bits de códec (§9.3). Un batch **transaccional** lleva datos de una
  transacción abierta; un batch de **control** no lleva datos de usuario sino un único **marcador**
  `EndTxnMarker` (COMMIT o ABORT, con la época del coordinador), que **cierra** la transacción en esa
  partición. El codec del marcador (clave/valor versionados, decodificador defensivo) vive en
  `common/control_record`; el marcador es un record normal del log (interopera con cifrado y tiering).
- **LSO y `read_committed`.** `PartitionTxnIndex` mantiene el **Last Stable Offset** =
  `min(high_watermark, primer offset de la transacción abierta más antigua)`: la frontera hasta la que
  **todo** está decidido. Un consumidor `read_committed` no lee más allá del LSO y **filtra** los
  records de transacciones **abortadas** y los marcadores de control (`filter_committed`);
  `read_uncommitted` lee hasta el *high-watermark* como hasta ahora. Los offsets nunca se reescriben
  ni se borran por abortar: la visibilidad la decide el lector según su nivel de aislamiento.
