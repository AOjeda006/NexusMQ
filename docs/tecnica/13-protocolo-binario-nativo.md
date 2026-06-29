# 13. Protocolo binario nativo

Este capítulo **explica y enlaza** el protocolo binario propio del plano de datos de NexusMQ —el rationale, las capas que lo implementan y los flujos de petición/respuesta—. El contrato *as-built* (byte-layout exacto, tipos campo a campo) es [`../protocol.md`](../protocol.md); este capítulo no lo duplica.

## 13.1. Por qué un protocolo propio

El plano de datos necesita un protocolo, y había dos caminos en tensión: un **binario propio** o la **compatibilidad con Apache Kafka** desde el inicio. La decisión está registrada en [ADR-0004](../adr/adr-0004-protocolo-binario-propio-gateway-rest.md): se implementa un **protocolo binario propio** para las fases nucleares, con un **gateway REST** para interoperabilidad temprana, y se **difiere** un subconjunto Kafka-compatible a la Fase 4 (ver [capítulo 14](./14-subconjunto-kafka.md)).

Las razones, en una frase: un protocolo propio **maximiza el aprendizaje y el control** —*framing*, varint, *multiplexing* por *correlation id*, versionado negociado, control de flujo por créditos— y compone **sin *impedance mismatch*** con la arquitectura *shared-nothing thread-per-core*. La compatibilidad Kafka habría dado ecosistema, pero obliga a implementar una especificación ajena y enorme (más de 70 *API keys* versionadas, *consumer group protocol*…), atando el diseño antes de existir. El titular «Kafka-compatible» queda **recuperable más tarde** sin pagar su coste por adelantado.

## 13.2. La capa `nexus-wire` (framing sobre conexión)

El protocolo está partido en dos capas con responsabilidades distintas, una decisión de capas limpias registrada en [ADR-0013](../adr/adr-0013-capa-nexus-wire.md):

- **`nexus-protocol`** es **protocolo puro**: *codec* (`Encoder`/`Decoder`), `FrameHeader`, mensajes y códigos de error. Depende **solo** de `nexus-common` —sin E/S, sin async—, de modo que la (de)serialización se prueba y se *fuzzea* sin tocar sockets ni io_uring.
- **`nexus-wire`** aloja el **framing sobre conexión**: `Frame`, `FrameReader` y `FrameWriter`. Lee y escribe tramas longitud-prefijo sobre un `Socket` mediante el `Proactor`, así que depende de `nexus-io` (sockets, proactor) y de las corutinas (`task<expected<T>>`). Los consumidores del transporte de tramas —broker, client, ingress, server— dependen de `nexus-wire`.

`FrameReader::read_frame` expone el payload como **vista dentro del búfer del lector** (válida hasta la siguiente lectura): es *zero-copy* a cambio de una invariante de vida documentada. Esto encaja con la lectura del framing descrita abajo.

## 13.3. Estructura de la cabecera de trama

Toda trama es **longitud-prefijo**: una cabecera de **14 bytes** (`FrameHeader::kEncodedSize`) seguida del payload, en **little-endian**. La cabecera lleva, en orden, `length:u32`, `apiKey:u16`, `apiVersion:u16`, `correlationId:u32` y `flags:u16`; el payload ocupa `length − 10` bytes. El byte-layout exacto está en el [diagrama 16](../diagramas/16-frame-nativo.md) y en [`../protocol.md`](../protocol.md#trama-framing); aquí basta con el modelo de lectura y la semántica de cada campo.

- **Lectura en dos fases:** se leen 4 bytes (`length`), y luego **exactamente `length` bytes** más. Como `length` cuenta los bytes *tras* su propio campo (resto de cabecera + payload), un lector nunca necesita conocer de antemano el tamaño total.
- **Cotas de seguridad:** el lector valida una **cota inferior** (debe caber el resto de la cabecera) y una **cota superior anti-DoS** (`max_frame`, por defecto **16 MiB**) antes de reservar el búfer.
- **`flags`:** un mapa de bits de control. Hoy se usa `0x0001` (*credit update*): la trama acarrea una actualización de créditos del control de flujo/*backpressure*. `FrameHeader::has_credit_update()` lo consulta.

### ApiKeys

El campo `apiKey` selecciona la operación. El enum `ApiKey` (`src/protocol/frame.hpp`) define las operaciones del plano de datos: `ApiVersions`, `Metadata`, `Produce`, `Fetch`, la familia de offsets de grupo (`OffsetCommit`/`OffsetFetch`), el protocolo de grupos de consumidores (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`) y administración de topics (`CreateTopic`/`DeleteTopic`). La tabla de valores numéricos y su descripción está en [`../protocol.md`](../protocol.md#apikeys).

## 13.4. Versionado negociado

Cada operación tiene un **esquema versionado** por su `apiVersion`, y la versión efectiva se **negocia** al abrir la conexión:

1. El cliente envía `ApiVersions` anunciando el **máximo** que soporta por `ApiKey`.
2. El servidor publica un rango `[min, max]` por `ApiKey`.
3. La versión efectiva es la **mayor que ambos soportan**: `min(client_max, server.max)` si alcanza `server.min`; `0` si la `ApiKey` no es negociable o no hay solape.

A partir de ahí, cada trama lleva su `apiVersion` y el *codec* (de)serializa según el esquema de esa versión. La lógica de negociación vive en `src/protocol/versioning.hpp`. Este versionado permite **evolucionar el esquema sin romper** clientes antiguos (campos opcionales en versiones nuevas), coherente con la evolución de contratos del proyecto.

## 13.5. Correlation id y multiplexing

El transporte es **TCP**, y una **única conexión multiplexa muchas peticiones** simultáneas. El mecanismo es el `correlationId`:

- Lo **elige el cliente** en cada petición (típicamente un contador monótono por conexión).
- La respuesta del broker **espeja** (`echo`) ese mismo `correlationId`.
- El cliente casa respuesta con petición por ese id, sin necesidad de que las respuestas lleguen en orden de envío.

Esto evita el coste de abrir una conexión TCP (+TLS) por petición y minimiza RTTs: las peticiones se pueden *pipeline*-ar sobre una conexión persistente.

## 13.6. Flujos produce/fetch

Sobre este framing se construyen los dos flujos calientes del plano de datos. El **detalle de los mensajes** (campos de `ProduceRequest`/`FetchResponse`, etc.) está en `src/protocol/messages.hpp` y resumido en [`../protocol.md`](../protocol.md); aquí, el flujo de extremo a extremo:

- **`Produce`** publica un `RecordBatch` en una partición. El batch viaja **intacto** por el log y por la replicación: es la **unidad de escritura y de entrada de Raft** (ADR-0014). La respuesta acarrea el offset asignado y un `errorCode:i16`. La política de `acks` (0/1/quorum) se resuelve sobre el `commitIndex` de Raft de la partición (ver [capítulo 10](./10-replicacion-y-consenso.md)).
- **`Fetch`** lee registros desde un offset hasta el *high-watermark* de la partición. El blob de records puede ir **comprimido** (LZ4/Zstd, indicado en los 2 bits bajos de `attrs`): el broker lo trata como **opaco** —lo guarda y replica comprimido— y solo el cliente lo descomprime al consumir. El bloque comprimido lleva su tamaño original como prefijo (defensa anti *decompression bomb*).

Las respuestas siempre incluyen el `errorCode:i16` del contrato externo de errores; su taxonomía y la traducción en el borde se tratan en el [capítulo 16](./16-modelo-errores-wire-codes.md).

## 13.7. Seguridad del transporte

El plano de datos puede terminar **TLS 1.3** (y **mTLS** intra-clúster) por delante del framing (ADR-0019): el protocolo binario es idéntico, cifrado o en claro. El puente TLS sobre el proactor asíncrono se trata en el [capítulo 11 (Ingress)](./11-ingress.md) y el [capítulo 27 (Seguridad)](./27-seguridad.md).

## 13.8. Referencias

- Contrato *as-built*: [`../protocol.md`](../protocol.md).
- Diagrama del frame: [diagrama 16](../diagramas/16-frame-nativo.md).
- ADRs: [ADR-0004](../adr/adr-0004-protocolo-binario-propio-gateway-rest.md) (protocolo propio), [ADR-0013](../adr/adr-0013-capa-nexus-wire.md) (`nexus-wire`), [ADR-0009](../adr/adr-0009-manejo-errores-por-capa.md) (errores por capa).
- Capítulos relacionados: [14 (Kafka)](./14-subconjunto-kafka.md), [16 (modelo de errores)](./16-modelo-errores-wire-codes.md).
