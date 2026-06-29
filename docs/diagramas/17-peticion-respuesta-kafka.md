# Diagrama 17: Petición/respuesta del subconjunto Kafka

El *listener* compatible con Apache Kafka (`nexusd --kafka-port <N>`) habla el *wire format* de Kafka:
**big-endian**, *framing* `Size:INT32` y cabeceras **clásicas o flexibles** según API y versión. El
`RecordBatch` v2 de Kafka viaja como **blob opaco** (el broker no lo reinterpreta). Contrato:
[`../kafka.md`](../kafka.md); código: `../../src/server/kafka_connection.hpp`,
`../../src/kafka/codec.hpp`.

## Framing (big-endian)

Cada mensaje —petición y respuesta— va longitud-prefijo con un `Size:INT32` big-endian, seguido de la
cabecera correspondiente y el cuerpo de la API.

| Campo    | Tamaño | Tipo    | Descripción                                                                 |
| -------- | ------ | ------- | --------------------------------------------------------------------------- |
| `Size`   | 4      | `INT32` | Bytes del **resto** del mensaje (cabecera + cuerpo), big-endian.            |
| cabecera | var.   | —       | Cabecera de request/response (clásica o flexible, ver abajo).               |
| cuerpo   | var.   | —       | Cuerpo de la API negociada (`Produce`/`Fetch`/`Metadata`/`ListOffsets`/`ApiVersions`). |

### Clásico vs flexible (*tagged fields*)

El codec elige el formato **por API y versión** según el umbral `flexible_since`: una versión `>=` su
umbral es **flexible** (arrays/strings *compact* con longitud en `UNSIGNED_VARINT` y *tagged fields*);
por debajo es **clásica**.

| API           | `flexible_since` | Cabecera petición          | Cabecera respuesta                 |
| ------------- | ---------------- | -------------------------- | ---------------------------------- |
| `Produce`     | 9                | v1 clásica / v2 flexible   | v0 clásica / v1 flexible           |
| `Fetch`       | 12               | v1 clásica / v2 flexible   | v0 clásica / v1 flexible           |
| `ListOffsets` | 6                | v1 clásica / v2 flexible   | v0 clásica / v1 flexible           |
| `Metadata`    | 9                | v1 clásica / v2 flexible   | v0 clásica / v1 flexible           |
| `ApiVersions` | 3                | v1 clásica / v2 flexible   | **siempre v0** (regla de Kafka)    |

Por eso las versiones que negocia librdkafka 2.x (`Produce` v7, `Fetch` v11) van en formato **clásico**,
mientras que `Metadata` v9, `ListOffsets` v7 y `ApiVersions` v3 van en **flexible**. **Excepción:**
`ApiVersions` responde **siempre** con cabecera v0 aunque su cuerpo sea flexible.

## Intercambio cliente (`kcat`/librdkafka) ↔ gateway

```mermaid
sequenceDiagram
    autonumber
    participant C as Cliente (kcat / librdkafka)
    participant G as Gateway Kafka (nexusd --kafka-port)
    participant B as Broker (KafkaServerBroker)

    Note over C,G: Conexión TCP; cada mensaje lleva Size:INT32 (big-endian) por delante

    C->>G: ApiVersions v3 (cab. petición flexible)
    G-->>C: ApiVersions (cuerpo flexible, cab. respuesta v0)<br/>rangos min-max por ApiKey

    Note over C: El cliente fija la versión por API dentro de los rangos

    C->>G: Metadata v9 (flexible)
    G->>B: enruta a la partición (PartitionRouter, cross-core)
    B-->>G: topics/particiones (este nodo = líder)
    G-->>C: Metadata (flexible) con correlationId espejado

    C->>G: Produce v7 (clásico) + RecordBatch v2 (blob opaco)
    G->>B: produce sobre la partición destino (almacena el batch intacto)
    B-->>G: baseOffset asignado / errorCode
    G-->>C: Produce response (clásico)

    C->>G: Fetch v11 (clásico) desde offset
    G->>B: lee desde offset hasta high-watermark
    B-->>G: RecordBatch(es) opacos (baseOffset reescrito, CRC intacto)
    G-->>C: Fetch response; si vacío, MessageSet presente de longitud 0 (nunca -1)
```

El `correlationId` espeja petición↔respuesta (igual que Kafka). En `Fetch` el blob se reconstruye
reescribiendo solo el `baseOffset`: barato, porque el CRC32C de Kafka v2 cubre desde `attributes` en
adelante, así que no hay que recalcularlo. Un `Fetch` sin datos devuelve un `MessageSet` **presente de
longitud 0**, nunca `null` (-1), que librdkafka rechazaría como corrupto.
