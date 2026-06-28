# Subconjunto Kafka-compatible de NexusMQ

Además del [protocolo binario propio](protocol.md), NexusMQ expone un **listener compatible con el
protocolo de Apache Kafka** sobre TCP, pensado para interoperar con clientes y herramientas del
ecosistema (verificado en vivo con **`kcat`**/librdkafka). Es un **subconjunto** deliberado: las APIs
imprescindibles para *metadata*, *produce* y *consume*, no el protocolo completo.

- **Activación:** se arranca con `nexusd --kafka-port <N>` (apagado si no se indica). Convive con el
  plano de datos nativo (`--port`) y el de administración (`--admin-port`) en el mismo proceso.
- **Decisión de diseño:** ADR-0029 (adaptador Kafka **asíncrono cross-core** sobre el broker vivo +
  **codec por versión** + almacenamiento **opaco** del `RecordBatch`).
- **Fuentes de verdad del código:** `src/kafka/` (codec y mensajes), `src/server/kafka_connection.hpp`
  (listener + framing) y `src/server/kafka_adapter.{hpp,cpp}` (puente al broker).

## Transporte y *framing*

- **TCP**, petición/respuesta multiplexada por `correlationId` (igual que Kafka).
- Cada mensaje va **longitud-prefijo** con un `Size:INT32` **big-endian**: 4 bytes con el tamaño del
  resto del mensaje, seguido de ese número de bytes.
- **Endianness:** **big-endian** en todo el protocolo (contrato de Kafka), a diferencia del protocolo
  nativo de NexusMQ, que es little-endian.

## APIs soportadas

| ApiKey | Nombre        | Versiones (`min`–`max`) | Negociada por librdkafka/`kcat` | Formato |
| -----: | ------------- | ----------------------- | ------------------------------- | ------- |
| 0      | `Produce`     | 0–9                     | v7                              | clásico |
| 1      | `Fetch`       | 0–12                    | v11                             | clásico |
| 2      | `ListOffsets` | 0–7                     | v7                              | flexible |
| 3      | `Metadata`    | 0–9                     | v9                              | flexible |
| 18     | `ApiVersions` | 0–3                     | v3                              | flexible |

El cliente abre con **`ApiVersions`** y el broker publica estos rangos; a partir de ahí cada petición
lleva su versión. Lo no listado **no** se soporta (el cliente recibe el rango y se ajusta).

### Clásico vs *flexible*

Kafka cambió a partir de ciertas versiones a un formato **flexible** (cabeceras con *tagged fields* y
arrays/strings *compact* con longitud en `UNSIGNED_VARINT`). El codec elige el formato **por API y
versión** según el umbral `flexible_since` (`src/kafka/messages.cpp`):

| API           | `flexible_since` |
| ------------- | ---------------- |
| `Produce`     | 9                |
| `Fetch`       | 12               |
| `ListOffsets` | 6                |
| `Metadata`    | 9                |
| `ApiVersions` | 3                |

Una versión `>=` su umbral es flexible; por debajo, clásica. Por eso las versiones que negocia
librdkafka 2.x (Produce v7 y Fetch v11) van en formato **clásico**, mientras que Metadata v9,
ListOffsets v7 y ApiVersions v3 van en **flexible**. Detalles de cabecera:

- **Cabecera de petición:** v1 (clásica, con `clientId`) o v2 (flexible, con *tagged fields*).
- **Cabecera de respuesta:** v0 (clásica) o v1 (flexible). **Excepción:** `ApiVersions` responde
  **siempre** con cabecera v0 aunque su cuerpo sea flexible (regla del propio Kafka).

## RecordBatch y almacenamiento

`Produce`/`Fetch` transportan **RecordBatch v2 de Kafka** (cabecera de 61 bytes con su `CRC32C`). El
broker lo guarda y replica como **blob opaco** dentro de su envoltorio interno (`RecordBatch`,
`kHeaderSize = 36`): no reinterpreta el contenido. En `Fetch`, el blob se reconstruye y se le
**reescribe el `baseOffset`** — barato, porque el CRC de Kafka v2 cubre desde `attributes` en
adelante, así que `baseOffset` se puede parchear sin recalcular el CRC.

- **`Fetch` vacío:** se devuelve un `MessageSet` **presente pero de longitud 0**, nunca `null` (-1):
  librdkafka rechaza un `MessageSetSize` de -1 como corrupto.

## Mapeo al broker

El adaptador (`KafkaServerBroker`) traduce cada petición Kafka a operaciones del broker vivo,
**enrutando a la partición** por su núcleo dueño (cross-core vía `PartitionRouter`, ADR-0026):

- **`Metadata`** → topics y particiones del nodo (un solo broker, este nodo como líder).
- **`Produce`** → `produce` sobre la partición destino (el RecordBatch se almacena opaco).
- **`Fetch`** → lectura desde el offset pedido hasta el *high-watermark*.
- **`ListOffsets`** → *earliest* (`log_start_offset`) / *latest* (*high-watermark*).
- **`ApiVersions`** → los rangos de la tabla anterior.

## Límites conocidos

- Subconjunto **un-nodo**: la `Metadata` anuncia este nodo como único broker y líder de sus
  particiones; no hay descubrimiento de clúster por el plano Kafka.
- Sin grupos de consumidores por el plano Kafka (`OffsetCommit`/`JoinGroup`/… no están en el
  subconjunto): el seguimiento de offsets del lado servidor vive en el plano nativo.
- Sin transacciones ni *idempotent producer* de Kafka (la idempotencia del broker es la del plano
  nativo, §5.9 del anteproyecto).
