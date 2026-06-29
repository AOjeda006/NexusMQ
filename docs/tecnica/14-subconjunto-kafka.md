# 14. Subconjunto Kafka

Este capítulo **explica y enlaza** el *listener* compatible con el protocolo de Apache Kafka: por qué existe, qué subconjunto cubre y cómo encaja en la arquitectura *shared-nothing*. El contrato *as-built* (APIs, versiones, framing, RecordBatch) es [`../kafka.md`](../kafka.md); este capítulo no lo duplica.

## 14.1. Por qué un subconjunto, y por qué diferido

La compatibilidad Kafka no es el plano de datos principal de NexusMQ —ese es el [protocolo binario nativo](./13-protocolo-binario-nativo.md)—. Según [ADR-0004](../adr/adr-0004-protocolo-binario-propio-gateway-rest.md), implementar la especificación completa de Kafka desde el inicio habría atado el diseño a un contrato ajeno y enorme antes de que el broker existiera. Por eso se **difirió un subconjunto** a la **Fase 4** como *stretch*: lo justo para la demo «`kcat` habla con mi broker», con **interop verificada en vivo** (`kcat`/librdkafka), sin apostar el proyecto a la especificación entera.

El cableado real al servidor —el adaptador que conecta el protocolo Kafka al broker vivo— está registrado en [ADR-0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md), cuya pieza central se explica abajo.

- **Activación:** `nexusd --kafka-port <N>` (apagado si no se indica). Convive en el mismo proceso con el plano de datos nativo (`--port`) y el de administración (`--admin-port`).

## 14.2. Adaptador asíncrono cross-core (ADR-0029)

El plano de datos es *shared-nothing thread-per-core* con **sharding partición→núcleo** (ADR-0026): una partición la **posee un único núcleo**. Pero una conexión Kafka aceptada en un reactor cualquiera puede pedir particiones que pertenecen a **otro** núcleo. La solución de [ADR-0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md):

- El **puerto** `KafkaBroker` es **asíncrono**: sus métodos (`metadata`/`produce`/`fetch`/`list_offsets`) devuelven `task<...>`, y el `KafkaGateway` hace `co_await broker_.X(req)`. Así el adaptador puede **saltar al reactor dueño** de la partición y volver, en vez de serializar todo el tráfico Kafka en el núcleo 0.
- El **adaptador** concreto `KafkaServerBroker` (en `nexus-server`) implementa ese puerto **enrutando cada partición a su núcleo dueño** vía `PartitionRouter::route`. El puerto `KafkaBroker` no conoce `nexus-broker` (DIP): se testea con un `FakeBroker`.
- El `Server` abre el *listener* en `--kafka-port`; cada conexión la sirve `serve_kafka_connection`, que alimenta `KafkaGateway::handle_request`. El código vive en `src/kafka/` (codec y mensajes) y `src/server/kafka_*` (listener y adaptador).

## 14.3. Transporte y framing

El framing de Kafka difiere del nativo en dos puntos clave:

- Cada mensaje va **longitud-prefijo** con un `Size:INT32` **big-endian**: 4 bytes con el tamaño del resto del mensaje, seguido de ese número de bytes.
- **Endianness big-endian** en todo el protocolo (contrato de Kafka), a diferencia del protocolo nativo, que es **little-endian**.

Como en el nativo, el transporte es **TCP** y la petición/respuesta se multiplexa por `correlationId`. El detalle está en [`../kafka.md`](../kafka.md#transporte-y-framing) y en el [diagrama 17](../diagramas/17-peticion-respuesta-kafka.md).

## 14.4. APIs y versiones soportadas

El subconjunto cubre **cinco APIs**: `ApiVersions`, `Metadata`, `Produce`, `Fetch` y `ListOffsets` —lo imprescindible para *metadata*, *produce* y *consume*—. El cliente abre con `ApiVersions`, el broker publica los rangos `[min, max]` soportados, y a partir de ahí cada petición lleva su versión; **lo no listado no se soporta** (el cliente recibe el rango y se ajusta). La tabla exacta de ApiKeys y rangos está en [`../kafka.md`](../kafka.md#apis-soportadas).

`ApiVersions` anuncia deliberadamente rangos que **cubren lo que negocia librdkafka 2.x** (notablemente Produce v7 y Fetch v11), no las versiones máximas teóricas.

## 14.5. Clásico vs flexible (tagged fields)

A partir de ciertas versiones, Kafka cambió a un formato **flexible**: cabeceras con *tagged fields* y arrays/strings *compact* con longitud en `UNSIGNED_VARINT`. El codec elige el formato **por API y versión**, según un umbral `flexible_since`: una versión `>=` su umbral es **flexible**; por debajo, **clásica**. Los umbrales por API están en [`../kafka.md`](../kafka.md#clásico-vs-flexible).

La consecuencia práctica, y un detalle que [ADR-0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md) corrige explícitamente: las versiones que negocia librdkafka (**Produce v7 y Fetch v11**) son **clásicas** (longitudes `INT16`/`INT32`, sin *tagged fields*), mientras que **Metadata v9, ListOffsets v7 y ApiVersions v3** son **flexibles**. Por eso el codec **debe** ramificar por versión, con *gates* de campo por versión (p. ej. `last_fetched_epoch` solo v12+ — el bug que rompía Fetch v11). Asumir versiones flexibles e ignorar las clásicas rompe con `kcat`: era el fallo de las iteraciones previas, descartado en ADR-0029.

Una excepción heredada del propio Kafka: `ApiVersions` responde **siempre** con cabecera de respuesta v0 (clásica) aunque su cuerpo sea flexible.

## 14.6. RecordBatch tratado como opaco

`Produce`/`Fetch` transportan **RecordBatch v2 de Kafka** (cabecera de 61 bytes con su `CRC32C`). El broker lo guarda y replica como **blob opaco** dentro de su envoltorio interno: **no reinterpreta el contenido**, solo su `record_count` gobierna la asignación de offsets. Esta decisión evita el coste y la fragilidad de traducir el RecordBatch a/desde el formato interno (reencode + recálculo de CRC en el *hot path*).

En `Fetch`, el blob se reconstruye y se le **reescribe el `baseOffset`** autoritativo. Es barato: el CRC de Kafka v2 cubre desde `attributes` en adelante, así que parchear `baseOffset` **no** invalida el CRC.

Un detalle de interop importante: un `Fetch` sin datos nuevos (consumidor al día con el *high-watermark*) devuelve un `MessageSet` **presente pero de longitud 0**, nunca `null` (-1) — librdkafka rechaza un `MessageSetSize` de -1 como trama corrupta.

## 14.7. Mapeo al broker e interop

El adaptador traduce cada petición Kafka a operaciones del broker vivo: `Metadata` → topics/particiones del nodo; `Produce` → `produce` sobre la partición destino (RecordBatch opaco); `Fetch` → lectura desde el offset pedido hasta el *high-watermark*; `ListOffsets` → *earliest* (`log_start_offset`) / *latest* (*high-watermark*); `ApiVersions` → los rangos publicados. El mapeo completo está en [`../kafka.md`](../kafka.md#mapeo-al-broker).

Con esto, `kcat` interopera en vivo: `-L` (metadata), `-P` (produce) y `-C` (consume), incluido **multi-partición cross-core** (mensajes servidos desde el núcleo dueño de cada partición).

## 14.8. Límites conocidos

El subconjunto es deliberadamente parcial (las limitaciones se detallan también en el [capítulo 30](./30-limitaciones-y-trabajo-futuro.md)):

- **Un-nodo:** la `Metadata` anuncia este nodo como único broker y líder de sus particiones; no hay descubrimiento de clúster por el plano Kafka.
- **Sin grupos de consumidores** por el plano Kafka (`OffsetCommit`/`JoinGroup`/… no están en el subconjunto): el seguimiento de offsets del lado servidor vive en el plano nativo.
- **Sin transacciones ni *idempotent producer*** de Kafka: la idempotencia del broker es la del plano nativo.

## 14.9. Referencias

- Contrato *as-built*: [`../kafka.md`](../kafka.md).
- Diagrama: [diagrama 17](../diagramas/17-peticion-respuesta-kafka.md).
- ADRs: [ADR-0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md) (adaptador async cross-core), [ADR-0004](../adr/adr-0004-protocolo-binario-propio-gateway-rest.md) (subconjunto diferido a Fase 4).
- Capítulos relacionados: [13 (protocolo nativo)](./13-protocolo-binario-nativo.md), [7 (concurrencia / sharding)](./07-concurrencia.md), [30 (limitaciones)](./30-limitaciones-y-trabajo-futuro.md).
