# ADR-0029: Adaptador Kafka **asíncrono cross-core** sobre el broker vivo + codec por versión (interop `kcat`)

- **Estado:** aceptado
- **Fecha:** 2026-06-28

## Contexto

F7a–F7e entregaron el subconjunto Kafka como **librería de protocolo** (`src/kafka/`: codec big-endian, cabeceras, `ApiVersions`/`Metadata`/`Produce`/`Fetch`, *dispatcher* `KafkaGateway` con el puerto `KafkaBroker`), pero **sin cablear al servidor**: `nexusd` no escuchaba Kafka por ningún socket y no existía un adaptador real del puerto `KafkaBroker`. Faltaba cerrar el objetivo declarado de F7 —"habla con `kcat`"— con **interop verificada en vivo**.

Dos fuerzas condicionan el diseño:

1. el plano de datos es **shared-nothing thread-per-core** con **sharding partición→núcleo** (ADR-0026), así que una conexión Kafka aceptada en un reactor cualquiera debe poder tocar particiones que **posee otro núcleo**;
2. `kcat`/librdkafka **no negocia las versiones flexibles** que F7d/F7e asumían: usa **Produce v7** y **Fetch v11**, que son **clásicas** (longitudes `INT16`/`INT32`, sin *tagged fields*), mientras que `Metadata v9`/`ListOffsets v7`/`ApiVersions v3` sí son flexibles.

## Decisión

**Un adaptador `KafkaServerBroker` (en `nexus-server`) que implementa el puerto `KafkaBroker` como corrutinas `task<...>` y enruta cada partición a su núcleo dueño vía `PartitionRouter::route` (ADR-0026), con un *listener* Kafka en puerto propio (`--kafka-port`) y codecs Produce/Fetch gobernados por la versión negociada.** Cinco piezas:

1. **Puerto asíncrono.** Los métodos de `KafkaBroker` (`metadata`/`produce`/`fetch`/`list_offsets`) pasan a devolver `task<...>`: el `KafkaGateway` hace `co_await broker_.X(req)`. Así el adaptador puede **saltar al reactor dueño** de la partición y volver, en vez de restringir el servicio al núcleo 0. Es la elección **correcta** frente a un adaptador síncrono-solo-núcleo-0: respeta el *sharding* sin serializar todo en un núcleo.

2. **Cableado.** `Server` abre un *listener* en `--kafka-port` (separado del 9092 nativo); cada conexión la sirve `serve_kafka_connection` con el framing **`Size:INT32`** (big-endian) que alimenta `KafkaGateway::handle_request`. El adaptador se construye en el *composition root* del servidor (núcleo 0 como ancla de metadatos; `bind_cluster` le inyecta el `PartitionRouter` y los `TopicManager` por núcleo).

3. **Codec por versión.** `decode/encode` de Produce y Fetch reciben `api_version` y ramifican **clásico vs flexible** (`is_flexible(api_key, version)`), con *gates* de campo por versión (p. ej. `transactional_id` v3+, `last_fetched_epoch` **solo v12+** —el bug que rompía Fetch v11—). `ApiVersions` anuncia rangos que cubren lo que negocia librdkafka.

4. **Almacenamiento opaco con rebase de offset.** El `RecordBatch` v2 de Kafka se guarda **opaco** dentro del envoltorio interno del log (solo su `record_count` gobierna los offsets); en `Fetch` se reconstruye el blob y se **reescribe el `baseOffset`** autoritativo (el CRC de Kafka cubre desde `attributes`, así que parchear `baseOffset` **no** invalida el CRC).

5. **Records nunca `null`.** Un `Fetch` sin datos nuevos (consumidor al día con el *high-watermark*) devuelve un MessageSet **vacío** (longitud 0), **no** `null` (-1): librdkafka rechaza un `MessageSetSize` de -1 como trama corrupta.

## Consecuencias

- (+) `kcat` **interopera en vivo**: `-L` (metadata), `-P` (produce) y `-C` (consume) funcionan, incluido **multi-partición cross-core** (mensajes servidos desde el núcleo dueño).
- (+) El puerto `KafkaBroker` queda **desacoplado** del broker (DIP): el protocolo no conoce `nexus-broker` y se testea con un `FakeBroker`; el adaptador con un broker local *unbound*.
- (+) El diseño de ADR-0026 se confirma: bastó `co_await route(...)` por partición.
- (−) El adaptador asíncrono **propaga `task<>`** a todo el puerto (más verboso que síncrono), justificado por la corrección cross-core.
- (−) Cobertura **parcial** del protocolo: solo las 5 APIs y las versiones que negocia librdkafka; sin grupos de consumidores (`JoinGroup`/`Heartbeat`) ni transacciones —fuera del alcance *stretch*.

## Alternativas consideradas

- **Adaptador síncrono que sirve solo desde el núcleo 0:** *diff* menor, pero **viola el sharding** (toda partición tendría que vivir o copiarse al núcleo 0) o serializa el tráfico Kafka en un núcleo. Descartada: la corrección y la coherencia con ADR-0026 pesan más que el tamaño del cambio.
- **Asumir versiones flexibles (v9/v12) e ignorar las clásicas:** es lo que hacía F7d/F7e y **rompe con `kcat`** (Produce v7 / Fetch v11). El codec **debe** ramificar por versión. Descartada.
- **Traducir el `RecordBatch` de Kafka a/desde el formato interno (no opaco):** caro y frágil (reencode más recálculo de CRC en el *hot path*) sin beneficio: el log solo necesita contar records y asignar offsets. Descartada a favor del blob opaco más rebase de `baseOffset`.
