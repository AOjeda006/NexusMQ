# 3. Estado del arte

> Dónde se sitúa NexusMQ en el mapa de los *brokers* de mensajería: qué hereda del
> modelo clásico de log particionado y por qué adopta la **frontera actual**
> (*thread-per-core* + Raft por partición) en lugar del diseño de hace más de una década.

## 3.1 El modelo de log particionado

Apache Kafka popularizó el **log distribuido append-only particionado** como sustrato
común de *event streaming*, *event sourcing*, integración de datos y comunicación entre
microservicios. Ese modelo —*topics* divididos en **particiones**, cada una un log
inmutable de registros identificados por **offset** monótono, con consumidores que leen
desde el offset que eligen— es la base que NexusMQ **hereda** (ver el [capítulo 9,
Almacenamiento](./09-almacenamiento.md)). De Kafka se toman también el **high-watermark**,
los **grupos de consumidores**, la **retención** y el **formato de *record batch* v2**.

## 3.2 Dos generaciones de arquitectura

La diferencia entre NexusMQ y un Kafka clásico no está en *qué* es (un log
particionado), sino en *cómo* se ejecuta por dentro:

| Dimensión | Kafka clásico (≈2011) | Frontera actual (Redpanda) | NexusMQ |
| --------- | --------------------- | -------------------------- | ------- |
| Concurrencia | *thread-pool* + estado compartido bajo *locks* | ***shared-nothing thread-per-core*** | *thread-per-core* (reactor propio) |
| Replicación | ISR + *controller* (consenso solo para metadatos) | **Raft por partición** | **Raft por partición** |
| I/O | JVM + *page cache* del SO | io_uring (Seastar) | proactor io_uring/IOCP + corutinas C++23 |
| Lenguaje | Scala/Java (JVM, GC) | **C++** (Seastar) | **C++23** (reactor mínimo propio) |

El salto de generación es deliberado: NexusMQ no se posiciona en la frontera de 2011
(*thread-pool* + ISR clásico), sino en la **frontera actual**, la que usa **Redpanda**
para batir a Kafka en latencia. Es el diseño más ambicioso, el más coherente con el
objetivo de *correctitud demostrable* y el que mejor compone consigo mismo: un grupo
Raft anclado a un núcleo, un anillo io_uring por núcleo, sin estado mutable compartido.

## 3.3 Análisis de referentes

| Sistema | Lenguaje | Modelo | Qué se toma como referencia |
| ------- | -------- | ------ | --------------------------- |
| **Apache Kafka** | Scala/Java | Log particionado + ISR; KRaft para metadatos | Modelo de log, particiones, grupos de consumidores, *high-watermark*, retención, *record batch*. |
| **Redpanda** | **C++** (Seastar) | Kafka-compatible; **Raft por partición**; *thread-per-core* / shared-nothing | **Validación directa del diseño adoptado**: un broker *thread-per-core* + Raft por partición es viable y *state-of-the-art* en C++. |
| **RabbitMQ** | Erlang | Colas AMQP, *routing* por *exchange* | Semánticas de entrega, *dead letter queues*, ACK/NACK. |
| **NATS / JetStream** | Go | Mensajería ligera + *streams* durables | Simplicidad del protocolo, baja latencia, *at-least-once*. |
| **Apache Pulsar** | Java | *Compute/storage* separados (BookKeeper) | Separación de capas, *tiered storage* (idea para *stretch*). |

## 3.4 Posicionamiento de NexusMQ

- **Redpanda como validación de diseño, no como dependencia.** Redpanda demuestra que la
  arquitectura *thread-per-core* + Raft por partición es construible en C++ con
  rendimiento de primera línea. NexusMQ construye su **propio reactor mínimo** sobre
  io_uring + corutinas; **no usa Seastar** (dependencia pesada, *Linux-only*): "construir"
  demuestra más que "usar" y encaja con la política de hacer el núcleo a mano
  (ver [ADR-0005](../adr/adr-0005-concurrencia-shared-nothing-thread-per-core.md)).
- **No compite con Kafka en producción.** El objetivo es **demostrar**, en una base de
  código propia y comprensible, las técnicas que hacen viable un sistema así —profundidad
  técnica y criterio—, no acumular funcionalidades ni superar a Kafka en *features*.
- **Compatibilidad Kafka como subconjunto recuperable.** En vez de atarse a la
  especificación completa de Kafka, se implementa un **subconjunto** Kafka-compatible
  como *stretch* de Fase 4 (interop con `kcat`), preservando el control del protocolo
  binario propio (ver [ADR-0004](../adr/adr-0004-protocolo-binario-propio-gateway-rest.md)
  y el [capítulo 14](./14-subconjunto-kafka.md)).
