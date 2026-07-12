# 2. Contexto y motivación

> El problema que NexusMQ aborda, por qué se construye, qué objetivos persigue y —tan
> importante como lo anterior— qué queda explícitamente fuera de alcance.

## 2.1 El problema: la mensajería como columna vertebral

Los sistemas distribuidos modernos se construyen, cada vez más, alrededor de una **columna
vertebral de mensajería asíncrona**. En lugar de que los servicios se llamen entre sí de forma
síncrona y acoplada, **publican** y **consumen** eventos a través de un *broker* que los desacopla
en el tiempo y en el espacio. Apache Kafka popularizó el modelo del **log distribuido append-only
particionado** como sustrato común para *event streaming*, *event sourcing*, integración de datos y
comunicación entre microservicios.

El modelo de log particionado resuelve varios problemas a la vez:

- **Desacoplo temporal** — productor y consumidor no necesitan estar vivos a la vez.
- **Re-lectura (*replay*)** — varios consumidores independientes pueden leer el mismo flujo, y
  rebobinar a un *offset* histórico.
- **Escalado horizontal** — por particiones, cada una unidad de paralelismo y de replicación.

Construir un broker de esta clase **desde cero** obliga a enfrentarse, sin atajos, a los problemas
centrales de la ingeniería de sistemas: **durabilidad** ante fallos, **consenso y replicación**,
**control de concurrencia** de alto rendimiento, **gestión explícita de memoria**, **I/O asíncrona**
eficiente y **diseño de protocolos** de red. Es, por tanto, un vehículo idóneo para demostrar
dominio de C++ moderno y de los fundamentos de los sistemas distribuidos.

## 2.2 Objetivos de aprendizaje y portfolio

NexusMQ es, ante todo, un **proyecto de aprendizaje y de portfolio**. No busca competir con Kafka en
producción, sino **demostrar**, en una base de código propia y comprensible, las técnicas que hacen
viable un sistema así.

La propuesta de valor del proyecto se sostiene en cuatro diferenciadores:

- **Arquitectura *shared-nothing thread-per-core* con reactor propio.** No hay *thread-pool* con
  estado compartido bajo *locks*: cada núcleo es un reactor independiente (su *event loop* io_uring,
  su *allocator*, su subconjunto de particiones) y se comunican por **paso de mensajes** explícito.
  Es la frontera actual del diseño de brokers, no la de hace más de una década (ver
  [estado del arte](./03-estado-del-arte.md)).
- **Capa de *ingress* integrada** —no como pieza externa (HAProxy/Nginx) sino como parte del
  producto— y con **dos modos** explícitos (cliente nativo directo al líder + proxy/REST),
  demostrando criterio en vez de "un *ingress* que lo hace todo".
- **C++ de alto rendimiento con criterio.** Cada técnica avanzada (lock-free, SIMD, *allocators* por
  núcleo, io_uring) se introduce **donde un *profiling* lo justifica** y se acompaña de su
  *benchmark* antes/después, con metodología anti-*coordinated-omission*.
- **Portabilidad por diseño.** Linux primero, con una abstracción de I/O (*proactor*) preparada para
  un backend Windows (IOCP) que más tarde se completó y verificó en runtime.

El proyecto busca evidenciar **profundidad técnica en un sistema coherente** (correctitud
distribuida + latencia + funcionamiento *end-to-end*) y **criterio** (decisiones justificadas y
medidas), por encima de la mera acumulación de funcionalidades. La ambición se persigue como
**profundidad dentro de una sola tesis arquitectónica**, nunca como amplitud: dos arquitecturas en
paralelo sería *scope sprawl*, no ambición.

### Casos de uso objetivo

El diseño se valida contra cuatro patrones de uso representativos:

- **Streaming de eventos** entre microservicios (publicación/suscripción durable).
- **Cola de trabajo** con grupos de consumidores y reparto de carga.
- **Event sourcing / auditoría** — log inmutable re-leíble desde cualquier *offset*.
- **Buffer de absorción de picos** (*backpressure*) entre sistemas de ritmos distintos.

## 2.3 Objetivos específicos (OE-1..OE-7)

El objetivo general es diseñar e implementar, por fases, un broker de mensajería distribuido de alto
rendimiento en C++23, con arquitectura *shared-nothing thread-per-core*, persistencia durable,
replicación con consenso (Raft por partición) y una capa de *ingress* inteligente, acompañado de
herramientas de operación y observabilidad. Se desglosa en siete objetivos específicos:

- **OE-1.** Construir un *storage engine* de log *append-only* con segmentación, índice disperso,
  *checksums* (CRC32C) y política de durabilidad configurable.
- **OE-2.** Definir e implementar un **protocolo binario propio** con *framing*, *multiplexing* y
  versionado, una librería cliente nativa en C++ y un **gateway REST** para interoperabilidad.
- **OE-3.** Implementar el núcleo del broker sobre un **reactor *thread-per-core* propio**: *topics*,
  particiones (cada una anclada a un núcleo), grupos de consumidores y semánticas de entrega.
- **OE-4.** Dotar al sistema de **replicación con consenso mediante Raft por partición** (elección de
  líder y *failover* automático) para tolerancia a fallos.
- **OE-5.** Construir la **capa de *ingress*** en dos modos (cliente nativo directo al líder + proxy/
  REST), con terminación TLS, *rate limiting*, *circuit breaker*, *health checks* y *load balancing*.
- **OE-6.** Proporcionar **observabilidad** de grado producción (métricas Prometheus, logs
  estructurados, *tracing*) y herramientas de administración (API REST + CLI).
- **OE-7.** Demostrar, con **benchmarks reproducibles y metodológicamente rigurosos**, el impacto de
  las técnicas de alto rendimiento aplicadas (throughput y latencia con percentiles, sin
  *coordinated omission*).

## 2.4 Alcance funcional

El sistema se organiza en seis bloques funcionales:

1. **Ingress Layer** — proxy/gateway en dos modos: TLS 1.3, *rate limiting* (*token bucket*),
   *circuit breaker*, *health checks*, *load balancing*, *retry* con *backoff* + *jitter*, y gateway
   REST.
2. **Core Broker** — *topics*, particiones (ancladas a núcleo), grupos de consumidores, gestión de
   *offsets*, semánticas de entrega, DLQ, TTL, *batching*/compresión, *backpressure* por créditos.
3. **Consenso y replicación** — Raft por partición: elección de líder, replicación de log, *failover*
   automático, *high-watermark* derivado del `commitIndex` de Raft.
4. **Protocolo de red** — *framing*, *multiplexing*, versionado; librería cliente C++ nativa +
   gateway REST.
5. **Storage engine** — WAL, segmentos, índice disperso, *mmap*/io_uring, retención y compactación.
6. **Admin & observabilidad** — API REST, CLI (`nexus-cli`), métricas Prometheus, logs JSON,
   *tracing*.

### Requisitos funcionales (selección)

- **RF-01.** Un productor publica mensajes en un *topic*/partición y recibe confirmación con el
  *offset* asignado y la política de *ack* solicitada.
- **RF-02.** Un consumidor lee desde un *offset* arbitrario y avanza (*commit* manual y automático).
- **RF-03.** Los mensajes persisten de forma durable y sobreviven a un reinicio del broker.
- **RF-04.** Los grupos de consumidores reparten particiones entre miembros con rebalanceo.
- **RF-05.** El sistema replica cada partición (grupo Raft) y elige un nuevo líder automáticamente
  ante la caída del actual, **sin pérdida de datos *committed***.
- **RF-06.** La capa de *ingress* aplica *rate limiting* y *circuit breaking* por cliente/*topic*, y
  ofrece un gateway REST.
- **RF-07.** Operación vía API REST y CLI; métricas expuestas en formato Prometheus.

### Requisitos no funcionales (cualitativos)

Las cifras objetivo (throughput, latencias por percentil, retención) son orientativas y se fijan con
la metodología del capítulo [Rendimiento y benchmarks](./23-rendimiento-y-benchmarks.md); las cifras
reales viven en [`benchmarks.md`](../benchmarks.md). Los requisitos cualitativos son:

- **Durabilidad / ACK.** Modelo de *ack* configurable por petición: `acks=0` (sin esperar,
  *at-most-once*), `acks=1` (escrito en el líder) y `acks=quorum` (replicado y *committed* por la
  mayoría del grupo Raft; **por defecto**). Ningún mensaje confirmado con `acks=quorum` se pierde
  mientras sobreviva la mayoría del grupo. La política de `fsync` (cada N mensajes / N ms / por
  *commit*) es ortogonal y combinable.
- **Consistencia / Disponibilidad.** Postura **CP** ([ADR-0007](../adr/adr-0007-consistencia-cp-pacelc.md)):
  ante partición de red, una partición sin quórum **deja de aceptar escrituras** antes que arriesgar
  divergencia. *Failover* automático sin pérdida de datos *committed* por el quórum.
- **Seguridad.** TLS 1.3 en el borde (terminación en *ingress*) y opción de mTLS intra-cluster;
  autenticación de cliente y autorización básica por *topic*.
- **Observabilidad.** Métricas, logs estructurados y *tracing* con *correlation IDs*.
- **Portabilidad.** Núcleo independiente de plataforma tras la abstracción de I/O (*proactor*); Linux
  primero, Windows después.
- **Mantenibilidad.** Separación estricta plano de datos / plano de control; cobertura de pruebas en
  la lógica de dominio.

## 2.5 Fuera de alcance (explícito)

Para acotar el proyecto y evitar la dispersión —el riesgo número uno de un proyecto "infinito"—,
**quedan fuera** de forma deliberada:

- **Compatibilidad total con el protocolo de Kafka.** Se aborda solo un **subconjunto**
  (`ApiVersions` / `Metadata` / `Produce` / `Fetch`) como *stretch* de Fase 4, suficiente para que
  `kcat` hable con el broker; sin grupos de consumidores Kafka (`JoinGroup`/`Heartbeat`) ni
  transacciones. Ver [ADR-0004](../adr/adr-0004-protocolo-binario-propio-gateway-rest.md) y
  [ADR-0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md).
- **Exactly-once transaccional entre particiones.** Sí se contempla **productor idempotente**
  (*effectively-once* por partición).
- **Tiered storage** a almacenamiento de objetos (idea futura, inspirada en Pulsar).
- **Multi-tenancy y ACLs avanzadas.** (El **cifrado en reposo** del log **sí** está implementado —
  AES-256-GCM opcional, [ADR-0031](../adr/adr-0031-cifrado-en-reposo-aes-gcm.md); lo que queda fuera
  es la rotación de la clave maestra.)
- **Dashboard gráfico propio.** La observabilidad se expone vía Prometheus/CLI; un panel Grafana es
  opcional y *self-hosted*.
- **Despliegue de pago en cloud.** El proyecto se **orienta** a Azure/AWS por diseño (imagen Docker,
  configuración 12-factor), pero su desarrollo, prueba y *benchmark* **no requieren** gasto en cloud:
  el cluster de 3 nodos se levanta en local con Docker Compose. Ver
  [ADR-0008](../adr/adr-0008-coste-cero.md).

El detalle de las limitaciones aceptadas y la evolución posible se trata en el capítulo
[Limitaciones y trabajo futuro](./30-limitaciones-y-trabajo-futuro.md).
