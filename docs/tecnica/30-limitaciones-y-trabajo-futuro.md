# 30. Limitaciones y trabajo futuro

> Qué **no** hace NexusMQ (a propósito) y por dónde podría crecer. Acotar el alcance es
> una decisión de ingeniería: mantiene el proyecto profundo en una sola tesis
> arquitectónica en lugar de disperso. Base: el alcance declarado del proyecto y las
> consecuencias registradas en los ADR.

## 30.1 Limitaciones conscientes (fuera de alcance)

- **Compatibilidad total con Kafka.** Solo se implementa un **subconjunto**
  (`ApiVersions`/`Metadata`/`Produce`/`Fetch` y lo necesario para `kcat`), no las 70+ *API
  keys* versionadas ni el *consumer group protocol* completo
  ([ADR-0004](../adr/adr-0004-protocolo-binario-propio-gateway-rest.md),
  [ADR-0029](../adr/adr-0029-adaptador-kafka-async-cross-core.md)).
- **Transacciones en el subset Kafka y *exactly-once* de entrega literal.** El
  *exactly-once* multi-partición **sí** está implementado en la **superficie nativa**
  (transacciones con coordinador, marcadores de control, LSO y `read_committed`,
  [ADR-0033](../adr/adr-0033-exactly-once-nativo-transacciones.md) /
  [ADR-0034](../adr/adr-0034-2pc-logueado-recuperable.md)); es *effectively-once* **honesto**
  (deduplicación + visibilidad atómica), no una entrega *exactly-once* literal (inalcanzable
  ante fallos arbitrarios). Queda fuera: exponer transacciones en el **subset Kafka** (coherente
  con la partición mono-protocolo, ADR-0030).
- **Adaptador S3 real y offload asíncrono del tiered storage.** El **tiered storage sí** está
  implementado (puerto `StorageTier` + adaptador local, offload de segmentos sellados y rehidratación
  transparente, [ADR-0032](../adr/adr-0032-tiered-storage-puerto-y-tier-local.md)); lo que queda como
  trabajo futuro es un **adaptador S3** sobre el mismo puerto (tras `find_package`), la **descarga
  asíncrona** en un hilo de mantenimiento (hoy es síncrona en la rotación), una **caché de
  rehidratación** persistente (hoy se baja el segmento entero por lectura fría) y la recuperación
  ante **pérdida del disco local**.
- **Multi-tenancy y ACLs avanzadas** no se abordan en las fases actuales.
- **Rotación de la clave maestra (KEK) de cifrado en reposo.** El cifrado en reposo **sí** está
  implementado (AES-256-GCM opcional, [ADR-0031](../adr/adr-0031-cifrado-en-reposo-aes-gcm.md)); lo
  que queda como trabajo futuro es la **rotación** de la KEK (re-cifrado en caliente). El formato ya
  reserva *salt* e identificadores por segmento para soportarla.
- **Interfaz gráfica dentro del broker**: el broker **no sirve UI**. Expone la observabilidad por
  Prometheus/CLI y, para la consola, el snapshot JSON y el *stream* SSE del plano REST de operación
  (ver [capítulo 15](./15-api-rest-administracion.md)); un panel Grafana es opcional y *self-hosted*.
  La consola web vive en un **repositorio aparte** (*NexusMQ Console*), que consume esa API como
  cliente externo: mantener la UI fuera del broker evita acoplar el núcleo a un ciclo de vida de
  *frontend*.
- **HLC y ordenación causal entre particiones.** El orden es por **offset** dentro de una
  partición; no hay relojes lógicos híbridos entre particiones.

## 30.2 Deudas acotadas

- **Clúster multinodo entre procesos.** La maquinaria de consenso por partición existe y se valida
  con clústeres **simulados** (`replicated_partition`, `cluster_e2e`), pero `nexusd` no expone aún
  flags para configurar *peers*: cada proceso arranca aislado con `replication_factor=1` (incluidos
  los tres servicios del *compose*). El clúster real —*membership*, RF≥2, replicación entre
  procesos— es un hito posterior con su propio ADR (ver
  [capítulo 25](./25-despliegue.md), [ADR-0040](../adr/adr-0040-topologia-raft-single-node.md)).
- **Madurez del modo proxy.** El modo nativo (*smart-client* al líder) es el primario y el
  más optimizado; el modo proxy con *upstream pool*
  ([ADR-0027](../adr/adr-0027-modo-proxy-upstream-pool.md)) es secundario y opt-in.
- **Escala de muchos grupos Raft.** Raft por partición implica un grupo (y su *heartbeat*)
  por partición; no es problema a escala de portfolio, pero a escala de miles de
  particiones requeriría agrupación de *heartbeats* y rebalanceo más sofisticado
  ([ADR-0003](../adr/adr-0003-replicacion-raft-por-particion.md)).
- **Profundidad opcional de I/O.** *Direct I/O* (`O_DIRECT`) con caché/*readahead* propios
  quedó como profundidad medida, no como camino por defecto.

## 30.3 Líneas de evolución posibles

- **Ampliar el subset Kafka** hacia más *API keys* y versiones según demanda real de
  herramientas del ecosistema.
- **Adaptador S3 del tiered storage** sobre el puerto `StorageTier` ya existente (ADR-0032),
  más offload asíncrono y caché de rehidratación, para retención larga a bajo coste en la nube.
- **Lecturas desde *followers*** (*opt-in*, con garantías documentadas) para repartir la
  carga de lectura sin romper la postura CP.
- **Pre-vote / leadership transfer / learners** de Raft (de la tesis de Ongaro) para
  *failover* más suave y reconfiguración de membresía.
- **Optimización medida** del camino caliente (registered buffers/fixed files de io_uring,
  *allocators* NUMA-aware, *hugepages*) allí donde el *profiling* lo justifique.
- **Demo de despliegue en cloud** (el diseño ya es *cloud-ready*, 12-factor) con
  infraestructura como código, bajo demanda y sin coste recurrente
  ([ADR-0008](../adr/adr-0008-coste-cero.md)).
