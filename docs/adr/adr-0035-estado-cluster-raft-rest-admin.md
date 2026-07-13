# ADR-0035: Estado de clúster/Raft por la superficie REST admin (`GET /api/v1/cluster`)

- **Estado:** aceptado
- **Fecha:** 2026-07-13

## Contexto

La consola de administración (repositorio aparte: React SPA + BFF NestJS) necesita observar el
estado del **clúster** y del **consenso** por partición: qué nodos hay, quién es líder de cada
partición replicada, término, *commit-index* (= *high-watermark* de la réplica), último índice del
log y el retraso de cada seguidor. Hasta ahora ese estado solo era visible de forma **agregada** por
`/metrics` (series Prometheus etiquetadas por `(topic, partition)`, ADR-0017): útil para *scraping*
pero incómodo para una vista estructurada por partición.

El estado observable de Raft ya existe como *getters* `const` en `RaftNode` (`role`, `current_term`,
`commit_index`, `last_log_index`, `leader_epoch`, `leader_hint`, `peers`, `match_index`), y cada
réplica la conduce un `RaftCarrier` **reactor-local** en el núcleo dueño de su partición
(`partition % N`, [ADR-0026](adr-0026-sharding-por-nucleo.md)). El `RaftNode` no es *thread-safe*: no
se puede leer desde el núcleo 0 (donde se sirve el admin) sin cruzar al núcleo dueño.

El problema de diseño es **cómo alcanzar** ese estado desde el plano de operación sin (a) romper el
confinamiento por núcleo, (b) usar `dynamic_cast` sobre `PartitionBase` para distinguir
`ReplicatedPartition` (convención del proyecto: sin RTTI descendente), ni (c) filtrar el `RaftNode`
al plano REST.

## Decisión

Se expone el estado de clúster/Raft por la superficie REST admin con `GET /api/v1/cluster`, sobre el
puerto `AdminService` ([ADR-0018](adr-0018-rest-admin-puerto-adaptador.md)):

1. **Accesor de solo lectura en `RaftCarrier`.** Se añade `RaftCarrier::observe() const`, que devuelve
   un `RaftObservation` (snapshot de valor: rol, término, *commit-index*, último índice, época, líder
   y progreso por peer). El portador ya posee la referencia al `RaftNode` y vive en el núcleo dueño;
   `observe()` solo lee *getters* `const`. No expone el `RaftNode` (encapsulación) ni requiere
   `dynamic_cast`: los portadores se alcanzan por `TopicManager::carriers()`, no por `PartitionBase`.

2. **`AdminService::describe_cluster()` + DTOs** (`ClusterInfo`, `NodeInfo`, `PartitionRaftInfo`,
   `FollowerProgress`) en `nexus-ingress`, planos y sin dependencia del broker/consensus (traducción
   en el borde, ADR-0009).

3. **Agregación cross-core** en el adaptador `AdminApi` con el patrón de `describe_topic`/
   `partition_info`: por cada núcleo, un `call_on` al reactor dueño observa sus portadores
   (`carriers()`) en su propio hilo y devuelve las observaciones; el núcleo 0 las traduce a DTOs y las
   ordena de forma determinista por `(topic, partición)`. La membresía de nodos sale del
   `PeerDirectory` (este nodo + peers), cableada en `AdminApi::bind_cluster`.

El *lag* por seguidor (`last_log_index − match_index`) solo se puebla en la réplica **líder** (donde
`match_index` es significativo). El endpoint es de **solo lectura** y se autentica como el resto de
`/api/v1` (Bearer JWT si el nodo arrancó con secreto).

## Consecuencias

- (+) La consola obtiene una vista **estructurada por partición** del consenso sin *scrapear* ni
  reconstruir desde series Prometheus.
- (+) Respeta el **confinamiento por núcleo**: el `RaftNode` solo se lee en su reactor dueño; el
  cruce es por `call_on` (mismo patrón ya probado en `describe_topic`).
- (+) **Sin `dynamic_cast`**: el estado se alcanza por los portadores (`carriers()`), no por
  reflexión de tipo sobre `PartitionBase`.
- (+) El `RaftNode` **no se filtra** al plano REST: `RaftObservation` es un DTO de valor.
- (−) `observe()` copia un pequeño snapshot por réplica en cada consulta (coste despreciable, plano
  de operación, no *hot path*).
- (−) `AdminService` gana un método; los dobles de test deben implementarlo.

## Alternativas consideradas

- **`dynamic_cast<ReplicatedPartition*>` sobre `PartitionBase`:** viola la convención del proyecto
  (sin RTTI descendente) y acopla el plano REST al tipo concreto de partición; descartado.
- **Métodos virtuales de observabilidad de Raft en `PartitionBase`:** contamina la interfaz de
  *hot-path* con detalles de plano de control y obliga a las particiones no replicadas a devolver
  valores centinela; descartado a favor de `RaftCarrier::observe()`.
- **Exponer `const RaftNode&` desde el portador:** filtra el tipo de consensus al plano REST y tienta
  a leerlo fuera de su núcleo; el snapshot de valor `RaftObservation` es más seguro.
- **Solo `/metrics`:** ya existe, pero no da una vista estructurada por partición ni el detalle de
  seguidores de forma cómoda para una consola.
