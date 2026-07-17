# ADR-0040: Topología Raft visible en single-node (`GET /api/v1/cluster` puebla `partitions[]` con RF=1)

- **Estado:** aceptado
- **Fecha:** 2026-07-17

## Contexto

[ADR-0035](adr-0035-estado-cluster-raft-rest-admin.md) expone el estado de clúster/Raft por
`GET /api/v1/cluster`, con un array `partitions[]` construido a partir de los **portadores de Raft**
(`RaftCarrier::observe`) de cada partición replicada. La consola web dibuja la topología a partir de
ese array.

En la **única topología hoy arrancable** —un `nexusd` aislado, con topics de `replication_factor = 1`
(la del `docker-compose`, `demo:3`)— las particiones **no están respaldadas por Raft**: son
`Partition` mono-nodo, sin portador. Por eso `describe_cluster` recorría una lista de portadores
**vacía** y devolvía `partitions: []`. Resultado en la prueba E2E con la consola: la vista de
topología Raft salía **vacía** aunque el nodo tuviera sus tres particiones perfectamente vivas y
sirviendo tráfico. La información existía (offsets del log, épocas) pero no se exponía.

El **clúster multinodo real** (config de peers, `RF≥2`, replicación entre procesos) es un hito
aparte ([ver capítulo 25](../tecnica/25-despliegue.md)); esta decisión es solo de **visibilidad**: no
cambia el consenso ni el esquema del DTO.

## Decisión

`describe_cluster` puebla `partitions[]` **también** para las particiones locales **no replicadas**,
sintetizando su estado como el de un **líder estático** (el único nodo lo es de todas sus
particiones), con datos honestos derivados del log local:

1. **Unificación en el borde.** Una función `observe_partitions(manager, node_id)` —que corre en el
   **núcleo dueño** de las particiones (ADR-0026)— devuelve los DTOs de **todas** las particiones que
   atiende ese `TopicManager`: las **replicadas** desde su `RaftCarrier` (camino anterior, intacto) y
   las **no replicadas** sintetizadas. Se recorren las particiones declaradas de cada topic y se
   sintetiza solo lo que vive en ese núcleo y **no** tiene portador de Raft, evitando duplicados.

2. **Mapeo honesto single-node → DTO** (`to_single_node_info`):
   - `leader = node_id` (este nodo es el líder de todas sus particiones), `role = "leader"`.
   - `commit_index = last_log_index = high_watermark`: con *ack* local, todo lo escrito está
     confirmado y es visible, así que el high-watermark es a la vez el índice confirmado y el último
     del log (respeta el invariante `commit_index ≤ last_log_index`).
   - `leader_epoch = Partition::leader_epoch()` (época real de la partición).
   - `term = 0`: **no hubo elección de Raft** en mono-nodo; 0 es honesto (no un término inventado).
   - `followers: []`: no hay réplicas.

3. **Sin cambios de esquema ni de contrato.** El DTO `PartitionRaftInfo` (ADR-0035) ya tiene todos
   los campos; solo se rellenan en un caso que antes quedaba vacío. `GET /api/v1/cluster` sigue
   devolviendo la misma forma. No se toca el código de consenso.

## Consecuencias

- (+) La consola dibuja la topología del nodo y sus particiones **con datos reales** aunque no haya
  réplicas: deja de verse una vista vacía en la única topología hoy arrancable.
- (+) `commitIndex` de cada partición **coincide con el high-watermark** del `describe` del topic
  (misma fuente, el log local): la vista de topología y la de topics son coherentes.
- (+) Se **unifica** el recorrido replicado/no-replicado en un único punto del borde; añadir el
  multinodo real más adelante no requiere tocar la consola.
- (+) Cero cambios en el plano de consenso y en el esquema del DTO: es una decisión de visibilidad,
  reversible y de bajo riesgo.
- (−) Un `role: "leader"` con `term: 0` puede confundir a quien espere semántica estricta de Raft;
  se documenta que en mono-nodo es un **líder estático**, no electo. La distinción real (replicada vs
  estática) es observable: una partición replicada trae `term > 0` y, en el líder, `followers`.
- (−) La topología es **por nodo**: los tres `nexusd` del compose se ven cada uno a sí mismo, no como
  un clúster (por diseño; ver [capítulo 25](../tecnica/25-despliegue.md)).

## Alternativas consideradas

- **Dejar `partitions: []` en single-node.** Es lo que había: la consola no puede dibujar nada y el
  operador no ve sus particiones. Se descarta: la información existe y es útil.
- **Un DTO distinto para "particiones no replicadas".** Duplica el esquema y obliga a la consola a
  manejar dos formas para el mismo concepto (una partición y su líder). Se prefiere **rellenar el DTO
  existente** con valores honestos.
- **Inventar un `term` (p. ej. igualarlo a `leader_epoch`).** Mezcla dos conceptos distintos (época
  de liderazgo de la partición vs término de elección de Raft) y sugiere una elección que no ocurrió.
  `term = 0` es más honesto.
- **Fingir un portador de Raft de un solo miembro para la partición mono-nodo.** Arrastraría toda la
  maquinaria de consenso (elección, persistencia de estado) a un camino que no la necesita, con coste
  y complejidad reales. La síntesis en el borde es mucho más barata y no toca el hot-path.
