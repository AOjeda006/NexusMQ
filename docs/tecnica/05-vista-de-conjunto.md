# 5. Vista de conjunto

> El sistema a vista de pájaro: cómo se descompone NexusMQ en librerías, cómo fluyen sus
> dependencias y cómo se mapea al modelo C4. Es el mapa que sitúa los capítulos siguientes.

## 5.1 Un nodo, N reactores

Cada **nodo** del broker es un proceso (imagen Docker) que ejecuta **N reactores** (uno
por núcleo físico, *pinned*) y mantiene un subconjunto de **réplicas de partición** —líder
de unas, seguidor de otras—, donde **cada partición es su propio grupo Raft**. El cluster
de referencia son **3 nodos** (tolera la caída de uno con quórum de 2). En desarrollo, los
3 nodos se levantan con Docker Compose en una sola máquina (ver
[capítulo 25](./25-despliegue.md)). El contexto y los contenedores se ilustran en los
diagramas [1](../diagramas/01-contexto-c4.md) y [2](../diagramas/02-contenedores-c4.md).

## 5.2 Las 15 librerías `nexus-*`

La solución es un único árbol CMake con **15 librerías** `nexus-*`, más ejecutables y
herramientas. Por capas (de la base hacia arriba):

| Capa | Librerías | Responsabilidad |
| ---- | --------- | --------------- |
| Base | `nexus-common` | Tipos, `bytes`, `varint`, `crc32c`, `record`, `error`, `task` (corutinas), compresión. |
| Plataforma | `nexus-io`, `nexus-wire` | Proactor (io_uring/IOCP), `File`/`Socket`; framing sobre conexión. |
| Dominio E/S | `nexus-protocol`, `nexus-storage` | Protocolo binario (codec, frames, versionado); motor de log (segmentos, índice). |
| Ejecución | `nexus-reactor` | Reactor por núcleo, colas SPSC cross-core, `PartitionRouter`. |
| Consenso | `nexus-consensus`, `nexus-cluster` | Raft (FSM, log, RPC); transporte inter-nodo. |
| Broker | `nexus-broker`, `nexus-kafka` | Topics, particiones, grupos, offsets; subset Kafka. |
| Bordes | `nexus-ingress`, `nexus-telemetry` | Dos modos de ingress, TLS, REST admin; métricas/logs. |
| Cima | `nexus-server`, `nexus-client`, `nexus-ffi` | Composición de `nexusd`; cliente nativo; ABI C para bindings. |

Ejecutables y *tools*: `nexusd` (el servidor, `src/server/main.cpp`), `nexus-cli`,
`nexus-bench`, `nexus-loadgen` y `wincheck` (arnés Windows-only). El detalle por subsistema
está en el [capítulo 18](./18-catalogo-por-subsistema.md).

## 5.3 Regla de dependencia

Las dependencias **apuntan hacia el núcleo**: una librería nunca depende de otra de su
misma capa o superior. `nexus-common` no depende de nada interno; `nexus-server` está
arriba y compone casi todo. Esta disciplina (clean architecture aplicada a sistemas)
mantiene el núcleo —storage, broker, consenso— **independiente del detalle de I/O y de
protocolo**, que entran por *puertos* y *adaptadores* (DIP). El grafo completo está en el
[diagrama 3](../diagramas/03-grafo-dependencias.md) y se detalla en el
[capítulo 17](./17-mapa-de-modulos.md).

## 5.4 Modelo C4 (resumen)

- **Contexto:** productores y consumidores ↔ NexusMQ ↔ operadores (CLI/REST) y Prometheus;
  otros nodos del cluster por el plano Raft.
- **Contenedores:** ingress (dos modos), nodos de broker (N reactores *per-core* c/u),
  almacén en disco, plano de administración.
- **Componentes (por reactor):** *event loop* del proactor → *handlers* de protocolo
  (corutinas) → broker (particiones del núcleo) → storage engine → módulo Raft de esas
  particiones; colas SPSC *cross-core* hacia otros reactores.

## 5.5 Planos del sistema

NexusMQ separa explícitamente tres planos, lo que se refleja en su despliegue y en sus
puertos (ver [diagrama 15](../diagramas/15-planos-red.md)):

- **Plano de datos / cliente:** *produce*/*fetch* por el protocolo binario (o subset Kafka)
  contra el líder de cada partición. Es el camino caliente.
- **Plano de consenso / inter-nodo:** los RPC de Raft (`AppendEntries`/`RequestVote`) entre
  réplicas, por un puerto separado.
- **Plano de control / administración:** API REST (`/api/v1`), salud y métricas.

La separación plano de datos/plano de control es un principio rector: el camino caliente se
mantiene libre de lógica de administración (ver [capítulo 6](./06-principios-de-diseno.md)).
