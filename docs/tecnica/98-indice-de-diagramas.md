# Apéndice B. Índice de diagramas

> Referencia cruzada de los diagramas de NexusMQ. Las fuentes (Mermaid / tablas) viven en
> [`docs/diagramas/`](../diagramas/); el render a imagen para el PDF se genera a
> `docs/diagramas/img/`.

## Arquitectura y módulos

| # | Título | Tipo | Fichero |
|---|--------|------|---------|
| 1 | Contexto C4 — NexusMQ y sus actores | C4/graph | [01-contexto-c4.md](../diagramas/01-contexto-c4.md) |
| 2 | Contenedores C4 — `nexusd` y sus planos | C4/graph | [02-contenedores-c4.md](../diagramas/02-contenedores-c4.md) |
| 3 | Grafo de dependencias de las 15 librerías `nexus-*` | graph | [03-grafo-dependencias.md](../diagramas/03-grafo-dependencias.md) |
| 4 | Mapa fase→targets | graph/tabla | [04-mapa-fase-targets.md](../diagramas/04-mapa-fase-targets.md) |

## Runtime y concurrencia

| # | Título | Tipo | Fichero |
|---|--------|------|---------|
| 5 | Topología *thread-per-core* | flow | [05-topologia-thread-per-core.md](../diagramas/05-topologia-thread-per-core.md) |
| 6 | Sharding partición→núcleo y grupo→hash | flow | [06-sharding.md](../diagramas/06-sharding.md) |
| 7 | Secuencia del proactor (`submit`→`completion`) | sequence | [07-secuencia-proactor.md](../diagramas/07-secuencia-proactor.md) |

## Almacenamiento

| # | Título | Tipo | Fichero |
|---|--------|------|---------|
| 8 | Layout del log (segmentos `.log`/`.index`, RecordBatch v2) | tabla/ASCII | [08-layout-log.md](../diagramas/08-layout-log.md) |
| 9 | Retención y compactación | flow | [09-retencion-compactacion.md](../diagramas/09-retencion-compactacion.md) |
| 24 | Almacenamiento por niveles (tiered storage) | graph/sequence | [24-tiered-storage.md](../diagramas/24-tiered-storage.md) |

## Consenso (Raft por partición)

| # | Título | Tipo | Fichero |
|---|--------|------|---------|
| 10 | Estados de un nodo Raft | state | [10-estados-raft.md](../diagramas/10-estados-raft.md) |
| 11 | Replicación + commit | sequence | [11-replicacion-commit.md](../diagramas/11-replicacion-commit.md) |
| 12 | Failover / elección de líder | sequence | [12-failover-eleccion-lider.md](../diagramas/12-failover-eleccion-lider.md) |
| 13 | Dos espacios de coordenadas (índice vs offset) | diagram | [13-espacios-coordenadas.md](../diagramas/13-espacios-coordenadas.md) |
| 14 | Snapshot / InstallSnapshot | sequence | [14-snapshot-installsnapshot.md](../diagramas/14-snapshot-installsnapshot.md) |
| 15 | Planos de red (cliente vs inter-nodo Raft) | flow | [15-planos-red.md](../diagramas/15-planos-red.md) |

## Protocolos

| # | Título | Tipo | Fichero |
|---|--------|------|---------|
| 16 | Frame nativo (cabecera) | tabla/ASCII | [16-frame-nativo.md](../diagramas/16-frame-nativo.md) |
| 17 | Petición/respuesta Kafka | tabla/sequence | [17-peticion-respuesta-kafka.md](../diagramas/17-peticion-respuesta-kafka.md) |
| 18 | Flujo REST admin + JWT | sequence | [18-flujo-rest-jwt.md](../diagramas/18-flujo-rest-jwt.md) |

## Ingress / seguridad

| # | Título | Tipo | Fichero |
|---|--------|------|---------|
| 19 | Modo nativo vs proxy | flow | [19-modo-nativo-vs-proxy.md](../diagramas/19-modo-nativo-vs-proxy.md) |
| 20 | Puente TLS de BIOs sobre el proactor | sequence | [20-puente-tls-bios.md](../diagramas/20-puente-tls-bios.md) |

## Operación

| # | Título | Tipo | Fichero |
|---|--------|------|---------|
| 21 | Despliegue docker-compose (3 nodos + Prometheus + Grafana) | flow | [21-despliegue-docker-compose.md](../diagramas/21-despliegue-docker-compose.md) |
| 22 | Despliegue Kubernetes (StatefulSet + Service) | flow | [22-despliegue-kubernetes.md](../diagramas/22-despliegue-kubernetes.md) |
| 23 | Pipeline de observabilidad | flow | [23-pipeline-observabilidad.md](../diagramas/23-pipeline-observabilidad.md) |
