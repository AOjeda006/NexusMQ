# 25. Despliegue

> Cómo se empaqueta y se levanta NexusMQ: imagen Docker reproducible, un cluster local con
> docker-compose y un despliegue con estado en Kubernetes. Los artefactos reales viven en
> [`deploy/`](../../deploy/).

## 25.1 Imagen Docker

La imagen sigue las buenas prácticas (`deploy/Dockerfile`):

- **Build multi-stage:** una etapa compila con el *toolchain* C++ y otra ejecuta sobre una
  **base mínima/distroless**, copiando solo el binario.
- **Usuario no-root** (UID/GID `65532`, *distroless nonroot*): limita el impacto de una intrusión.
- **`HEALTHCHECK`** cableado a `/readyz` (el plano de operación): el runtime reinicia instancias
  que no responden de verdad.
- **Tags de versión explícitos** (no `latest` en producción), `.dockerignore` para no copiar
  artefactos de build, y **secretos por variables de entorno**, nunca horneados.
- Se construye **una sola vez** y se promueve el mismo binario entre entornos (ver
  [capítulo 22](./22-puerta-de-calidad-y-cicd.md)).

## 25.2 Cluster local con docker-compose

`deploy/docker-compose.yml` levanta el entorno de referencia en una sola máquina (ver
[diagrama 21](../diagramas/21-despliegue-docker-compose.md)):

- **3 nodos** `nexus1`/`nexus2`/`nexus3` (imagen `nexusmq:dev`), cada uno con `--port 9092`
  (plano de datos) y `--admin-port 9644` (operación), `--data-dir /var/lib/nexusmq`,
  `--node-id` y un *topic* de demo (`--topic demo:3`). Los puertos se exponen sin solapar
  (`9092/9093/9094` datos; `9644/9645/9646` operación) y cada nodo tiene su **volumen**
  persistente.
- **Prometheus** (`:9090`) scrapea `/metrics` de los tres nodos.
- **Grafana** (`:3000`) con *datasource* Prometheus aprovisionado.

> El cluster de 3 nodos es suficiente para Raft, quórum y *failover*
> ([ADR-0008](../adr/adr-0008-coste-cero.md)). El runtime de cluster multi-nodo
> (descubrimiento de líder, replicación entre nodos) llegó en fases posteriores; el compose
> sirve además para validar el empaquetado, la observabilidad y los *probes* sobre varias
> instancias.

## 25.3 Kubernetes

`deploy/k8s/` despliega NexusMQ como **StatefulSet** —el broker tiene estado persistente (los
logs de partición)— con `volumeClaimTemplates` (un PVC por réplica) y un **Service headless**
para DNS estable (`nexusmq-0.nexusmq`, etc.), que es como las réplicas se descubren entre sí
(ver [diagrama 22](../diagramas/22-despliegue-kubernetes.md)). Detalles:

- **3 réplicas**, `securityContext` no-root (`runAsUser/Group/fsGroup 65532`).
- **Probes** *liveness*/*readiness* contra el puerto de operación (`/healthz`, `/readyz`).
- Se asumen **límites de CPU/memoria** por pod y un nodo con soporte de **io_uring**.

`docker-compose` es para local/dev; Kubernetes, cuando se necesita escala, *self-healing* y
orquestación real.

## 25.4 Cloud-ready, sin coste obligatorio

El diseño es *cloud-ready* (imagen Docker, configuración 12-factor) de modo que el **mismo
binario** corre en local o en cloud sin cambios. Una demo desplegada es cubrible con *free
tiers* (AWS/Azure/Oracle Always Free) bajo demanda, pero **no es necesaria** para desarrollar,
probar ni medir, que se hacen en local sin coste
([ADR-0008](../adr/adr-0008-coste-cero.md)).
