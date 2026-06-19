# Despliegue de NexusMQ

Artefactos de empaquetado y orquestación. El broker expone dos puertos:

| Puerto | Rol | Endpoints |
| ------ | --- | --------- |
| `9092` | Plano de datos | protocolo binario (framed, `correlation_id`) |
| `9644` | Operación | `/healthz` (liveness), `/readyz` (readiness), `/metrics` (Prometheus), `/api/v1/...` (REST admin) |

> El kernel del host debe soportar **io_uring** (el broker usa el backend io_uring directo).

## Imagen Docker

Build multi-stage (Ubuntu para compilar → **distroless** `cc`, no-root) desde la raíz del repo:

```bash
docker build -f deploy/Dockerfile -t nexusmq:dev .
docker run --rm -p 9092:9092 -p 9644:9644 nexusmq:dev
```

El `HEALTHCHECK` reutiliza `nexus-cli diagnostics` (distroless no trae shell ni `curl`): el
contenedor pasa a *healthy* solo cuando `/healthz` y `/readyz` responden OK.

## docker compose (3 nodos + Prometheus + Grafana)

```bash
cd deploy
docker compose up --build
```

- Nodos: `localhost:9092/9644` (nexus1), `:9093/9645` (nexus2), `:9094/9646` (nexus3).
- Prometheus: <http://localhost:9090> (raspa `/metrics` de los tres por su nombre de servicio).
- Grafana: <http://localhost:3000> (login anónimo; datasource Prometheus provisionado).

En Fase 3 los nodos son brokers **independientes** (el clúster multi-nodo con replicación entre
nodos llega después); el compose valida empaquetado, observabilidad y *probes*.

## Kubernetes

`StatefulSet` (estado persistente → un PVC por réplica) + `Service` headless. Probes sobre el
puerto de operación: **liveness** `/healthz`, **readiness** `/readyz`.

```bash
# La imagen debe estar disponible para el clúster, p. ej. con kind:
kind load docker-image nexusmq:dev
kubectl apply -f deploy/k8s/
kubectl rollout status statefulset/nexusmq
```

Contenedor endurecido: `runAsNonRoot`, `readOnlyRootFilesystem`, `drop: [ALL]`,
`allowPrivilegeEscalation: false`.
