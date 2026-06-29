# Diagrama 2: Contenedores de un nodo (C4 nivel 2)

Vista interna de un proceso `nexusd`: los tres planos (datos/cliente, Raft inter-nodo y administración REST), el *reactor pool* thread-per-core que ejecuta la lógica del broker, y el almacenamiento append-only en disco. Cada reactor es dueño exclusivo de un subconjunto de réplicas de partición; la interacción entre núcleos es paso de mensajes (SPSC), nunca estado compartido.

```mermaid
graph TB
    client["Cliente nativo / Kafka"]
    admin["Administrador (CLI / REST)"]
    prom["Prometheus"]
    peers["Otros nodos<br/>(replicas Raft)"]

    subgraph nodo["Proceso nexusd (un nodo del cluster)"]
        direction TB

        subgraph planoCliente["Plano de datos / cliente"]
            ingress["Ingress<br/>(TLS 1.3, rate-limit,<br/>circuit-breaker, proxy)"]
            listener["Listeners + Connections<br/>(framing protocolo nativo y Kafka)"]
        end

        subgraph planoAdmin["Plano de administracion"]
            rest["REST Gateway / AdminApi<br/>(/api/v1, RFC 7807, JWT)"]
            metrics["MetricsRegistry<br/>(exposicion Prometheus /metrics)"]
        end

        subgraph nucleo["Reactor pool (thread-per-core, pinned)"]
            direction LR
            r0["Reactor core0<br/>proactor io_uring +<br/>allocator + particiones"]
            r1["Reactor core1<br/>proactor io_uring +<br/>allocator + particiones"]
            rn["Reactor coreN<br/>..."]
        end

        broker["Broker / Particiones<br/>(topics, grupos, offsets,<br/>idempotencia, creditos)"]
        raft["Consenso Raft por particion<br/>+ transporte inter-nodo (cluster)"]
    end

    disk[("Disco: log append-only<br/>segmentos .log + .index<br/>(el log Raft ES el WAL)")]

    client -->|TCP| ingress
    ingress --> listener
    listener -->|enruta al reactor dueno| nucleo
    admin -->|HTTPS| rest
    rest --> broker
    prom -->|HTTP GET| metrics

    nucleo --> broker
    broker --> raft
    broker -->|append / fetch| disk
    raft -->|replica el log| disk
    raft <-->|RPC Raft, mTLS| peers
    nucleo -. metricas .-> metrics

    classDef plane fill:#161b22,stroke:#30363d,color:#fff;
    classDef store fill:#5a4500,stroke:#3b2f00,color:#fff;
    classDef ext fill:#8b949e,stroke:#3b3f45,color:#fff;
    class disk store;
    class client,admin,prom,peers ext;
```
