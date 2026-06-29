# Diagrama 1: Contexto del sistema (C4 nivel 1)

NexusMQ visto como una caja negra y los actores y sistemas externos que interactúan con él: productores y consumidores (cliente nativo y `kcat` por el subset Kafka), administradores (REST/CLI), Prometheus (scrape de `/metrics`) y los demás nodos del propio cluster.

```mermaid
graph TB
    subgraph clientes["Aplicaciones de cliente"]
        prod["Productor / Consumidor<br/>(nexus-client nativo)<br/>[protocolo binario propio]"]
        kcat["kcat / cliente Kafka<br/>[subset del protocolo Kafka]"]
    end

    admin["Administrador<br/>(persona via nexus-cli o REST)"]
    prom["Prometheus<br/>(sistema de observabilidad)"]

    nexus["NexusMQ<br/><br/>Broker de mensajeria distribuido<br/>shared-nothing thread-per-core<br/>Raft por particion"]

    peers["Otros nodos del cluster<br/>(replicas Raft de las particiones)"]

    prod -->|"produce / fetch / commit<br/>TCP, puerto nativo"| nexus
    kcat -->|"Produce / Fetch / Metadata<br/>TCP, --kafka-port"| nexus
    admin -->|"crear/borrar topics, diagnostico<br/>HTTPS REST /api/v1, Bearer JWT"| nexus
    nexus -->|"expone metricas<br/>HTTP GET /metrics"| prom
    nexus <-->|"AppendEntries / RequestVote /<br/>InstallSnapshot (Raft, mTLS)"| peers

    classDef system fill:#1f6feb,stroke:#0b2a5b,color:#fff;
    classDef external fill:#8b949e,stroke:#3b3f45,color:#fff;
    class nexus system;
    class prod,kcat,admin,prom,peers external;
```
