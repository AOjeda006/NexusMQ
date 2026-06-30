# Diagrama 3: Grafo de dependencias de targets

Dependencias internas entre las 15 librerías `nexus-*`, los ejecutables (`nexusd`, `nexus-cli`, `nexus-bench`, `nexus-loadgen`) y la librería compartida `nexus-ffi`. Refleja el grafo real de dependencias del proyecto: las capas suben de abajo (`nexus-common`, sin dependencias internas) hacia arriba (`nexus-server`/`nexus-client`). Regla forzada en CMake: un target nunca depende de otro de su misma capa o superior. Una flecha `A --> B` significa "A depende de B".

```mermaid
graph TD
    %% --- Librerias nexus-* ---
    common["nexus-common"]
    io["nexus-io"]
    protocol["nexus-protocol"]
    telemetry["nexus-telemetry"]
    kafka["nexus-kafka"]
    wire["nexus-wire (INTERFACE)"]
    reactor["nexus-reactor"]
    storage["nexus-storage"]
    consensus["nexus-consensus"]
    cluster["nexus-cluster"]
    broker["nexus-broker"]
    ingress["nexus-ingress"]
    client["nexus-client"]
    ffi["nexus-ffi (SHARED)"]
    server["nexus-server"]

    %% --- Ejecutables / tools ---
    nexusd["nexusd (exe)"]
    cli["nexus-cli (exe)"]
    bench["nexus-bench (exe)"]
    loadgen["nexus-loadgen (exe)"]

    %% --- Dependencias ---
    io --> common
    protocol --> common
    telemetry --> common
    kafka --> common

    wire --> common
    wire --> io
    wire --> protocol

    reactor --> common
    reactor --> io

    storage --> common
    storage --> io

    consensus --> common
    consensus --> io
    consensus --> protocol
    consensus --> storage
    consensus --> telemetry

    cluster --> common
    cluster --> io
    cluster --> protocol
    cluster --> consensus

    broker --> common
    broker --> protocol
    broker --> reactor
    broker --> storage
    broker --> consensus
    broker --> telemetry

    ingress --> common
    ingress --> io
    ingress --> wire
    ingress --> cluster

    client --> common
    client --> io
    client --> protocol

    ffi --> common
    ffi --> telemetry

    server --> common
    server --> io
    server --> reactor
    server --> wire
    server --> broker
    server --> cluster
    server --> ingress
    server --> kafka
    server --> telemetry

    nexusd --> server
    cli --> common
    cli --> ingress
    bench --> common
    bench --> storage
    loadgen --> common
    loadgen --> client

    classDef base fill:#0b2a5b,stroke:#091d3f,color:#fff;
    classDef exe fill:#1a7f37,stroke:#0d401c,color:#fff;
    classDef shared fill:#6e40c9,stroke:#3c2270,color:#fff;
    class common base;
    class nexusd,cli,bench,loadgen exe;
    class ffi shared;
```
