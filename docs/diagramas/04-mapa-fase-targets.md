# Diagrama 4: Mapa fase → targets

Qué librerías y ejecutables se entregan en cada fase del plan de desarrollo. Las fases son incrementales: cada una se apoya en lo construido antes. Estado as-built: las fases 1→4 están implementadas, con el cierre 4b (Windows, deuda diferida y benchmarks).

## Tabla resumen

| Fase | Foco | Targets entregados |
|---|---|---|
| **1** | Monohilo + I/O bloqueante | `nexus-common`, `nexus-storage` (I/O bloqueante), `nexus-bench`, `nexus-tests` (unit/property/crash) |
| **1b** | Reactor + mono-nodo | `nexus-io` (proactor), `nexus-reactor`, `nexus-protocol`, `nexus-wire`, `nexus-broker` (mono-nodo), `nexus-client`, `nexus-server` (mono-nodo); tests de integración |
| **2** | Distribuido (Raft) | `nexus-consensus`, `nexus-cluster`, broker distribuido (grupos, rebalanceo); tests sim/chaos |
| **3** | Ingress + observabilidad | `nexus-ingress`, `nexus-telemetry`, `nexus-cli`, deploy/ (Docker, Prometheus, Grafana, k8s) |
| **4** | Stretch | direct I/O, `nexus-kafka` (subset), `nexus-ffi` (binding Python), backend IOCP (Windows) |
| **4b** | Cierre | bloque W (`nexusd` Windows completo), bloque D (deuda diferida), bloque L (`nexus-loadgen` + benchmarks) |

## Diagrama

```mermaid
graph LR
    f1["Fase 1<br/>monohilo + I/O bloqueante"]
    f1b["Fase 1b<br/>reactor + mono-nodo"]
    f2["Fase 2<br/>distribuido (Raft)"]
    f3["Fase 3<br/>ingress + observabilidad"]
    f4["Fase 4<br/>stretch"]
    f4b["Fase 4b<br/>cierre"]

    f1 --> f1b --> f2 --> f3 --> f4 --> f4b

    f1 --> t1["nexus-common<br/>nexus-storage (bloqueante)<br/>nexus-bench<br/>nexus-tests"]
    f1b --> t1b["nexus-io (proactor)<br/>nexus-reactor<br/>nexus-protocol<br/>nexus-wire<br/>nexus-broker (mono-nodo)<br/>nexus-client<br/>nexus-server"]
    f2 --> t2["nexus-consensus<br/>nexus-cluster<br/>broker distribuido"]
    f3 --> t3["nexus-ingress<br/>nexus-telemetry<br/>nexus-cli<br/>deploy/"]
    f4 --> t4["nexus-kafka (subset)<br/>nexus-ffi (Python)<br/>direct I/O<br/>backend IOCP (Windows)"]
    f4b --> t4b["nexusd Windows completo (W)<br/>deuda diferida (D)<br/>nexus-loadgen + benchmarks (L)"]

    classDef fase fill:#1f6feb,stroke:#0b2a5b,color:#fff;
    classDef targets fill:#161b22,stroke:#30363d,color:#fff;
    class f1,f1b,f2,f3,f4,f4b fase;
    class t1,t1b,t2,t3,t4,t4b targets;
```
