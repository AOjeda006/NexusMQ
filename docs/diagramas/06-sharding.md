# Diagrama 6: Sharding del plano de datos por núcleo

Cómo se reparte el estado entre los N reactores sin compartir nada mutable (ADR-0026, sobre ADR-0005). Cada pieza de estado tiene un **dueño único**, lo que la hace linealizable sin locks: un único núcleo serializa todas las operaciones sobre ella. Dos reglas de ubicación gobiernan el plano de datos:

- **Partición → núcleo dueño = `partition % N`** (lo fija `PartitionRouter::owner_core`, la misma regla que `ReactorPool::reactor_for`). El log y la pila Raft de cada partición viven **solo** en su reactor dueño; el resto de núcleos ni la instancian.
- **Grupo de consumidores → núcleo coordinador = `hash(group_id) % N`** con `hash` = **FNV-1a** sobre los bytes del `group_id` (contrato interno de ubicación, no viaja por *wire*). La membresía (`GroupCoordinator`) y los offsets confirmados (`OffsetManager`) viven en un solo núcleo, el coordinador del grupo.

Si la conexión la atiende un núcleo distinto del dueño/coordinador, la operación se enruta con `call_on` (petición/respuesta cross-core por el buzón SPSC); si coincide, es un *fast-path* local sin salto. Los metadatos de topics son la excepción: inmutables entre cambios y replicados por valor a cada núcleo, se validan localmente sin cross-core.

```mermaid
flowchart TB
    req["Petición entrante<br/>(la atiende el reactor de la conexión)"]
    kind{"¿Sobre partición<br/>o sobre grupo?"}
    req --> kind

    kind -->|"partición P (Produce / Fetch)"| pf["owner_core(P) = P % N<br/>(PartitionRouter)"]
    kind -->|"grupo G (Join / Sync / Heartbeat / OffsetCommit)"| gf["coordinator(G) = FNV-1a(group_id) % N"]

    pf --> route1["route / call_on al reactor dueño"]
    gf --> route2["call_on al reactor coordinador"]

    route1 --> owner["Reactor dueño de P<br/>único dueño del log + Raft de P<br/>serializa sin locks"]
    route2 --> coord["Reactor coordinador de G<br/>único dueño de membresía + offsets<br/>serializa sin locks"]

    owner --> resume["Reanuda al llamante con el resultado<br/>(local si coincide el núcleo; si no, cross-core SPSC)"]
    coord --> resume

    meta["Metadatos de topics<br/>INMUTABLE, replicados por valor a cada núcleo<br/>→ validación y Metadata locales, sin cross-core"]

    classDef start fill:#1f6feb,stroke:#0b2a5b,color:#fff;
    classDef decision fill:#9e6a03,stroke:#5a3d02,color:#fff;
    classDef map fill:#161b22,stroke:#30363d,color:#fff;
    classDef owner fill:#238636,stroke:#0b3d1a,color:#fff;
    classDef meta fill:#8957e5,stroke:#3b1f6b,color:#fff;
    class req start;
    class kind decision;
    class pf,gf,route1,route2,resume map;
    class owner,coord owner;
    class meta meta;
```

> El reparto por `partition % N` y por `FNV-1a(group_id) % N` da a cada shard un dueño único: no hay un `TopicManager` ni un coordinador global con lock, sino confinamiento por reactor. El coste es que tocar una partición o un grupo de otro núcleo paga un salto cross-core (ver [topología](./05-topologia-thread-per-core.md) y [secuencia del proactor](./07-secuencia-proactor.md)).
