# Diagrama 19: Ingress en dos modos — nativo directo vs proxy

El *ingress* de NexusMQ soporta **dos modos** con una jerarquía explícita (ADR-0006): el **nativo
directo** (primario) y el **proxy** (secundario, *opt-in*, cableado en el plano de datos por
ADR-0027). El nativo prioriza el rendimiento; el proxy, la conveniencia para clientes "tontos" y
HTTP, asumiendo a sabiendas un salto extra y la ruptura del *zero-copy*. Fuentes:
[`../adr/adr-0006-ingress-dos-modos.md`](../adr/adr-0006-ingress-dos-modos.md),
[`../adr/adr-0027-modo-proxy-upstream-pool.md`](../adr/adr-0027-modo-proxy-upstream-pool.md),
`src/ingress/proxy.hpp`, `src/ingress/load_balancer.hpp`, `src/ingress/upstream_pool.hpp`.

## Modo nativo directo (primario)

El *smart-client* conoce la topología: pide *metadata*, descubre el **líder** de cada partición y le
habla **directamente** por el plano de datos. Sin proxy, sin salto extra, sin romper el *zero-copy*.

```mermaid
graph LR
    SC["Smart-client<br/>(nexus-client)"]

    subgraph cluster["Clúster NexusMQ (plano de datos, :9092)"]
        direction TB
        N1["nexusd nodo 1<br/>líder de P0"]
        N2["nexusd nodo 2<br/>líder de P1"]
        N3["nexusd nodo 3<br/>líder de P2"]
    end

    SC -->|"1. Metadata<br/>(descubre líderes)"| N1
    SC -.->|"2a. Produce/Fetch P0<br/>(framed, correlation_id)"| N1
    SC -.->|"2b. Produce/Fetch P1"| N2
    SC -.->|"2c. Produce/Fetch P2"| N3
```

- El cliente **gestiona la *metadata*** y el descubrimiento de líder (coste asumido en el cliente).
- Camino caliente sin intermediarios: máxima latencia/throughput por defecto.

## Modo proxy (secundario, opt-in)

El cliente "tonto" no conoce la topología: se conecta al *ingress*, que **enruta** su tramo por
*consistent-hashing* (`LoadBalancer`, anillo FNV-1a con *vnodes*) y **releva** sus tramas
(petición/respuesta a nivel de trama, `Proxy::forward`) a una conexión obtenida del `UpstreamPool`.
Cada reactor tiene su propio *pool* (REACTOR-LOCAL, sin locks, *shared-nothing*).

```mermaid
graph TB
    DC["Cliente tonto<br/>(o gateway REST)"]

    subgraph reactor["Reactor del ingress (REACTOR-LOCAL)"]
        direction TB
        PX["Proxy<br/>route + forward"]
        LB["LoadBalancer<br/>(ConsistentHashing<br/>anillo FNV-1a, vnodes)"]
        UP["UpstreamPool<br/>(free-list por NodeId,<br/>préstamo exclusivo)"]
        PD["PeerDirectory<br/>(plano de datos:<br/>NodeId → host:puerto)"]
    end

    subgraph cluster["Clúster NexusMQ (plano de datos, :9092)"]
        direction TB
        N1["nexusd nodo 1"]
        N2["nexusd nodo 2"]
        N3["nexusd nodo 3"]
    end

    DC -->|"1. conexión + trama"| PX
    PX -->|"2. route(key)"| LB
    LB -->|"3. NodeId elegido"| PX
    PX -->|"4. acquire(node)"| UP
    UP -->|"5. resuelve dirección"| PD
    UP -.->|"6a. reúsa ociosa<br/>o async_connect"| N2
    PX -.->|"7. forward (relevo<br/>petición/respuesta)"| N2
    PX -->|"8. release(node, socket)<br/>al cerrar limpio"| UP
```

- **`route`**: `Proxy::route(key)` delega en `LoadBalancer::pick` con *consistent-hashing* (mínima
  perturbación al añadir/quitar nodos); devuelve `nullopt` si el anillo está vacío.
- **`acquire`/`release`**: el `UpstreamPool` entrega una conexión **en préstamo exclusivo** (reúsa
  una ociosa de la *free-list* del nodo o **diala** una nueva con `Socket::async_connect`, sin
  congelar el reactor) y la recupera al cerrar el cliente; la *free-list* está **acotada**
  (`max_idle_per_node`, def. 8) para no fugar descriptores.
- **Préstamo exclusivo**: garantiza que **no se intercalan** tramas de dos clientes en la misma
  conexión aguas arriba (corrección del relevo).
- **Dos `PeerDirectory` distintos**: el del plano de datos (este) es una **instancia separada** del
  de Raft (ADR-0025); mismo tipo, distinta instancia.

## Trade-offs (por qué dos modos, no uno)

| Aspecto | Nativo directo | Proxy |
| --- | --- | --- |
| Camino | cliente → líder | cliente → ingress → nodo |
| Saltos extra | 0 | 1 (asumido) |
| *Zero-copy* | sí | se rompe (copia en el relevo) |
| Cliente | *smart* (gestiona metadata) | *tonto* / HTTP |
| Activación | por defecto | *opt-in* |
