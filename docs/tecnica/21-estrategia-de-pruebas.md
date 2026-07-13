# 21. Estrategia de pruebas

> Cómo se gana confianza en NexusMQ: TDD como base, y un arsenal específico para código de
> sistemas, concurrente y distribuido. Las pruebas son **código de primera clase**; el
> no-determinismo se **inyecta**, no se tolera.

## 21.1 TDD y organización

El desarrollo sigue **TDD** (rojo→verde→refactor) sobre la lógica de dominio, con GoogleTest y
nombres `Metodo_Escenario_ResultadoEsperado`. La suite vive en `tests/`, organizada por tipo:

| Carpeta | Qué cubre |
| ------- | --------- |
| `tests/unit/` | Pruebas unitarias por subsistema (una carpeta por librería: `common`, `storage`, `reactor`, `consensus`, `protocol`, `broker`, `kafka`, `ingress`, `io`, `wire`, `telemetry`, `server`, `client`, `ffi`, `cli`, `bench`, `loadgen`, `cluster`). |
| `tests/sim/` | **Simulación determinista** de Raft (`raft_sim`). |
| `tests/crash/` | *Crash testing* (`partition_crash_test`). |
| `tests/e2e/` | Extremo a extremo: broker, cliente, cluster, admin HTTP, CLI, loadgen. |
| `tests/support/` | *Helpers*: `fake_proactor` (proactor virtual) y `test_certs`. |

## 21.2 Property-based y fuzzing

- **Property-based:** en serialización se afirman **invariantes** sobre entradas generadas
  —el *round-trip* `decode(encode(x)) == x` del `Record`/`RecordBatch` y del codec del
  protocolo—, con *shrinking* al contraejemplo mínimo.
- **Fuzzing:** los parsers de **entrada no confiable** (frames del protocolo, RecordBatch,
  subset Kafka) se alimentan con datos aleatorios/guiados por cobertura para descubrir
  *panics*, UB y desbordes. Es obligatorio en todo lo que parsea formato externo.

## 21.3 Simulación determinista (la pieza clave)

El núcleo de Raft es una **máquina de estados sin E/S**
([ADR-0015](../adr/adr-0015-raftnode-fsm-sin-io.md)), y eso es precisamente lo que permite
probarlo de forma **determinista**: en `tests/sim/` se inyecta un **reloj virtual** y una
**red virtual** (con `fake_proactor`), y se reproducen *timing*, elecciones de líder y
particiones de red de forma **repetible**. Así el consenso cumple **FIRST**
(determinismo/independencia) sin *flakiness*: una prueba *flaky* es un bug —de la prueba o del
diseño—, no algo tolerable. El no-determinismo se controla **inyectándolo**.

El mismo patrón cubre el **exactly-once transaccional**
([ADR-0033](../adr/adr-0033-exactly-once-nativo-transacciones.md)): `tests/sim/transaction_sim.hpp`
monta el `TransactionCoordinator` (FSM sin E/S) sobre particiones en memoria con reloj virtual y RNG
sembrado, y ejercita el 2PC de punta a punta —init/begin/produce/commit/abort → marcadores →
visibilidad `read_committed`— con **entrega parcial de marcadores** y **failover del coordinador**.
Los escenarios verifican las invariantes de atomicidad (un commit hace visible **toda** la data; un
abort, **ninguna**; una transacción abierta queda retenida por el LSO) e incluyen un **caos** de 300
transacciones con commit/abort aleatorios y failovers, donde al final se ve **exactamente** el
conjunto confirmado. Este arnés destapó y fijó un fallo real de *fencing* en el coordinador.

## 21.4 Crash y chaos

- **Crash testing** (`tests/crash/`): se mata el proceso a mitad de escritura (`kill -9`) y se
  verifica la **recuperación** —validación de CRC y truncado de la cola *torn*— y que no se
  pierden datos *committed*.
- **Chaos** (local, sin coste): particionar la red (`tc netem`), limitar recursos (`cgroups`)
  y matar nodos para verificar *failover* e invariantes tras el fallo
  ([ADR-0008](../adr/adr-0008-coste-cero.md)).

## 21.5 Sanitizers y detección de carreras

Toda la suite se ejecuta también bajo **sanitizers** (ASan/UBSan), que convierten bugs latentes
(UB, accesos fuera de rango, fugas) en fallos visibles. Para el código concurrente —las colas
lock-free SPSC/MPMC— se usa **ThreadSanitizer + estrés aleatorio**, porque las carreras no
aparecen en pruebas deterministas de un solo hilo. Estos pasos forman parte de la **puerta de
calidad** (ver [capítulo 22](./22-puerta-de-calidad-y-cicd.md)).
