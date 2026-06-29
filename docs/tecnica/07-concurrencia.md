# 7. Concurrencia

> El modelo de ejecución de NexusMQ: por qué no hay *locks* en el camino caliente y cómo se
> reparte el trabajo entre núcleos sin estado compartido. Decisiones de fondo:
> [ADR-0005](../adr/adr-0005-concurrencia-shared-nothing-thread-per-core.md) y
> [ADR-0026](../adr/adr-0026-sharding-por-nucleo.md).

## 7.1 Un reactor por núcleo

NexusMQ ejecuta **un reactor por núcleo físico**, cada uno *pinned* a su CPU. Un reactor es
dueño **exclusivo** de: su **anillo io_uring** (o IOCP en Windows), su ***allocator***
(arena/pool local, NUMA-aware) y un **subconjunto de réplicas de partición**. **No hay
estado mutable compartido entre reactores.** Esto elimina la clase de bug más cara —las
carreras sobre estado compartido— y, con ella, los *locks* del camino caliente: cada
partición es una **unidad de serialización** (estilo *actor*) anclada a un núcleo. La
topología se ilustra en el [diagrama 5](../diagramas/05-topologia-thread-per-core.md).

## 7.2 Sharding: qué núcleo es dueño de qué

El reparto de carga no se hace por *work-stealing* sobre una cola compartida, sino por
**asignación de particiones a núcleos** ([ADR-0026](../adr/adr-0026-sharding-por-nucleo.md)):

- **Partición → núcleo:** `partition % N` (N = número de reactores). El reactor resultante es
  el **único dueño** de esa réplica.
- **Grupo de consumidores → núcleo:** `hash(group_id) % N` (FNV-1a), para que la coordinación
  de cada grupo viva siempre en el mismo reactor.

Como cada *shard* tiene **un único dueño**, las operaciones sobre él son **linealizables sin
locks**: las sirve el reactor propietario, en orden, sin sincronización. Ver el
[diagrama 6](../diagramas/06-sharding.md).

## 7.3 Paso de mensajes cross-core

Cuando una petición llega al núcleo A pero su partición vive en B, **no** se accede a estado
de B: la petición se **reenvía** a B por una cola **SPSC** (*single-producer
single-consumer*), con despertar del destino (estilo `submit_to` de Seastar). El resultado
vuelve por el mismo mecanismo. SPSC es el caso simple y rápido (un *ring buffer* basta); el
diseño minimiza productores/consumidores por cola precisamente para evitar la complejidad
(CAS, ABA) de las colas MPMC.

- **Lock-free solo con motivo y medición.** Donde hay colas, se usa CAS con el `memory_order`
  mínimo correcto (`acquire`/`release` en productor-consumidor); en las MPMC se resuelve el
  problema **ABA** (contadores de versión / *hazard pointers* / *epoch-based reclamation*) y
  se valida con **ThreadSanitizer + estrés aleatorio** en CI.
- **False sharing.** Los campos escritos por hilos distintos (p. ej. *head*/*tail* de cada
  cola) se separan a líneas de caché distintas con
  `alignas(std::hardware_destructive_interference_size)`.

## 7.4 Backpressure por créditos

Las colas son **acotadas *end-to-end***. El receptor concede **créditos** al emisor; cuando
se agotan, el emisor se **frena** en lugar de descartar mensajes o dejar crecer los *buffers*
sin límite. Esto evita el colapso bajo sobrecarga y mantiene las latencias de cola acotadas
(`ConnectionState.credits`, ver [capítulo 8](./08-modelo-de-io.md)).

## 7.5 Nada bloqueante en el reactor

La contrapartida del modelo es **disciplina asíncrona total**: un `fsync` síncrono, un
`malloc` que entre al kernel o un *lock* contendido **congelan el núcleo entero** y con él
todas las tareas que multiplexa. Por eso **todo** I/O (incluido `fsync`) pasa por el proactor
y se usan *allocators* por núcleo para no entrar al kernel en el camino caliente. Las
corutinas del reactor nunca hacen `co_await` sobre operaciones bloqueantes
(ver [capítulo 8](./08-modelo-de-io.md)).

## 7.6 Reloj para la concurrencia distribuida

Todo lo que mide **intervalos** y no puede retroceder —*timeouts*, *heartbeats* y el
*election timeout* de Raft, *backoff*— usa el **reloj monotónico** (`steady_clock`), nunca el
*wall-clock*: un salto de NTP hacia atrás dispararía elecciones espurias. El *wall-clock* se
reserva para *timestamps* de `Record` y retención por tiempo. El orden dentro de una
partición lo da el **offset**, no la marca de tiempo.
