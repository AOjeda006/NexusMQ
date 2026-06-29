# ADR-0005: Arquitectura de concurrencia (*shared-nothing thread-per-core* con reactor propio)

- **Estado:** aceptado
- **Fecha:** 2026-06-07

> **Provisional revisable** tras los *benchmarks* de la Fase 1; si cambia, se **reemplaza** por un ADR nuevo.

## Contexto

El plano de datos exige un modelo de concurrencia. Hay dos familias en juego:

- **Thread-pool con estado compartido** (modelo Kafka clásico): *locks* sobre estructuras compartidas.
- ***Shared-nothing thread-per-core*** (modelo Redpanda/Seastar): un reactor por núcleo, estado por núcleo y paso de mensajes.

El proyecto prioriza la profundidad de sistemas, la *correctitud demostrable* y estar en el estado del arte.

## Decisión

Adoptar ***shared-nothing thread-per-core***: **un reactor por núcleo** (*pinned*), cada uno dueño de su anillo **io_uring**, su ***allocator*** y su subconjunto de particiones; la comunicación entre reactores se hace **solo por colas SPSC *cross-core*** (estilo `submit_to`).

Se **construye un reactor mínimo propio** sobre io_uring + corutinas C++20 y **no** se usa Seastar (dependencia pesada, Linux-only; "construir" demuestra más que "usar" y encaja con la política §3.4 de hacer el núcleo a mano). Seastar queda como **referencia conceptual**. La decisión es **provisional revisable** tras los *benchmarks* de la Fase 1.

## Consecuencias

- (+) Elimina la clase de bug más cara (carreras sobre estado compartido); cachés y NUMA locales; sin *locks* en el camino caliente; estado del arte.
- (+) Compone con Raft por partición (un grupo anclado a un núcleo) y con io_uring (un anillo por núcleo).
- (−) **Disciplina asíncrona total:** nada bloqueante en el reactor (un `fsync`, `malloc` o *lock* que entre al kernel congela el núcleo), lo que exige `fsync` asíncrono y *allocators* por núcleo.
- (−) Coordinación *cross-core* explícita (SPSC + *wakeup*).
- (−) Si se adopta *direct I/O*, hay que implementar caché/*readahead* propios (no se usa el *page cache*).

## Alternativas consideradas

- **Thread-pool con work-stealing / estado compartido:** más simple de arrancar, pero demuestra el diseño de hace 14 años, introduce contención y la clase de bug que queremos eliminar; descartado como primario.
- **Usar Seastar:** ahorra construir el reactor, pero demuestra menos, es Linux-only y pesado; descartado como dependencia, retenido como referencia.
