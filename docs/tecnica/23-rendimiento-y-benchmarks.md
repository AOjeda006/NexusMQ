# 23. Rendimiento y benchmarks

> Cómo se mide NexusMQ sin engañarse. Este capítulo **explica la metodología**; las cifras
> concretas y su entorno son contrato y viven en [`docs/benchmarks.md`](../benchmarks.md) — no
> se reproducen aquí para no duplicar ni desincronizar.

## 23.1 Principios de medición

- **Mide, no adivines.** Primero correcto y claro; se optimiza **con datos** y solo el cuello
  de botella dominante (ley de Amdahl). No se micro-optimiza el código frío.
- **Objetivos en percentiles, no en medias.** Se reporta **p50/p99/p999/max**: la media oculta
  la *long tail* que sufre el usuario peor servido. Se fija un **SLO** de latencia explícito.
- **Baseline + un cambio cada vez.** Sin línea base no hay mejora demostrable; cambiar varias
  cosas a la vez impide atribuir el efecto.
- **Perfila antes de tocar** (CPU profiler, *flame graphs*) para localizar el *hot path* real.

## 23.2 Evitar la *coordinated omission*

El error de medición más traicionero. Un cliente **closed-loop** (que espera cada respuesta
antes de enviar la siguiente) **subestima la cola** cuando el sistema se satura: deja de enviar
justo las peticiones que más tardarían. Por eso NexusMQ mide con un generador **open-loop**
(`nexus-loadgen`, ver [capítulo 20](./20-herramientas-y-bindings.md)): **tasa fija**,
independiente de las respuestas, a una carga representativa (no al throughput máximo, que no
dice nada de la latencia de cola). Los histogramas son de alta resolución (estilo
**HdrHistogram**). El detalle está en [`docs/benchmarks.md`](../benchmarks.md).

## 23.3 Qué se mide

- **Throughput** de *produce*/*fetch* (msg/s y MiB/s) a *batch* y `acks` dados.
- **Latencia** de *produce* (`acks=1` y `acks=quorum`) y de *fetch*, por percentiles.
- **Impacto del `fsync`** (agrupado vs por escritura) y de la replicación de quórum.
- **Lectura** desde *page cache*/mmap vs disco.
- **Microbenchmarks** del núcleo (append, CRC32C, codec) con `nexus-bench`.

Se sigue el **método USE** (utilización, saturación, errores) por recurso (CPU, memoria, disco,
red) y se tiene en cuenta el *observer effect*: la propia observabilidad tiene coste.

## 23.4 Throughput ≠ latencia, y escalabilidad

Throughput y latencia se optimizan distinto y a veces se oponen (el *batching* sube throughput
pero puede subir latencia). En escalabilidad, la fracción serie pone el techo (Amdahl) y la
contención puede **empeorar** al añadir núcleos (USL): por eso el diseño *shared-nothing*
reduce la sección crítica a (idealmente) cero —cada partición la sirve su único reactor— antes
de añadir núcleos (ver [capítulo 7](./07-concurrencia.md)).

## 23.5 Entorno y reproducibilidad

Las cifras se toman **en local**, con *hardware* y parámetros documentados (núcleos aislados
con `isolcpus`/`taskset`), nunca en *free tiers* de cloud —que no sirven para medidas serias
([ADR-0008](../adr/adr-0008-coste-cero.md))—. El procedimiento exacto de reproducción y los
resultados están en [`docs/benchmarks.md`](../benchmarks.md).
