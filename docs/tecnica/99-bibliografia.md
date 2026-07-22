# Apéndice A. Bibliografía

> Fuentes canónicas que sustentan las decisiones de diseño de NexusMQ. Coinciden con la
> autoridad de la `BibliotecaDocumentacion` (fundamentos de sistemas, concurrencia,
> rendimiento, datos distribuidos).

## Datos distribuidos y consenso

- Kleppmann, M. *Designing Data-Intensive Applications*. O'Reilly, 2017. (DDIA)
- Ongaro, D.; Ousterhout, J. *In Search of an Understandable Consensus Algorithm (Raft)*.
  USENIX ATC, 2014.
- Ongaro, D. *Consensus: Bridging Theory and Practice* (tesis; *pre-vote*, *leadership
  transfer*, *learners*, snapshots).
- Petrov, A. *Database Internals*. O'Reilly. (Storage engines, replicación, consenso.)
- Apache Kafka — *Documentation*: diseño del log particionado, *high-watermark* y formato
  *record batch* v2.

## Arquitectura de brokers de referencia

- Redpanda — arquitectura *thread-per-core* / shared-nothing (sobre Seastar) y Raft por
  partición.
- Seastar — *framework* asíncrono (referencia conceptual del reactor *per-core*:
  `submit_to`, *futures*, shared-nothing).

## Concurrencia y rendimiento

- Herlihy, M.; Shavit, N. *The Art of Multiprocessor Programming*. (Concurrencia,
  lock-free, consenso.)
- Williams, A. *C++ Concurrency in Action*. (Modelo de memoria, atómicos, lock-free.)
- Gregg, B. *Systems Performance*. (Método USE, *profiling*, latencia.)
- Tene, G. *How NOT to Measure Latency* / HdrHistogram. (*Coordinated omission*, percentiles.)
- Drepper, U. *What Every Programmer Should Know About Memory*. (Caché, NUMA, localidad.)

## Sistemas, SO e I/O

- Bryant, R.; O'Hallaron, D. *Computer Systems: A Programmer's Perspective* (CS:APP).
- Kerrisk, M. *The Linux Programming Interface* (TLPI). (syscalls, `fsync`, `mmap`.)
- Axboe, J. *Efficient IO with io_uring*. (Documento de diseño de io_uring.)

## C++ y diseño

- Meyers, S. *Effective Modern C++*.
- Stroustrup, B.; Sutter, H. *C++ Core Guidelines*.
- Stroustrup, B. *A Tour of C++*.
- Nygard, M. *Release It!*. (Circuit Breaker, Bulkhead, patrones de estabilidad.)
- Martin, R. C. *Clean Code* / *Clean Architecture*.

> Las convenciones de proyecto (naming, errores, testing, concurrencia, memoria,
> rendimiento, redes, datos distribuidos, seguridad, CI/CD) están normalizadas en la
> `BibliotecaDocumentacion`, la autoridad de estilo que este proyecto sigue; lo que de ella se
> aplica aquí queda recogido en el [capítulo 6](./06-principios-de-diseno.md) y en la
> [puerta de calidad](./22-puerta-de-calidad-y-cicd.md).
