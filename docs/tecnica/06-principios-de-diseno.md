# 6. Principios de diseño

> Los principios que vertebran NexusMQ y que explican por qué el código está como está.
> Especializan al dominio de sistemas los principios generales (SOLID, clean architecture)
> sin duplicarlos.

## 6.1 Principios rectores

- **Separación plano de datos / plano de control.** El camino caliente (*produce*/*fetch*)
  se mantiene libre de lógica de administración; la gestión (crear *topics*, rebalanceos)
  vive en un plano aparte (REST admin).
- **Shared-nothing.** El estado mutable es **propiedad de un único reactor**; nada se
  comparte entre núcleos salvo por paso de mensajes. Es el principio que hace el sistema
  razonable y testeable (ver [capítulo 7](./07-concurrencia.md)).
- **Hexagonal aplicada a sistemas.** El núcleo (storage, broker, consenso) no depende del
  detalle de I/O ni de protocolo: estos entran por **puertos** (interfaces) y **adaptadores**
  (p. ej. io_uring vs IOCP es un adaptador del puerto de I/O). Materializa **DIP** y permite
  romper ciclos de capas ([ADR-0017](../adr/adr-0017-nexus-telemetry.md),
  [ADR-0018](../adr/adr-0018-rest-admin-puerto-adaptador.md)).
- **Mecanismo vs política.** El storage ofrece *mecanismo* (append, read, fsync); la
  *política* (retención, durabilidad, `acks`) es configurable y externa.

## 6.2 Las decisiones de arquitectura, en una frase cada una

Los principios anteriores se concretan en decisiones registradas como ADR (ver
[capítulo 28](./28-registro-de-decisiones-adr.md)). Las que más condicionan el diseño:

- **Concurrencia *shared-nothing thread-per-core*** con reactor propio
  ([ADR-0005](../adr/adr-0005-concurrencia-shared-nothing-thread-per-core.md)): elimina la
  clase de bug más cara (carreras sobre estado compartido) y los *locks* del camino caliente.
- **Raft por partición** ([ADR-0003](../adr/adr-0003-replicacion-raft-por-particion.md)): un
  único mecanismo de ordenación, replicación y elección por partición; el log de Raft *es*
  el WAL; el *high-watermark* es el `commitIndex`.
- **Proactor / completions** ([ADR-0002](../adr/adr-0002-modelo-io-asincrona-proactor.md),
  [ADR-0012](../adr/adr-0012-io_uring-directo-uapi.md)): I/O modelada como *completions*
  (io_uring/IOCP), base portable sobre la que se asientan las corutinas C++23.
- **Postura CP / PACELC PC-EC**
  ([ADR-0007](../adr/adr-0007-consistencia-cp-pacelc.md)): ante partición se elige
  consistencia; en operación normal también consistencia sobre latencia.
- **Modelo de errores por capa**
  ([ADR-0009](../adr/adr-0009-manejo-errores-por-capa.md)): `expected<T>` en el núcleo,
  excepciones en el plano de control, códigos de wire en el protocolo
  (ver [capítulo 16](./16-modelo-errores-wire-codes.md)).

## 6.3 Patrones arquitectónicos y de almacenamiento

- **Reactor/Proactor** (I/O por *completions*, un reactor por núcleo).
- **Leader–Follower vía Raft:** cada partición tiene un líder que ordena las escrituras y
  *followers* que replican.
- **Pipeline:** recepción → decodificación → *append* → replicación → *ack*, como etapas
  conectadas por colas.
- **WAL + log-structured storage:** escritura secuencial append-only; **el log de Raft es el
  WAL**. **Segmentación + índice disperso** para *seek* y retención por segmentos enteros;
  **compactación por clave** como política alternativa.

## 6.4 Patrones distribuidos y de resiliencia

- **Consenso (Raft) por partición**, con extensiones contempladas: *pre-vote*, *leadership
  transfer* y réplicas *learner*.
- **Productor idempotente** (*producer-id* + *sequence*) para *effectively-once* por
  partición; **DLQ** para mensajes no procesables.
- **Resiliencia del ingress:** *Circuit Breaker* (closed/open/half-open), *Bulkhead*, *retry*
  con *backoff* + *jitter*, *rate limiting* (token bucket), **backpressure por créditos**
  (colas acotadas *end-to-end*; sin créditos, el productor se frena, no se descarta).

## 6.5 Disciplina de C++ y calidad

Transversal a todo el código: **RAII estricto** (sin `new`/`delete` crudos; recursos del SO
envueltos en tipos), semántica de valor, `const`-correcto, `enum class`; corutinas que
**nunca bloquean** el reactor; y una **puerta de calidad** (dos compiladores, sanitizers,
`clang-tidy`, `clang-format`) obligatoria antes de cada *commit*
(ver [capítulo 22](./22-puerta-de-calidad-y-cicd.md)). El detalle de convenciones lo fija la
`BibliotecaDocumentacion`, autoridad de estilo del proyecto.
