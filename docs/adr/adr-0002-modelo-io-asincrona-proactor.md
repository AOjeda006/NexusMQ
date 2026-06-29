# ADR-0002: Modelo de I/O asíncrona (proactor; io_uring primero, IOCP después)

- **Estado:** aceptado
- **Fecha:** 2026-06-07

## Contexto

El plano de datos del broker es I/O intensiva, por lo que el modelo de E/S asíncrona es una decisión central. Los dos *targets* previstos (Linux y Windows) ofrecen modelos asíncronos: io_uring e IOCP, **ambos basados en *completions***. En cambio, epoll es un modelo *readiness-based*, conceptualmente distinto.

## Decisión

Modelar la capa de I/O como **proactor**: la interfaz consiste en enviar una operación asíncrona y recibir una *completion*. El **backend será io_uring ahora** e **IOCP después**. Las **coroutines de C++20** se asientan sobre este proactor.

El storage engine arranca con I/O **bloqueante** (Fase 1) y adopta io_uring como **optimización medida**. Bajo el reactor *thread-per-core*, **todo** el I/O —incluido `fsync`— pasa por io_uring, porque nada bloqueante puede vivir en el reactor (matiz R6). El *direct I/O* con caché propia queda como profundidad opcional de la Fase 4.

## Consecuencias

- (+) Una sola forma de I/O para ambos *targets*; coroutines naturales; un anillo por núcleo encaja con la arquitectura *shared-nothing*.
- (+) La portabilidad queda como capacidad documentada, sin necesidad de doble implementación inicial.
- (−) Un proactor es más complejo que un *reactor* epoll directo; si hiciera falta epoll, se emularía como *completion*.
- (−) Prohibir bloqueos en el reactor exige una disciplina asíncrona total (allocators por núcleo, `fsync` asíncrono).

## Alternativas consideradas

- **Reactor epoll (*readiness*):** descartado como forma canónica por no mapear bien a IOCP (Windows).
- **Librería asíncrona externa (ASIO/Seastar):** descartada para el núcleo por ser una pieza de aprendizaje central; Seastar se valora solo como referencia conceptual.
