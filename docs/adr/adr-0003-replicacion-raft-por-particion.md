# ADR-0003: Modelo de replicación (Raft por partición)

- **Estado:** aceptado
- **Fecha:** 2026-06-07

> **Provisional revisable:** se reconfirma con los *benchmarks* de la Fase 1; si cambia, se **reemplaza** por un ADR nuevo (no se edita).

## Contexto

La Fase 2 requiere replicar particiones y tolerar caídas de líder. Existen dos arquitecturas de referencia:

- **Raft por partición** (modelo Redpanda): cada partición es su propio grupo Raft, y el log de Raft *es* el log de la partición.
- **ISR estilo Kafka:** el consenso se usa solo para metadatos/controller, y los datos se replican por primario-backup con *high-watermark*.

## Decisión

Adoptar **Raft por partición**: un **único mecanismo** de ordenación, replicación y elección por partición. El *high-watermark* se deriva del `commitIndex` de Raft, y el modelo de `acks` (§4.3) se asienta sobre su *commit*.

Se prefiere por ser **conceptualmente uniforme**, **formalmente especificado** y **testeable de forma determinista** —lo que sirve al objetivo de *correctitud demostrable*— y porque compone con la arquitectura *shared-nothing* (un grupo Raft anclado a un núcleo).

## Consecuencias

- (+) Un solo mecanismo que razonar y probar; *failover* sin pérdida de datos *committed*; encaje con *thread-per-core*.
- (+) Base natural para el modelo de ACK y la postura CP (ver ADR-0007).
- (−) Coste de **quórum en cada escritura** (latencia de mayoría) en el camino caliente.
- (−) Muchas particiones implican muchos grupos Raft y, por tanto, mucho *heartbeat*; no es problemático a escala de portfolio, pero sí relevante a escala de miles de particiones.

## Alternativas consideradas

- **ISR estilo Kafka:** ofrece mayor throughput potencial, pero introduce más piezas (controller, gestión de ISR, *high-watermark* separado) y más superficie de error; descartado por complejidad y por ser el diseño "de hace 14 años".
- **Raft por partición:** elegido (ver Decisión).
