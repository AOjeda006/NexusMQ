# ADR-0004: Protocolo del plano de datos (binario propio + gateway REST)

- **Estado:** aceptado
- **Fecha:** 2026-06-07

## Contexto

El plano de datos necesita un protocolo. Hay dos caminos en tensión:

- Un **binario propio** maximiza el aprendizaje y el control (framing, varint, *multiplexing*/*correlation IDs*, versionado, control de flujo) y compone sin *impedance mismatch* con la arquitectura *shared-nothing*.
- La **compatibilidad con Kafka** daría ecosistema (kcat, consolas, clientes), pero obliga a implementar una especificación ajena y enorme (más de 70 *API keys* versionadas, *record batch* v2, *consumer group protocol*…), atando el diseño y *front-loadeando* un esfuerzo ingrato.

## Decisión

Implementar un **protocolo binario propio** para las fases nucleares, acompañado de un **gateway REST** para interoperabilidad temprana (cubre el "¿funciona con herramientas reales?" vía HTTP/curl/Postman).

Se difiere un **subconjunto Kafka-compatible** (`ApiVersions`/`Metadata`/`Produce`/`Fetch`) a la **Fase 4** como *stretch*: basta para la demo "kcat habla con mi broker" sin apostar el proyecto a la especificación completa. El titular "Kafka-compatible" queda así **recuperable más tarde** sin pagar su coste por adelantado.

## Consecuencias

- (+) Control total del protocolo; aprendizaje de diseño de protocolos (lo que el proyecto quiere demostrar); libertad para ajustarlo a *shared-nothing* y al modelo de créditos.
- (+) Interoperabilidad inmediata vía REST.
- (−) Sin ecosistema Kafka "gratis" hasta la Fase 4.
- (−) El cliente nativo hay que escribirlo (ya estaba en alcance, OE-2).

## Alternativas consideradas

- **Compat Kafka desde el inicio:** ofrece ecosistema instantáneo, pero a costa de un esfuerzo enorme e ingrato, riesgo de *uncanny valley* (compatibilidad parcial que casi funciona) y un diseño atado antes de existir; descartado para el núcleo y diferido como subconjunto de la Fase 4.
- **Binario propio + REST:** elegido (ver Decisión).
