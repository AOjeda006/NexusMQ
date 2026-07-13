# ADR-0037: Config de topic mutable en caliente y publicada cross-core (`PATCH /api/v1/topics/{name}`)

- **Estado:** aceptado
- **Fecha:** 2026-07-13

## Contexto

`TopicConfig` se documentaba y trataba como **INMUTABLE**: se fijaba al crear el topic y no había
forma de cambiarla en un broker vivo. Con la aplicación de retención en runtime
([ADR-0036](adr-0036-aplicacion-retencion-runtime.md)) eso deja un hueco operativo: no se puede
ajustar `retentionMs`/`retentionBytes` de un topic sin borrarlo y recrearlo (perdiendo los datos). La
consola de administración necesita alterar la retención en caliente.

Hay dos obstáculos de diseño:

1. **Coherencia por núcleo.** Los metadatos de topic (incluida su config) se registran **completos en
   cada núcleo** ([ADR-0026](adr-0026-sharding-por-nucleo.md)): cada `TopicManager` tiene su propia
   copia del `Topic`. Un cambio de config debe **publicarse a todos los núcleos**, o el barrido de
   retención de un núcleo leería un valor obsoleto.

2. **No todo es mutable.** `segmentBytes` está **horneado** en los segmentos ya escritos (cada
   `.log`/`.index` se rotó con ese tamaño): cambiarlo en caliente no puede reescribir el pasado y
   dejaría el log inconsistente respecto a su config. Solo la **retención** (`retentionMs`,
   `retentionBytes`) es realmente mutable sin tocar datos ya escritos.

## Decisión

Se hace la config de topic **parcialmente mutable en caliente**, con publicación cross-core:

1. **Solo la retención es mutable.** `Topic::set_retention` actualiza `retentionMs`/`retentionBytes`
   en la copia de metadatos del núcleo (REACTOR-LOCAL). `segmentBytes` es **create-only**: el borde
   REST **rechaza** un `PATCH` que lo incluya con un **400 RFC 7807** claro (recréa el topic para
   cambiarlo).

2. **Semántica PATCH (parcial).** `AlterTopicSpec` lleva `optional<retentionMs>` y
   `optional<retentionBytes>`: solo se aplican los campos presentes; los ausentes conservan su valor.

3. **Fan-out cross-core.** `update_topic_config_on_cluster` publica el cambio en **todos** los núcleos
   por paso de mensajes (`call_on`), igual que `create_topic_on_cluster`. Como el *update* es
   idempotente y no reserva recursos, no hace falta *rollback*. El núcleo 0 es autoritativo.

4. **Lo lee el barrido de retención.** `enforce_retention_all` (ADR-0036) lee `topic->meta().config`
   en **cada** ciclo, así que un `PATCH` surte efecto en el siguiente barrido sin reiniciar ni reabrir
   particiones.

5. **Observabilidad.** `describe_topic` (`GET /api/v1/topics/{name}`) expone ahora la config vigente
   (`config.retentionMs/retentionBytes/segmentBytes`), de modo que un `PATCH` es **verificable** de
   inmediato.

Este ADR **refina** la anotación previa de `TopicConfig` como INMUTABLE (documentación/código), sin
editar ADRs aceptados: la config pasa a ser REACTOR-LOCAL con la retención mutable en caliente.

## Consecuencias

- (+) Se puede ajustar la retención de un topic **sin recrearlo** ni perder datos.
- (+) Coherente en todos los núcleos (fan-out cross-core, mismo patrón que crear/borrar).
- (+) El `PATCH` es **verificable**: `describe_topic` muestra la config vigente y el barrido de
  retención la aplica en el siguiente ciclo.
- (+) `segmentBytes` protegido: un intento de cambiarlo se rechaza con un error claro, en vez de dejar
  el log inconsistente en silencio.
- (−) La config deja de ser estrictamente inmutable: hay que razonar sobre concurrencia (resuelto por
  el confinamiento REACTOR-LOCAL: cada núcleo la muta y la lee en su propio hilo).
- (−) El cambio no es transaccional entre núcleos: durante el fan-out, distintos núcleos pueden ver
  brevemente valores distintos (aceptable: la retención es mantenimiento de fondo, no linealizable).

## Alternativas consideradas

- **Config totalmente inmutable (recrear el topic para cambiar retención):** operacionalmente
  inviable (pierde datos); descartado.
- **Hacer `segmentBytes` también mutable:** requeriría reescribir/re-sellar segmentos pasados o
  aceptar una config que no refleja los datos en disco; se deja **create-only**.
- **Config replicada por Raft (linealizable):** los metadatos de topic no van por Raft en este corte
  (ADR-0026 los replica por fan-out, no por consenso); un cambio de retención no necesita
  linealizabilidad (es mantenimiento de fondo). Se mantiene el fan-out.
- **PATCH que reemplaza toda la config (PUT):** menos ergonómico para la consola y arriesga pisar
  campos; se elige la semántica PATCH parcial con `optional`.
