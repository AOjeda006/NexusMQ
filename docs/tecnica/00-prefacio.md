# Prefacio

> Este documento es la **documentación técnica final** de NexusMQ: la referencia
> autoritativa del *qué*, el *porqué* y el *cómo* del sistema, organizada para leerse
> de principio a fin o consultarse por partes.

## Qué es este documento

NexusMQ es un *message broker* de mensajería distribuido de alto rendimiento, escrito
en **C++23**, con arquitectura **shared-nothing thread-per-core** (un reactor por núcleo
físico, *pinned*) y **Raft por partición** como mecanismo único de replicación y consenso.
Es un **proyecto de aprendizaje y portfolio**: su objetivo nº1 es demostrar profundidad
técnica en un sistema coherente y criterio de ingeniería, no acumular funcionalidades.

Esta documentación recoge ese sistema **tal como está construido** (*as-built*): la visión
y el contexto, la arquitectura y sus principios, los contratos de red, la implementación por
subsistemas, la estrategia de calidad, la operación y, por último, el registro de decisiones
de arquitectura (ADR) y la historia de su evolución por fases.

## A quién va dirigido

- **Al autor**, como mapa de construcción y memoria del diseño.
- **A revisores técnicos** (entrevistadores, colaboradores) que evalúen el proyecto como
  muestra de competencia en sistemas e infraestructura, C++ de bajo nivel y *backend*
  distribuido. Lo que se pretende evidenciar es **profundidad dentro de una sola tesis
  arquitectónica** —correctitud distribuida, latencia y funcionamiento *end-to-end*— y
  **decisiones justificadas y medidas**, no una lista de características.

Se asume familiaridad con conceptos de sistemas (E/S, concurrencia, memoria) y de datos
distribuidos (replicación, consenso). Los términos de dominio se definen en el
[Glosario](./04-glosario.md).

## Cómo está organizado

La documentación se divide en **siete partes** más **apéndices**:

| Parte | Contenido |
| ----- | --------- |
| **I — Introducción y visión** | Resumen ejecutivo, contexto y motivación, estado del arte y glosario. |
| **II — Arquitectura** | Vista de conjunto, principios de diseño, concurrencia, modelo de I/O, almacenamiento, replicación y consenso, *ingress* y observabilidad. |
| **III — Contratos** | Protocolo binario nativo, subconjunto Kafka, API REST de administración y modelo de errores. |
| **IV — Implementación** | Mapa de módulos, catálogo por subsistema, arranque y *composition root*, herramientas y *bindings*. |
| **V — Calidad** | Estrategia de pruebas, puerta de calidad y CI/CD, rendimiento y *benchmarks*, portabilidad. |
| **VI — Operación y despliegue** | Despliegue, configuración y operación, seguridad. |
| **VII — Decisiones y evolución** | Registro de decisiones (ADR), historia de desarrollo y trabajo futuro. |

Los **apéndices** recogen la bibliografía, el índice de diagramas y una guía rápida de
construcción y ejecución.

Cada capítulo vive en su propio fichero Markdown numerado bajo `docs/tecnica/`. Los
**diagramas** (Mermaid, tablas y *layouts* ASCII) viven en `docs/diagramas/` y se referencian
desde la prosa.

## Relación con los contratos as-built y con los ADR

Esta documentación distingue, siguiendo **Diátaxis**, dos tipos de texto:

- **Referencia** — los **contratos as-built** en `docs/`: el protocolo binario nativo
  ([`protocol.md`](../protocol.md)), el subconjunto Kafka ([`kafka.md`](../kafka.md)), la API
  REST de administración ([`openapi.yaml`](../openapi.yaml)) y la metodología y cifras de
  rendimiento ([`benchmarks.md`](../benchmarks.md)). Son la **única fuente de verdad** de los
  detalles precisos (byte-layouts, ApiKeys y versiones, *endpoints*, números). Se consultan.
- **Explicación** — los capítulos narrativos de `docs/tecnica/`. Se leen en orden. Los
  capítulos de contratos (Parte III) **explican y enlazan** —dan *rationale*, flujos, ejemplos
  y diagramas— pero **no duplican** el contrato: remiten a él para evitar divergencia (*drift*).

Las **decisiones de arquitectura** se registran como ADR, uno por archivo, en
[`docs/adr/`](../adr/) (ADR-0001 a ADR-0029). Un ADR aceptado **no se edita**: si una decisión
cambia, se **reemplaza** por un ADR nuevo que marca al anterior como reemplazado. El registro
narrado vive en el capítulo 28; los capítulos los referencian por su número (p. ej.
[ADR-0005](../adr/adr-0005-concurrencia-shared-nothing-thread-per-core.md)).

## Sustituye a `DocumentacionProvisional/`

Esta documentación técnica **sustituye** a la carpeta `DocumentacionProvisional/` (anteproyecto,
desgloses y hoja de ruta), reparte y **amplía** su contenido por las siete partes, **extrae** sus
ADR a `docs/adr/` y se apoya en los contratos as-built de `docs/`. Una vez compilada y verificado
que nada queda solo en la carpeta provisional, esta se elimina y el repositorio se publica.
