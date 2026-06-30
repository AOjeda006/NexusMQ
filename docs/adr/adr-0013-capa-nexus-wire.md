# ADR-0013: Capa `nexus-wire` para el framing sobre conexión (`FrameReader`/`FrameWriter`)

- **Estado:** aceptado
- **Fecha:** 2026-06-13

## Contexto

Este ADR **refina el diseño detallado** (no cambia el diseño del protocolo): el diseño detallado original ubicaba `FrameReader`/`FrameWriter` en `protocol/frame.hpp`, pero el grafo de dependencias fija `nexus-protocol → common` y «protocolo puro (encode/decode, sin E/S ni async)». La decisión resuelve ese conflicto entre dos fuentes de verdad.

`FrameReader`/`FrameWriter` leen y escriben tramas longitud-prefijo (§7.2) sobre un `Socket` mediante el `Proactor`: necesitan `nexus-io` (`Socket`, `Proactor`) y corrutinas (`task<expected<T>>`). Colocarlos en `nexus-protocol` obligaría a que el protocolo —hoy puro encode/decode, testable y fuzzeable sin E/S— dependiera de `nexus-io` y de la maquinaria async, contradiciendo el grafo (`protocol → common`) y el principio de capas limpias. Los consumidores del framing-sobre-conexión (broker, client, ingress, server) ya dependen tanto de `io` como de `protocol`.

## Decisión

Se crea un target nuevo, **`nexus-wire`** (`src/wire/`), que depende de **common + io + protocol** y aloja `Frame`, `FrameReader` y `FrameWriter`. `nexus-protocol` se mantiene **puro** (codec, `FrameHeader`, mensajes, códigos de error: solo→common, sin E/S ni async, independientemente testeable). broker/client/ingress/server dependen de `nexus-wire` para el transporte de tramas. El `Buffer` de `nexus-common` gana `extend`/`truncate` (cola mutable para `recv` sin copia intermedia), que usa `FrameReader`.

## Consecuencias

- (+) `nexus-protocol` queda independiente de E/S: la (de)serialización se prueba y fuzzea sin tocar sockets ni io_uring.
- (+) Separación de responsabilidades clara (protocolo = bytes; wire = tramas sobre conexión) y reutilizable por todos los consumidores.
- (+) Grafo acíclico y descendente (`wire → {common, io, protocol}`).
- (−) Un target más en el árbol (15 en total) y una desviación respecto a la ubicación prevista en el diseño detallado original.
- (−) `read_frame` expone el payload como vista **dentro del búfer del lector** (válida hasta la siguiente lectura): zero-copy a cambio de una invariante de vida documentada.

## Alternativas consideradas

- **`nexus-protocol` depende de `io` (`FrameReader` en `protocol/frame_io.hpp`):** sigue el diseño detallado original y evita un target nuevo, pero rompe la pureza del protocolo (lo acopla a io_uring/async) y el grafo `protocol → common`; descartado por el autor a favor de capas limpias.
- **`FrameReader` genérico en `nexus-io` (sin conocer `FrameHeader`):** mantiene `io → common`, pero parte el concepto de «trama» en dos capas y se aparta del `read_frame() → Frame` del diseño detallado original; descartado.
