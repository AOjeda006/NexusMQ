# ADR-0009: Política de manejo de errores por capa (excepciones / `std::expected` / códigos de wire)

- **Estado:** aceptado
- **Fecha:** 2026-06-10

## Contexto

`principios/manejo-errores.md` prescribe **excepciones, no códigos de error** para la **lógica de negocio de aplicación**, mientras que `stacks/cpp` avala **`std::expected`/`error_code`** en **código de sistemas y baja latencia**. El broker tiene **tres superficies de error** distintas: el **protocolo de red** (contrato), el **núcleo de sistemas** (camino caliente) y el **plano de control** (admin/REST). Hay que fijar qué mecanismo rige en cada una para no contradecir la normativa ni pagar el coste de las excepciones en el *hot path*.

## Decisión

Política por capa, alineada con la biblioteca:

- **Protocolo / *wire* (§7.2.2):** **códigos de error numéricos** (`errorCode:i16`) como **contrato**; se **traducen al modelo interno en el borde**, no se propagan crudos por el núcleo (`manejo-errores`: *"los códigos de error de protocolo… son legítimos"*).
- **Núcleo de sistemas / camino caliente** (storage, reactor, consensus): **`std::expected<T,E>`** (o `std::error_code`/`Result`) — el error es un **valor**, de coste predecible; **sin excepciones en el *hot path***. Se evaluará **`-fno-exceptions`** solo con **justificación medida** (coste de *unwinding* / latencia de cola).
- **Plano de control / aplicación** (admin, configuración): **excepciones para lo excepcional** más `std::optional` para la ausencia de valor; en el **borde REST**, traducción a **`ProblemDetail` (RFC 7807)** (§7.6).
- **Invariantes transversales (no cambian):** aportar **contexto** en el error; **validar en el borde** (*fail-fast*, §7.9); mantener **una sola política central**; **nunca tragar** un error; y **envolver** los errores de librerías de terceros (liburing/OpenSSL) en tipos propios.

## Consecuencias

- (+) Coherente con `principios/manejo-errores.md` **y** con `stacks/cpp`; sin coste de excepciones en el camino caliente; modelo de error de cara al cliente estándar (códigos en binario, RFC 7807 en REST).
- (−) Hay que **propagar `expected` a mano** (mitigado con los monádicos `and_then`/`or_else`) y mantener la **traducción en los bordes** entre los tres modelos.

## Alternativas consideradas

- **Excepciones en todo (incluido el *hot path*):** contradice la baja latencia (*unwinding* no determinista) y la guía de sistemas de `stacks/cpp`; descartada para el núcleo.
- **Códigos de error en todo (incluida la aplicación):** contradice `manejo-errores.md` para la lógica de aplicación; descartada fuera del núcleo/contrato.
