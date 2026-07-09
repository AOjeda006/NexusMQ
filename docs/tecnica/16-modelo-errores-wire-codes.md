# 16. Modelo de errores y códigos de wire

> Cómo se representa y propaga un fallo en NexusMQ. La regla es **una política por capa**:
> el mecanismo cambia según la superficie (protocolo, núcleo, plano de control), pero las
> invariantes transversales no. Decisión de fondo:
> [ADR-0009](../adr/adr-0009-manejo-errores-por-capa.md).

## 16.1 Tres superficies de error

El broker tiene tres superficies con exigencias distintas, y por eso tres mecanismos:

| Superficie | Mecanismo | Por qué |
| ---------- | --------- | ------- |
| **Protocolo / *wire*** | `errorCode:i16` (código numérico) | Es **contrato** de red, parte del *wire format*; legítimo y estable entre versiones. |
| **Núcleo / *hot-path*** (storage, reactor, consensus) | `std::expected<T, Error>` | El error es un **valor** de coste predecible; sin *unwinding* de excepciones en el camino caliente. |
| **Plano de control** (admin, configuración) | Excepciones + `std::optional` | Lógica de aplicación: las excepciones separan el flujo normal del fallo; ausencia → `optional`. |

Esta política reconcilia las dos autoridades de la biblioteca: "excepciones, no códigos
de error" rige la **lógica de aplicación**, mientras que en **sistemas/baja latencia** se
prefiere un error explícito como valor (`std::expected`). Ambas conviven sin contradicción
porque se aplican a capas distintas.

## 16.2 Traducción en el borde

Los códigos de wire **no se propagan crudos** por el núcleo: se **traducen al modelo
interno en el borde** (al decodificar una petición) y de vuelta a un código al codificar
la respuesta. El núcleo razona con `Error`/`expected`, no con enteros del protocolo. Del
mismo modo, los errores de librerías de terceros (p. ej. OpenSSL) se **envuelven** en
tipos propios para no acoplar el núcleo a su modelo de errores.

```
   wire (i16)  ──decode──►  Error interno  ──core (expected<T>)──►  Error interno  ──encode──►  wire (i16)
                  borde                                                              borde
```

## 16.3 El tipo `Error` y `expected<T>`

El núcleo usa el alias `expected<T>` = `std::expected<T, Error>` (C++23, que es la razón
de subir el estándar y de exigir libc++ en Clang —
[ADR-0011](../adr/adr-0011-cpp23-libcxx-clang.md)). Las corutinas del *hot-path* devuelven
`task<expected<T>>`. La propagación se encadena con los monádicos `and_then`/`or_else`,
evitando el ruido de comprobar cada retorno a mano. Todo `Error` aporta **contexto** (qué
operación falló y de qué tipo de fallo se trata): nunca se "traga" un error en silencio.

## 16.4 Taxonomía de códigos del protocolo

El protocolo nativo define un conjunto acotado de códigos `i16` (definidos en
`src/protocol/error_code.hpp`) que cubren las familias habituales de un broker, por ejemplo:

- **Entrada inválida:** mensaje/batch demasiado grande (`MESSAGE_TOO_LARGE`),
  petición malformada (`InvalidRequest`) —incluido un **nombre de *topic* inválido** en
  `CreateTopic`, validado por `TopicManager` (fuente única) e idéntico en REST y en el
  protocolo nativo—, versión de API no soportada.
- **Enrutado/topología:** *topic*/partición inexistente, **no soy el líder** de esa
  partición (el cliente debe redirigir al líder vía *metadata*).
- **Disponibilidad/consistencia:** sin quórum para escribir (postura CP), *timeout* de
  replicación.
- **Offset/retención:** offset fuera de rango (purgado por retención).

> La lista exacta y sus valores son **contrato**: viven en el código y en el
> [contrato del protocolo](../protocol.md). Para el subset Kafka, los códigos se mapean a
> los **error codes de Kafka** en el borde del adaptador
> (ver [capítulo 14](./14-subconjunto-kafka.md)).

## 16.5 En la API REST

En el plano de control, el borde REST traduce a **`ProblemDetail` (RFC 7807)** con una
**política central** (ver [capítulo 15](./15-api-rest-administracion.md)). Es la misma
filosofía —una sola política de traducción de errores hacia el exterior— materializada en
HTTP en lugar de en el *wire* binario.
