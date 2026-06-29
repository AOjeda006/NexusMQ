# ADR-0012: Backend io_uring directo sobre el uapi del kernel (sin liburing)

- **Estado:** aceptado (precisa el desglose de `nexus-io`, no cambia el diseño)
- **Fecha:** 2026-06-12

> **Precisa el desglose de `nexus-io`** (no cambia el diseño): el desglose anotaba el backend del `Proactor` como *«io_uring (liburing)»*. Este ADR registra que la **implementación** habla con io_uring **directamente** por el uapi del kernel, sin la biblioteca liburing. El **puerto `Proactor` y sus contratos no cambian**; solo el backend.

## Contexto

El backend io_uring (R5) debe cumplir la **puerta de calidad**: compilar y testear **en local y en CI**, en GCC y Clang, bajo sanitizers. En el entorno de desarrollo no hay `sudo` para instalar `liburing-dev` y vcpkg no está *bootstrapeado* (se cae a FetchContent); liburing usa *autotools* (no CMake), así que vendorizarla de forma reproducible es frágil.

En cambio, el **uapi del kernel** (`<linux/io_uring.h>`) está presente tanto en local (WSL2, kernel 6.18) como en el *runner* de CI (ubuntu-24.04), y io_uring funciona en tiempo de ejecución en local (probado: `io_uring_setup` OK, `IORING_FEAT_SINGLE_MMAP`).

## Decisión

Implementar `IoUringBackend` **directamente sobre el uapi**: `io_uring_setup`/`io_uring_enter` vía `syscall`, anillos SQ/CQ mapeados con `mmap` (se exige `IORING_FEAT_SINGLE_MMAP`), barreras *acquire/release* sobre los índices compartidos con el kernel y SQEs gestionadas a mano. **Cero dependencias externas**: el mismo binario compila en todas partes solo con cabeceras del kernel.

CMake compila el backend solo donde existe `<linux/io_uring.h>` (define `NEXUS_HAVE_IOURING`); el *smoke-test* hace E/S real y se **omite en ejecución** (`GTEST_SKIP`) si io_uring no está disponible (kernel viejo, *seccomp* del CI). Toda la gestión cruda (fd, mmap, ops en vuelo) queda confinada en un *pimpl* RAII, sin filtrar el uapi al resto del árbol.

## Consecuencias

- (+) Sin dependencias ni instalación: build idéntica en local y CI, sin `sudo`/vcpkg/autotools.
- (+) Control total del anillo (afín al *shared-nothing* y al objetivo de aprender sistemas) y *hot-path* sin biblioteca intermedia.
- (+) Validado en local en los cuatro *lanes* (GCC/Clang/ASan/TSan) porque io_uring corre aquí.
- (−) Reimplementamos lo que liburing ofrece hecho (setup de anillos, *helpers* de SQE): más código propio que mantener y más superficie de error de bajo nivel.
- (−) Acceso a uniones del `io_uring_sqe` (ABI del kernel): obliga a desactivar `cppcoreguidelines-pro-type-union-access` (igual criterio que `pro-bounds-*`/`pro-type-vararg` para código de sistemas).
- (−) `wake()` queda como *no-op* hasta R6 (requiere registrar un `eventfd` en el anillo).

## Alternativas consideradas

- **liburing vía `liburing-dev` (apt):** lo estándar, pero requiere `sudo` (no disponible) y añade una dependencia de sistema a instalar en cada entorno; descartado por la restricción del entorno.
- **liburing vendorizada (FetchContent/ExternalProject):** reproducible sin `sudo`, pero liburing usa *autotools* (genera cabeceras con `./configure`): wrapper frágil en CMake y más lento en CI; descartado por complejidad frente a la opción sin dependencias.
