# ADR-0020: *Binding* de Python vía **ABI C estable** (`nexus-ffi` + `ctypes`) en lugar de pybind11

- **Estado:** aceptado
- **Fecha:** 2026-06-20

> **Ajusta el alcance previsto** (la fase F9 contemplaba «*binding* Python (pybind11) *si el entorno lo soporta*»): fija **cómo** se expone NexusMQ a Python cuando el entorno **no** soporta construir extensiones de CPython.

## Contexto

F9 (Fase 4) pide un *binding* de Python. La opción por defecto (pybind11) compila una **extensión nativa de CPython**, que necesita las **cabeceras de desarrollo de Python** (`Python.h`, `pyconfig.h`) y una *toolchain* de extensiones. El entorno de desarrollo y los *runners* de CI **no** las tienen (solo el intérprete; sin `python3-dev`, sin `pip`, sin `sudo` para instalarlas), de modo que una extensión pybind11 **no se podría compilar ni verificar** aquí — chocaría con la puerta de calidad (TDD, «nunca pushear en rojo», verificar en local en los dos compiladores).

## Decisión

Exponer un subconjunto **puro y transversal** del núcleo tras una **frontera C `extern "C"`** en un target nuevo `nexus-ffi`, compilado como **librería compartida** (`libnexus-ffi.so`), y consumirla desde Python con **`ctypes`** (módulo `bindings/python/nexusmq.py`). La ABI (`src/ffi/nexus_ffi.h`) cubre: versión, **CRC32C** (integridad de records) y el codec de **contexto de traza W3C `traceparent`** (F8). Las ventajas son tres:

1. Se compila con el **propio compilador de C++** (sin cabeceras de Python), así que entra en la puerta de calidad (GCC/Clang/ASan + clang-tidy sobre `src/ffi/nexus_ffi.cpp`).
2. Se verifica con **cualquier** intérprete de Python presente (prueba de humo `bindings/python/smoke_test.py` con `ctypes`).
3. La ABI C es **estable** y sirve a otros lenguajes (Rust/Go/Node FFI), no solo a Python.

Para enlazar los archivos estáticos del núcleo dentro de la `.so`, `nexus-common` y `nexus-telemetry` se marcan `POSITION_INDEPENDENT_CODE`.

## Consecuencias

- (+) *Binding* **realmente verificado** en este entorno (ABI por GoogleTest en la puerta de calidad; lado Python por la prueba de humo), no código sin compilar.
- (+) Sin dependencias nuevas de build (ni pybind11 ni `python3-dev`); CI intacto.
- (+) La frontera C es reutilizable por otros lenguajes.
- (−) Ergonomía más baja que pybind11 (gestión manual de tipos/`ctypes`, sin objetos Python ricos automáticos) y superficie acotada a funciones puras (no expone aún el cliente productor/consumidor).
- (−) Introduce la primera librería **compartida** del árbol (y PIC en dos targets base).

## Alternativas consideradas

- **pybind11 (extensión de CPython):** mejor ergonomía, pero **no construible ni verificable** sin `python3-dev`/*toolchain* en este entorno/CI; quedaría como código sin compilar, contra la normativa. Reconsiderable si el entorno gana las cabeceras de desarrollo.
- **Cabeceras de Python vendoreadas a mano (prefijo local, como OpenSSL en ADR-0019):** `pyconfig.h` es **generado por la build de CPython** y específico de plataforma; replicarlo a mano es frágil y los *runners* de CI tampoco lo tendrían. Descartado.
- **Servicio REST + cliente HTTP en Python:** ya existe el gateway REST (Fase 3), pero eso es interoperabilidad de red, no un *binding* en proceso; complementario, no sustituto.
