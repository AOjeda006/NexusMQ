# ADR-0011: Estándar C++23 + libc++ en Clang (para `std::expected`)

- **Estado:** aceptado (refina el mecanismo de ADR-0009, no lo reemplaza)
- **Fecha:** 2026-06-11

> **Refina el mecanismo de ADR-0009** (no lo reemplaza). ADR-0009 fija `expected<T>` = `std::expected<T, Error>` en el núcleo; este ADR registra las **consecuencias de toolchain** de esa decisión, descubiertas al implementar el modelo de errores en M2.

## Contexto

`std::expected` es **C++23**, pero el proyecto se fijó en C++20 (`cxx_std_20`). Al implementar `error.hpp` se constató, además, que **Clang 18 + libstdc++ no compila `<expected>`** ni siquiera en C++23: el `<expected>` de libstdc++ exige `__cpp_concepts >= 202002L` y Clang 18 reporta `201907L`. GCC (libstdc++) sí lo compila (`__cpp_concepts = 202002L`).

Sin solución, el *lane* de Clang del CI —y **clang-tidy**, que parsea con el frontend de Clang— quedarían rotos en todo fichero que use el modelo de errores.

## Decisión

Subir el estándar a **`cxx_std_23`**. **GCC** compila con **libstdc++** (por defecto). **Clang** compila con **libc++** (`-stdlib=libc++`), fijado **globalmente** en el preset `linux-clang` y en el CI (build + lint), de modo que también las dependencias traídas por FetchContent (GoogleTest/Benchmark) usen libc++ y no haya choque de ABI.

`clang-tidy` consume el `compile_commands.json` de ese preset, así que parsea con libc++. Se documenta en `CLAUDE.md` (hechos del proyecto) y se requiere `libc++-dev`/`libc++abi-dev` donde se use Clang.

## Consecuencias

- (+) `std::expected` disponible y **probado en ambos compiladores** (GCC/libstdc++ y Clang/libc++); se conserva la cobertura de Clang y de clang-tidy.
- (+) Modelo de error de cara al cliente estándar (sin coste de excepciones en el camino caliente, ADR-0009).
- (−) Conviven **dos** librerías estándar (libstdc++ con GCC, libc++ con Clang): hay que instalar libc++ donde se compile con Clang; algo más de superficie de toolchain.
- (−) El estándar mínimo sube de C++20 a C++23 (el toolchain objetivo lo soporta de sobra).

## Alternativas consideradas

- **`expected` propio (C++20) sobre `std::variant`:** portable sin libc++ y sin subir de estándar, pero se aparta del mecanismo elegido en ADR-0009 (`std::expected`); descartado por preferencia explícita de mantener el tipo estándar.
- **`std::expected` solo con GCC (sin lane Clang):** simple, pero pierde la cobertura de Clang y deja **clang-tidy** sin poder parsear el modelo de errores; descartado.
