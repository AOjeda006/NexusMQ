# 22. Puerta de calidad y CI/CD

> Lo que tiene que estar verde antes de cada *commit*. La puerta es **obligatoria** y se
> ejecuta en local; el CI la replica. Nunca se *pushea* en rojo.

## 22.1 Los dos compiladores

Todo compila y pasa la suite en **GCC/libstdc++** y **Clang/libc++**. No es redundante: los
dos *toolchains* **divergen** en puntos sensibles —en especial `std::expected` y las versiones
de `clang-tidy`—, así que un cambio que pasa en uno puede romper en el otro. Clang **requiere
libc++** (`-stdlib=libc++`): el `<expected>` de libstdc++ no compila con Clang por
`__cpp_concepts` ([ADR-0011](../adr/adr-0011-cpp23-libcxx-clang.md)).

```bash
cmake --preset linux-gcc   && cmake --build --preset linux-gcc   && ctest --preset linux-gcc
cmake --preset linux-clang && cmake --build --preset linux-clang && ctest --preset linux-clang
```

## 22.2 Sanitizers, formato y análisis estático

- **Sanitizers:** la build de pruebas corre bajo **ASan/UBSan** (`linux-gcc-asan`) y
  **ThreadSanitizer** (`linux-gcc-tsan`) para el código concurrente.
- **Formato:** `clang-format --dry-run --Werror` sobre todo `*.hpp`/`*.cpp` (sin salida = ok).
- **`clang-tidy`** (autoridad de convenciones) sobre `src/`, con el preset Clang/libc++. El
  `.clang-tidy` versionado manda; sus checks desactivados son **decisiones de proyecto**
  (pragma-once; `pro-bounds-*`/`pro-type-vararg` para búferes de bytes y POSIX variádico;
  magic-numbers…). Se aplica solo a `src/` porque tests y *bench* usan macros de terceros.
- **`cppcheck`** como análisis estático complementario.
- **`-Wall -Wextra -Wpedantic -Werror`:** un aviso es un bug en potencia.

## 22.3 El pipeline de CI

El CI (GitHub Actions, `.github/workflows/ci.yml`) replica la puerta en varios *jobs*:

- **build & test** en matriz `[linux-gcc, linux-clang]` (configurar → compilar con
  *warnings-as-errors* → `ctest`).
- **sanitizers** (ASan/UBSan, `linux-gcc-asan`).
- **ThreadSanitizer** (`linux-gcc-tsan`) para las colas lock-free.
- **formato & análisis estático**: `clang-format --dry-run --Werror` sobre todos los `.hpp`/`.cpp`
  versionados y `clang-tidy` sobre `src/*.cpp` con el `compile_commands.json` del preset
  Clang/libc++.

El gate de CI **no es solo build+test**: incluye lint, formato y sanitizers; todo debe pasar.
Dispara en *push* y *pull request* contra `main`, con `concurrency` que **cancela** la ejecución
anterior de la misma referencia si llega un *push* nuevo.

> **Estado actual:** las GitHub Actions están **activas** (`.github/workflows/ci.yml`); el estado
> de la última ejecución se ve en el *badge* del [README](../../README.md). La puerta de calidad
> se ejecuta igualmente **en local** antes de cada *push*: el CI confirma, no sustituye.

## 22.4 Build-once, promote

El artefacto se **construye una sola vez** y se promueve el **mismo binario** entre entornos
(dev → staging → prod), sin recompilar por entorno: la diferencia es la **configuración**
(12-factor), no el binario (ver [capítulo 19](./19-arranque-y-composition-root.md) y
[ADR-0008](../adr/adr-0008-coste-cero.md)). Es coherente con la imagen Docker reproducible del
[capítulo 25](./25-despliegue.md).
