# CLAUDE.md — NexusMQ

> Contexto automático para el agente. Léelo antes de proponer o escribir nada.

## Qué es NexusMQ

Broker de mensajería distribuido de alto rendimiento en **C++20**, con arquitectura
**shared-nothing thread-per-core** (un reactor por núcleo) y **Raft por partición**.
Es un **proyecto de aprendizaje y portfolio**: el objetivo nº1 es que el autor
**aprenda C++ moderno y sistemas**, no entregar funcionalidad rápido.

Fuentes de verdad del **diseño** (no las contradigas; si hay que ajustar el desglose
al implementar, se propone y se decide; un **ADR aceptado no se edita**, se reemplaza):

- `DocumentacionProvisional/anteproyecto.md` — visión, alcance, arquitectura y **ADR-0001..0009** (el *qué* y el *porqué*).
- `DocumentacionProvisional/Desglose/nexusmqdesglose.md` — vista de conjunto: 14 *targets*, grafo de dependencias, mapa fase→targets.
- `DocumentacionProvisional/Desglose/nexusmqdesglosedetallado.md` — el plano: cada clase/campo/método con firma y visibilidad.
- `DocumentacionProvisional/hoja-de-ruta.md` — **plan de desarrollo vivo**; se actualiza tras cada paso.

## Modo de trabajo (REGLA DE ORO)

**Tutor + par de programación, no implementador automático.** El autor escribe el
código C++ guiado paso a paso; el agente explica el *para qué* (idiomas de C++ moderno,
conceptos de sistemas: RAII, corutinas, io_uring, lock-free, Raft…) a nivel didáctico.

Cada paso: (1) **qué** vamos a hacer y por qué encaja en el diseño → (2) el **concepto**
C++/sistemas implicado → (3) lo **implementamos** (lo teclea el autor; el agente revisa)
→ (4) **verificamos** (compila/test) → (5) se **actualiza la hoja de ruta**.

- Pasos **pequeños**; guiar, no adelantarse. Conciso pero didáctico: prioriza que el autor entienda.
- Si el autor dice **"hazlo tú"** para un trozo repetitivo o sin valor de aprendizaje, lo hace el agente.
- **Excepción al modo tutor:** el **andamiaje y la documentación** (hoja de ruta, README, LICENSE,
  este CLAUDE.md, ficheros de build, CI) los crea el agente directamente. El modo tutor aplica al **código C++**.

## Hechos del proyecto (vinculantes)

- **Lenguaje/estándar:** **C++23** (`cxx_std_23`). Lo exige `std::expected` (modelo de errores del núcleo, ADR-0009). **Clang requiere libc++** (`-stdlib=libc++`, en el preset `linux-clang` y el CI); GCC usa libstdc++. Motivo: el `<expected>` de libstdc++ se bloquea con Clang por `__cpp_concepts`.
- **Build/deps:** **CMake** (con `CMakePresets.json`) + **vcpkg** (modo *manifest*). Un único árbol CMake (una "solución").
- **Rama:** **`main` única**. Se commitea/pushea directamente a `main`. **No** se crean ramas de feature ni PRs (anula el flujo por defecto). *(Decisión del autor para este repo; el resto de su normativa git aplica.)*
- **Commits:** **Conventional Commits** (`feat:`/`fix:`/`docs:`/`test:`/`refactor:`/`chore:`); **atómicos**; cada ADR con `docs:`. Se commitea al cerrar cada paso (o cuando el autor lo diga).
- **Modelo de errores (ADR-0009):** `expected<T>` (= `std::expected<T, Error>`) en **núcleo/hot-path** (storage, reactor, consensus); **excepciones** solo en plano de control; **códigos de wire** (`errorCode:i16`) en el protocolo, traducidos al modelo interno **en el borde**; en REST, **RFC 7807**. Corutinas devuelven `task<expected<T>>`.
- **Naming:** tipos `PascalCase`; funciones/variables/miembros `snake_case`; constantes `kPascalCase`; miembros privados con sufijo `_`; **sin** prefijo `I` en interfaces. Identificadores **en inglés**.
- **Documentación (Doxygen):** doc-comments **en español**, identificadores **en inglés**. `@file`/`@brief`/`@ingroup` por subsistema; en cada tipo, anotación de **afinidad** (`REACTOR-LOCAL` / `INMUTABLE` / `CROSS-CORE` / `THREAD-SAFE`) e invariantes. Estilo de marcador `///`.
- **Calidad de build:** `-Wall -Wextra -Wpedantic -Werror`; **sanitizers** (ASan/UBSan/TSan) en la build de pruebas; `clang-tidy` + `clang-format` (`.clang-format`/`.clang-tidy` versionados) + `cppcheck`.
- **TDD obligatorio:** rojo → verde → refactor en la lógica de dominio. Tests **deterministas** (inyectar reloj/red virtuales; nada *flaky*). GoogleTest; nombres `Metodo_Escenario_ResultadoEsperado`. Property-based en serialización; fuzzing en parsers.
- **RAII estricto:** sin `new`/`delete` crudos ni punteros propietarios crudos (los allocators de bajo nivel van **confinados** en un tipo RAII). Semántica de valor; `const`-correcto; `enum class`.
- **Fasing (no te adelantes):** **Fase 1 = monohilo + I/O bloqueante** (cero reactor, cero io_uring). El reactor llega en **1b**, Raft en **2**, ingress/observabilidad en **3**, *stretch* en **4**.

## Normativa de la biblioteca (autoridad de convenciones — IMPORTADA)

La `BibliotecaDocumentacion` está clonada como **carpeta hermana** (`../BibliotecaDocumentacion`).
Síguela **sin excepciones silenciosas**.

@../BibliotecaDocumentacion/stacks/cpp/convenciones.md
@../BibliotecaDocumentacion/principios/naming-y-estilo.md
@../BibliotecaDocumentacion/principios/manejo-errores.md
@../BibliotecaDocumentacion/principios/comentarios-y-documentacion.md
@../BibliotecaDocumentacion/principios/testing.md
@../BibliotecaDocumentacion/principios/git-workflow.md
@../BibliotecaDocumentacion/principios/clean-architecture.md
@../BibliotecaDocumentacion/principios/solid.md
@../BibliotecaDocumentacion/patrones/plantilla-adr.md
@../BibliotecaDocumentacion/patrones/inyeccion-dependencias.md
@../BibliotecaDocumentacion/fundamentos/concurrencia/convenciones.md
@../BibliotecaDocumentacion/fundamentos/sistemas-y-os/convenciones.md
@../BibliotecaDocumentacion/fundamentos/memoria/convenciones.md
@../BibliotecaDocumentacion/fundamentos/rendimiento/convenciones.md
@../BibliotecaDocumentacion/fundamentos/datos-distribuidos/convenciones.md
@../BibliotecaDocumentacion/fundamentos/redes/convenciones.md
@../BibliotecaDocumentacion/herramientas/docker.md
@../BibliotecaDocumentacion/herramientas/api-rest.md
@../BibliotecaDocumentacion/herramientas/seguridad.md
@../BibliotecaDocumentacion/herramientas/entrega-continua.md

> Si la ruta hermana no existiera en algún entorno, consultar al autor antes de seguir
> (no inventar convenciones). El detalle extenso vive en los `referencia.md` de cada tema.
