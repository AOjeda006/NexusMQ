# ADR-0010: Entorno de desarrollo (VS Code sobre WSL; reemplaza el IDE de ADR-0001)

- **Estado:** aceptado (reemplaza la elección de IDE de ADR-0001)
- **Fecha:** 2026-06-11

> **Reemplaza la elección de IDE de ADR-0001.** El resto de ADR-0001 (*target* Linux primero, WSL2, CMake + vcpkg, Windows después) **sigue vigente**; ADR-0001 no se edita (un ADR aceptado se reemplaza, no se modifica).

## Contexto

ADR-0001 fijó desarrollar **desde Visual Studio** (workload "Linux development with C++") sobre WSL2. El código del proyecto vive **dentro del sistema de ficheros de WSL** (`/home/…`, visible desde Windows como `\\wsl.localhost\…`).

El modelo CMake+WSL de Visual Studio **asume que el código reside en el FS de Windows** y lo sincroniza a WSL con `rsync`; al abrir una carpeta que ya vive en WSL sobre el *share* `\\wsl.localhost`, VS **no activa la integración CMake** (no genera caché, no aparecen *targets* ni "Elemento de inicio"), con cuelgues al cerrar. Se verificó con **VS 2026 (18.7)**: presets válidos (`cmake --list-presets` OK), `enableCMake: true` y componentes Linux/WSL instalados — el bloqueo es de **diseño de VS**, no de configuración.

## Decisión

Migrar el IDE a **VS Code conectado a WSL** (extensiones **Remote-WSL** + **CMake Tools** + **C/C++**), abriendo el proyecto en su ruta Linux real (`code .` desde WSL). CMake Tools consume el **mismo `CMakePresets.json`** y permite configurar, compilar, ejecutar y **depurar (gdb)** *nativo* en WSL, sin copias ni `rsync`. El código permanece en WSL (mejor I/O que `/mnt/c`).

Se **estandariza** en los presets `linux-gcc/clang/asan` (Ninja, los mismos que el CI) y se eliminan los presets `wsl-ubuntu*` (Makefiles) y los *vendor maps* `microsoft.com/VisualStudioSettings` introducidos para VS.

## Consecuencias

- (+) El IDE trabaja donde vive el código: sin `rsync`, sin cuelgues, **un único `CMakePresets.json`** para IDE, CI y terminal, con idéntico *toolchain* (gcc/gdb/cmake).
- (+) Experiencia gráfica equivalente (configurar/compilar/ejecutar/depurar).
- (+) Menos superficie de configuración (un solo conjunto de presets).
- (−) Se renuncia a Visual Studio como IDE (preferencia previa del autor) para este proyecto.
- (−) Recuperar VS exigiría mover el repo al FS de Windows — descartado por el flujo *Linux-first*.

## Alternativas consideradas

- **Mover el repo a `C:\` y seguir en Visual Studio (modelo `rsync` de VS):** funcional, pero el origen pasa a Windows, con peor rendimiento de I/O en WSL2 y fricción de *line endings*/permisos; contradice *Linux-first*. Descartado.
- **Forzar VS sobre `\\wsl.localhost`:** no es un flujo soportado; es la causa de la no-activación y de los cuelgues. Descartado.
- **VS Code + Remote-WSL:** elegido (ver Decisión).
