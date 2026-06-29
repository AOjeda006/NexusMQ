# ADR-0001: Plataforma de desarrollo y *target* (Linux primero, Windows después)

- **Estado:** aceptado
- **Fecha:** 2026-06-07

## Contexto

El autor desarrolla en Windows con Visual Studio, pero el dominio del proyecto —un broker de mensajería— se despliega de forma natural en Linux y contenedores. Además, el ecosistema de *tooling* de alto rendimiento (perf, flamegraphs, io_uring, hugepages, NUMA) es Linux. Existe, por tanto, una tensión entre el entorno de trabajo preferido (Visual Studio) y la plataforma natural del dominio (Linux), y se desea no renunciar a ninguno de los dos.

## Decisión

Se adopta **Linux** (x86-64, Docker) como *target* primario, desarrollado **desde Visual Studio vía WSL2** (workload "Linux development with C++"), con **CMake + vcpkg** como sistema de build y de dependencias portables. El *target* **Windows nativo** queda como un **objetivo posterior comprometido** —no descartado—, que se aborda tras consolidar la plataforma Linux.

## Consecuencias

- (+) Encaje con el dominio y con el *tooling* esperado por los perfiles de sistemas/HFT, conservando a la vez el IDE preferido.
- (+) La portabilidad se diseña desde el inicio sin pagar por adelantado su coste completo.
- (+) La estructura de **solución única** (un único árbol CMake) **no bloquea** el *target* Windows posterior: el mismo árbol cambia de *preset* a MSVC y solo añade el adaptador **IOCP** en `src/io/`, sin reestructurar (ver §5.2).
- (−) Se asume la fricción de WSL2 y la de mantener un *toolchain* multiplataforma.

## Alternativas consideradas

- **Windows nativo (IOCP/RIO) como primario:** descartado como primario por encaje de dominio y de *tooling*, aunque sería un diferenciador; se mantiene como fase posterior.
- **Cross-platform total desde el día 1:** descartado por el coste de mantener dos backends de I/O y de fichero antes de tener producto.
