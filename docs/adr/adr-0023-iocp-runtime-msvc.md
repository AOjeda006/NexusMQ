# ADR-0023: Backend **IOCP** (Windows) — **verificado en runtime** sobre Windows con MSVC (reemplaza ADR-0022)

- **Estado:** aceptado
- **Fecha:** 2026-06-20

> **Reemplaza ADR-0022.** Aquel **implementó y compile-verificó** el backend IOCP con MinGW-w64, pero dejó como deuda explícita acotada la verificación en **runtime** («Runtime no verificado en Windows … un job `windows-latest` lo cerraría»). Esa deuda **ya no aplica**: el código se ha **ejecutado** en una máquina Windows real con MSVC y funciona. El diseño no cambia; ADR-0023 lo eleva de *compile-verificado* a *runtime-verificado* (mismo patrón que ADR-0021→0022).

## Contexto

ADR-0022 cerró con `nexus-io` compile/enlace-verificado contra headers Win32 reales vía cross-compiler, pero **nunca ejecutado** (sin MSVC ni máquina Windows). MinGW (GCC) no ve las divergencias de MSVC (por ejemplo, `__builtin_*`, atributos `[[gnu::*]]`) ni cubre el **comportamiento en ejecución** (asociación de handles/sockets al puerto, `AcceptEx` + `SO_UPDATE_ACCEPT_CONTEXT`, `OVERLAPPED` en vuelo, drenado de `GetQueuedCompletionStatusEx`, temporizadores por *timeout*). El autor pasa a trabajar en **Windows real** con **Visual Studio 2026** (MSVC 19.51, CMake/Ninja *bundled*), lo que permite cerrar la deuda.

## Decisión

**Cerrar la deuda de runtime ejecutando `nexus-io` en Windows real con MSVC, mediante un arnés de verificación dedicado** (en lugar de un job de CI: el CI sigue desactivado por cuota y el árbol completo no compila en Windows).

1. El preset `windows-msvc` con `-D NEXUS_BUILD_TESTS=OFF` compila **solo** `nexus-io` (+ `nexus-common`) con MSVC `/W4 /WX /permissive-`.
2. `tools/wincheck` es un ejecutable **Windows-only** (`CMakeLists` bajo `if(WIN32)`, **sin** ctest ni `ReactorPool` —que es POSIX—) enlazado a `nexus::io` que **ejercita** la capa: `File` bloqueante (round-trip `write_at`/`read_at`, `size`/`sync`/`truncate`) y `File` con E/S directa (`FILE_FLAG_NO_BUFFERING`, búfer alineado a 4 KiB); **eco IOCP por loopback** end-to-end (`Listener::bind`/`Socket::connect` + `AcceptEx`/`WSARecv`/`WSASend`) con cierre limpio (EOF), bombeando `wait_completions`/`run_completions` a mano con un **driver de corrutinas** propio (`sync_wait` no sirve: la *task* se suspende en E/S asíncrona); y `submit_timer` (deadline corto que vence y reanuda).
3. **Resultado: los 4 casos PASAN** con MSVC 19.51 (VS 2026).

## Consecuencias

- (+) `nexus-io` deja de tener deuda de runtime: la portabilidad del puerto `Proactor` a otro proactor (IOCP) queda demostrada **ejecutándose**, no solo compilando.
- (+) MSVC `/W4 /WX` validó lo que MinGW no veía y reveló **una sola** divergencia real, de portabilidad de detección de *CPU features* en `nexus-common` (`crc32c.cpp`: `__builtin_cpu_supports`/`[[gnu::target("sse4.2")]]` de GCC → `__cpuid` + intrínsecos SSE4.2 incondicionales en MSVC, tras `#if defined(_MSC_VER)`); el camino GCC/Clang queda intacto.
- (+) El **diseño de ADR-0022 se confirma sin cambios**: ningún ajuste en el código IOCP/`File`/`Socket` fue necesario para que funcionara en runtime (las rutas Win32/Winsock estaban correctas).
- (−) La verificación es **manual** (ejecutar `tools/wincheck`), no automática: al reactivar el CI, un job `windows-latest` debería ejecutarlo para evitar regresiones (tarea de cierre).
- (−) Sigue siendo **solo `nexus-io`**: el resto del servidor (reactor *pinning* con `pthread_setaffinity_np`, `nexusd` con señales POSIX/eventfd, backend io_uring) es Linux-nativo; un `nexusd` Windows completo queda fuera de alcance.

## Alternativas consideradas

- **Mantener ADR-0022 (runtime como deuda):** ya no se sostiene —hay máquina Windows con MSVC disponible—; dejar la capa sin ejecutar nunca era el único hueco de F10. Descartado.
- **Job de CI en `windows-latest` en vez de arnés local:** cerraría la regresión de forma automática, pero el CI está desactivado por cuota y el árbol completo no compila en Windows; el arnés local es **ejecutable hoy**. No es excluyente: ese job ejecutaría precisamente `tools/wincheck`. Aplazado.
- **Convertir `wincheck` en test de ctest (GoogleTest):** mezclaría la verificación Windows con la suite Linux (que no compila en Windows) y arrastraría el *toolchain* de tests; un **ejecutable autónomo** es más simple, aislado y no depende de `NEXUS_BUILD_TESTS`. Descartado.
