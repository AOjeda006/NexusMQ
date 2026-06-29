# ADR-0022: Backend **IOCP** (Windows) — **implementado** y compile-verificado con MinGW (reemplaza ADR-0021)

- **Estado:** reemplazado por adr-0023
- **Fecha:** 2026-06-20

> **Reemplaza ADR-0021.** Aquel difirió la implementación IOCP por considerarla *no verificable en este entorno* (sin MSVC/SDK; CI solo Linux). Esa premisa **ya no se sostiene**: el entorno **sí** dispone (vía `apt`) del cross-compiler **MinGW-w64** con headers Win32 reales, que permite **compilar y enlazar** el código Windows sin una máquina Windows. Decisión del autor: llevar Windows al mismo nivel de desarrollo que Linux.

## Contexto

ADR-0021 dejó `iocp_backend.hpp` como diseño bajo `#ifdef _WIN32` y `File`/`Socket` como POSIX puro, con `int fd` en el puerto `Proactor`. El obstáculo declarado era la verificación. Con MinGW-w64 disponible, el código Win32 se puede compilar con `-Wall -Wextra -Wpedantic -Werror` contra los headers reales (`<windows.h>`, `<winsock2.h>`, `<mswsock.h>`) y enlazar contra `ws2_32`/`mswsock` — cubriendo la inmensa mayoría de errores (firmas, uso de API, tipos). Solo queda fuera la verificación en **runtime** (requiere Windows real), que se asume como deuda explícita acotada.

## Decisión

**Implementar el port Windows de la capa de E/S y verificarlo por compilación+enlace con MinGW.**

1. `NativeHandle` (= `int` en POSIX, `uintptr_t` en Windows) sustituye a `int fd` en el puerto `Proactor`, `File` y `Socket`; el `IoResult` del `Completion` se ensancha a `intptr_t` para alojar un `SOCKET`/`HANDLE` aceptado (en Linux, sin cambio de comportamiento).
2. `File` y `Socket`/`Listener` ganan su rama `#if defined(_WIN32)` (Win32: `CreateFileA`/`ReadFile`/`WriteFile` con `OVERLAPPED`, `FlushFileBuffers`, `SetEndOfFile`, `GetFileSizeEx`, `FILE_FLAG_NO_BUFFERING` como `O_DIRECT`; Winsock: `socket`/`connect`/`bind`/`closesocket`, `WSAStartup` RAII).
3. `iocp_backend.cpp` implementa `IocpBackend` sobre `CreateIoCompletionPort`/`GetQueuedCompletionStatusEx`/`PostQueuedCompletionStatus`, con `AcceptEx`, temporizadores por *timeout* de la espera y la maquinaria Win32 confinada en un *pimpl* `Port`.
4. CMake compila `iocp_backend.cpp` y enlaza Winsock en `WIN32`.
5. `tools/verify-windows-io.sh` reproduce la verificación cruzada.

## Consecuencias

- (+) `nexus-io` (la capa que aísla la plataforma) es **Windows-capaz y compile/enlace-verificada**, demostrando que el puerto `Proactor` es portable a otro proactor sin tocar el reactor.
- (+) Cero impacto en Linux: el cambio `NativeHandle`/`IoResult` es *type-identical* (alias de `int`/`intptr_t`), 599 tests verdes en GCC y Clang/libc++.
- (+) La verificación es **honesta**: compila y enlaza con toolchain Windows real, no es código «a ciegas».
- (−) **Runtime no verificado** en Windows (sin máquina/CI Windows): deuda explícita acotada — un job `windows-latest` lo cerraría al reactivar el CI.
- (−) El **resto del servidor** (reactor *pinning* con `pthread_setaffinity_np`, `nexusd` con señales POSIX/eventfd) sigue siendo Linux-nativo: un `nexusd` Windows completo es trabajo futuro fuera de este alcance; lo entregado es la **capa de E/S** portable.

## Alternativas consideradas

- **Mantener ADR-0021 (solo diseño):** desaprovecha que MinGW está disponible; deja Windows sin avanzar pese a poder verificarlo. Descartado por decisión del autor.
- **Solo refactor portable (sin los `.cpp` Win32):** verificable en Linux pero no entrega el backend. Descartado a favor del port completo.
- **Job de CI en `windows-latest` para runtime:** cerraría la deuda de runtime, pero el CI está desactivado por cuota (se reactiva al publicar) y el árbol completo aún no compila en Windows (POSIX fuera de `nexus-io`). Aplazado.
