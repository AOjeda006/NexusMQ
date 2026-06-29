# ADR-0021: Backend **IOCP** (Windows) — diseño fijado, **implementación diferida**

- **Estado:** reemplazado por adr-0022
- **Fecha:** 2026-06-20

> **Cierra F10** (Fase 4, *stretch*, marcada «*[limitado]* — no verificable en este entorno»): decide **cómo** encajaría IOCP en la arquitectura y **por qué** la implementación se difiere.

## Contexto

F10 pide un backend de E/S para **Windows** (IOCP) en `nexus-io` y un preset `windows-msvc`. IOCP es, como io_uring, un modelo **proactor** (se inicia la operación y el SO notifica al completarse), así que conceptualmente encaja en el puerto `Proactor` (ADR-0002/0005) sin tocar el bucle del reactor. Pero hay dos obstáculos duros:

1. **No es verificable en este entorno:** el desarrollo y el CI son **solo Linux** (WSL), sin *toolchain* MSVC ni Windows SDK; no se puede compilar ni probar nada de Win32 aquí, y la normativa prohíbe *pushear* código no verificado como si funcionara.
2. **Alcance real:** NexusMQ es **Linux-nativo de arriba abajo** — `io_uring`, `O_DIRECT`, `eventfd`, sockets POSIX, `fcntl`, *file descriptors* `int` en el propio puerto `Proactor`. Un backend IOCP útil exige **portar también `File` y `Socket`** a Win32 (`HANDLE`/`SOCKET`, `ReadFile`/`WriteFile` con `OVERLAPPED`, Winsock, `AcceptEx`), un esfuerzo transversal desproporcionado para un ítem *stretch*.

## Decisión

**Fijar el diseño y diferir la implementación.**

1. Se documenta el mapeo IOCP→`Proactor` operación a operación en `src/io/iocp_backend.hpp` (`IocpBackend : Proactor`, *pimpl* que oculta `<windows.h>`/`<winsock2.h>`): `CreateIoCompletionPort` para el puerto y asociación de cada `HANDLE`/`SOCKET`; `ReadFile`/`WriteFile` (con `Offset`/`OffsetHigh`), `WSARecv`/`WSASend`, `AcceptEx`, `FlushFileBuffers`, *waitable timers*; `GetQueuedCompletionStatusEx` para drenar *completions* en **lote** (espera con *timeout* hasta el `deadline`); `PostQueuedCompletionStatus` para `wake`; traducción de `dwNumberOfBytesTransferred`/`GetLastError` al `result` estilo io_uring del puerto.
2. Todo el encabezado va bajo `#ifdef _WIN32` y **sin `.cpp`**: en Linux es vacío, no entra en la build ni en la puerta de calidad (no rompe nada).
3. Se añade el preset `windows-msvc` como **andamio**, oculto fuera de Windows por `condition`.

## Consecuencias

- (+) El diseño Windows queda **registrado y concreto** (declaración + mapeo), demostrando que el puerto `Proactor` es portable a otro proactor sin cambiar el reactor.
- (+) Cero impacto en el árbol Linux y en CI (encabezado vacío bajo `#ifdef`, preset condicionado).
- (+) Honestidad de verificación: no se publica un IOCP «de mentira» sin compilar.
- (−) F10 **no** entrega un backend funcional: queda como diseño + andamio.
- (−) El port real requerirá, antes que `IocpBackend`, abstraer `File`/`Socket` del POSIX (trabajo futuro fuera de Fase 4).

## Alternativas consideradas

- **Implementar `IocpBackend` completo (con `.cpp`) ahora:** imposible de compilar/verificar aquí; sería código no probado contra la normativa (TDD, «nunca en rojo»). Descartado.
- **Job de CI en *runner* Windows:** GitHub ofrece `windows-latest`, pero el árbol entero no compila en MSVC (POSIX por doquier); habilitar un build Windows real es el *port* completo, no F10. Descartado por alcance.
- **Capa de compatibilidad POSIX en Windows (Cygwin/MSYS2 o Winsock+wepoll):** permitiría reusar el código POSIX, pero no es IOCP nativo (objetivo de F10) y añade dependencias; descartado.
