# 24. Portabilidad

> Linux primero, Windows después —pero de verdad—. El objetivo primario es **Linux x86-64**;
> el port a Windows recorrió tres niveles de verificación hasta funcionar en runtime con MSVC.

## 24.1 Una abstracción pensada para portar

La portabilidad se diseñó desde el principio, no se parcheó al final: la I/O se modela como
**proactor** (*completions*), la forma común a **io_uring** (Linux) e **IOCP** (Windows)
([ADR-0002](../adr/adr-0002-modelo-io-asincrona-proactor.md)). El núcleo es independiente de
plataforma tras ese puerto; cambiar de SO es cambiar de **adaptador** (`io_uring_backend` ↔
`iocp_backend`) y de *preset* (`linux-gcc`/`linux-clang` ↔ `windows-msvc`/`windows-clang-cl`),
sin reestructurar. Los tipos portables `NativeHandle`/`IoResult` aíslan el resto.

## 24.2 Tres niveles de "funciona"

El port a Windows hace explícita una distinción que suele pasarse por alto —**compila ≠
funciona**— recorriéndola por estados (cadena de ADR):

| Nivel | Qué garantiza | Hito |
| ----- | ------------- | ---- |
| **Diseño** | El backend está diseñado (`iocp_backend.hpp`, preset `windows-msvc`); implementación diferida. | [ADR-0021](../adr/adr-0021-iocp-diseno-diferido.md) |
| **Compile-verified** | Compila con headers Win32 reales (MinGW-w64); `File`/`Socket`/`Listener` Win32+Winsock, `IocpBackend` sobre `GetQueuedCompletionStatusEx`/`AcceptEx`. | [ADR-0022](../adr/adr-0022-iocp-implementado-mingw.md) |
| **Runtime-verified** | **Funciona** en Windows real con MSVC (VS 2026, `/W4 /WX`): arnés `wincheck` con File (bloqueante+directa), eco IOCP por *loopback* y `submit_timer` — 4/4 PASAN. | [ADR-0023](../adr/adr-0023-iocp-runtime-msvc.md) |

El valor real llegó al **verificar en runtime**: compilar no demostraba que el ciclo
*submit→completion* funcionara de extremo a extremo.

## 24.3 Port completo de `nexusd`

[ADR-0028](../adr/adr-0028-port-completo-nexusd-windows.md) amplía el alcance de "solo
`nexus-io`" a **`nexusd` completo** en Windows: backend, **afinidad** y **señales** por
plataforma. `make_default_proactor` elige io_uring o IOCP; la afinidad usa la API de cada SO
(`SetThreadAffinityMask`); el apagado limpio atiende las señales equivalentes
(`SetConsoleCtrlHandler`). Así el servidor entero —no solo la capa de I/O— corre en ambos SO.

## 24.4 Diferencias por compilador

Portar no es solo el SO, también el **compilador**. El único arreglo de fondo que necesitó el
runtime de Windows fue **`crc32c.cpp`**: GCC/Clang usan *builtins* para detectar e invocar las
instrucciones de CRC de SSE4.2; MSVC necesita `__cpuid` + los **intrínsecos** SSE4.2
explícitos. La detección de *CPU features* y su uso se seleccionan con `#if` por compilador,
manteniendo un *fallback* software portable. Es un recordatorio de que las mismas instrucciones
hardware se expresan distinto en cada *toolchain* —el mismo motivo por el que el proyecto
compila siempre en los **dos compiladores** (ver
[capítulo 22](./22-puerta-de-calidad-y-cicd.md)).
