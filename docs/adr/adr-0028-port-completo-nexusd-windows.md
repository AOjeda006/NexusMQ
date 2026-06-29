# ADR-0028: Port **completo** de `nexusd` a Windows — backend, afinidad y señales por plataforma

- **Estado:** aceptado
- **Fecha:** 2026-06-27

## Contexto

ADR-0023 dejó `nexus-io` (File/Socket/Listener + `IocpBackend`) **verificado en runtime** en Windows, pero ese alcance se limitaba a `nexus-io`: el resto del árbol seguía Linux-nativo y **no compilaba** en Windows. Este ADR amplía aquel alcance "solo `nexus-io`" al daemon completo (W3). Los puntos que bloqueaban la compilación del árbol entero eran:

- la **factoría de proactor** del `Server` construía siempre `IoUringBackend` (declarado en todas las plataformas pero **definido solo en Linux** → fallo de enlace),
- el *pinning* del `ReactorPool` usaba `pthread_setaffinity_np`/`cpu_set_t`,
- `nexusd` registraba señales POSIX,
- quedaban fugas POSIX en hojas (cliente/CLI bloqueantes con sockets BSD, `gmtime_r`, `std::filesystem::path::c_str()` que es `wchar_t*` en Windows y choca con las APIs `const char*` de OpenSSL).

El valor de portabilidad —que el puerto `Proactor` admite otro proactor— ya estaba demostrado por `nexus-io`; faltaba **extenderlo al daemon entero** (W3).

## Decisión

**Portar todo el árbol a Windows confinando la divergencia tras la abstracción `nexus-io` (`Proactor`) y guardas `#if defined(_WIN32)`, dejando el camino POSIX byte-idéntico.** Cuatro piezas:

1. **Factoría de proactor (compilación).** `Server` elige el backend por defecto en compilación: `IocpBackend` en Windows, `IoUringBackend` en Linux (`make_default_proactor`, antes `make_io_uring_proactor`). Ambos son **proactores sobre el mismo puerto `Proactor`** (ADR-0021/0023), así que el bucle del reactor, el `RequestRouter` y las corrutinas de servicio **no cambian**. La DIP sigue intacta: los tests inyectan su doble.

2. **Afinidad de hilo.** `pin_thread_to_core` usa `SetThreadAffinityMask(handle, mask)` en Win32 y `pthread_setaffinity_np`/`cpu_set_t` en POSIX, con el **mismo reparto** `core % cpus`. Mejor esfuerzo en ambos; en Windows la máscara cubre un grupo de procesadores (≤ 64 CPUs), suficiente para el objetivo.

3. **Señales / parada.** `nexusd` registra `SetConsoleCtrlHandler` en Windows (capta Ctrl-C, Ctrl-Break, cierre de consola, *logoff* y apagado, en un hilo aparte) y `std::signal(SIGINT/SIGTERM)` en POSIX. Ambos manejadores solo **despiertan** el servidor: `Server::stop()` marca un flag y llama `Reactor::stop()` → `Proactor::wake()`, que en IOCP es `PostQueuedCompletionStatus` (thread-safe). El "despertar por `eventfd`" era un detalle del backend io_uring; **la abstracción `wake()` ya era portable**, así que el plano de control no necesitó rediseño, solo el registro del manejador.

4. **Hojas POSIX.** Sockets bloqueantes del cliente nativo y del CLI → Winsock (`SOCKET`/`closesocket`, firma y signo de `send`/`recv`, sin `MSG_NOSIGNAL`, `WSAStartup` por proceso vía RAII como en `nexus-io`); `gmtime_r` → `gmtime_s` (argumentos invertidos); rutas a OpenSSL vía un `native_path` que en POSIX devuelve la propia `path` (coste cero) y en Windows materializa la string narrow. Y se alinea la **política de avisos** MSVC con la canónica de Linux desactivando dos avisos `/W4` benignos que el gate Linux no exige (`C4324` —padding por `alignas` *deliberado* en las colas SPSC/MPMC— y `C4456` —*shadowing* idiomático de variables de condición en cadenas `if/else-if`—).

## Consecuencias

- (+) **Todo el árbol** (`nexusd`, `nexus-cli`, `nexus-loadgen`, librerías) **compila y enlaza en Windows** con MSVC `/W4 /WX` **y** con clang-cl (W2), no solo `nexus-io`: la portabilidad del puerto `Proactor` queda demostrada para el daemon completo.
- (+) Un solo binario, dos plataformas; comportamiento **idéntico en Linux** (todo guardado o inerte).
- (+) El diseño `Proactor` de ADR-0021/0023 se confirma: bastó **elegir** el backend, sin tocar el reactor.
- (−) El **runtime** del daemon en Windows (smoke produce/fetch por IOCP) y los **tests** (`ctest`) se verifican en etapas aparte (mismo patrón que ADR-0023).
- (−) La afinidad en máquinas de **> 64 CPUs** es aproximada en Windows (un solo grupo de procesadores); aceptable para el objetivo de *locality*.
- (−) El **gate canónico** (Linux: GCC/libstdc++ + Clang/libc++ + ASan/TSan) **no se ejecuta en Windows**: el cambio se entrega en la rama `windows-port` y el autor lo revalida y fusiona desde una sesión Linux.

## Alternativas consideradas

- **Selección de backend por polimorfismo en runtime (un flag/registro):** innecesaria —los backends son **excluyentes por plataforma**, conocida en compilación—; un `#if` es coste cero y no añade estado. Descartada.
- **Capa de abstracción de sockets bloqueantes compartida:** solo dos ficheros la usan (cliente nativo y CLI); una guarda local por fichero es más simple que un tipo nuevo. Aplazada (si surge un tercer consumidor).
- **Quedarse con `std::signal(SIGINT)` también en Windows:** compila, pero **no capta** cierre de consola/logoff/apagado y `SIGTERM` no se levanta en Windows; `SetConsoleCtrlHandler` es idiomático y completo. Descartada.
- **Renombrar las variables que disparan `C4456` o reordenar las colas para `C4324`:** churn en código **canónico** por avisos que la política de Linux no exige; se prefiere **alinear la política MSVC** (espejo de los checks apagados en `.clang-tidy`), dejando el código compartido intacto. Descartada.
