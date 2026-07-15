# Validación E2E en vivo (smoke de binarios reales)

> Registro **as-built** de las pruebas de humo end-to-end sobre los binarios reales (`nexusd`,
> `nexus-cli`), complementarias a la suite automática de GoogleTest. Mientras la suite ejerce el
> dominio en proceso, este smoke arranca el **daemon real** y valida el *composition root*, el ciclo
> de vida (arranque, readiness, apagado por señal con drenaje), la CLI, **toda** la superficie REST
> —incluidas las rutas nuevas— y TLS. Se ejecuta en las **dos** plataformas soportadas.

## Última ejecución — 2026-07-15

| Plataforma | Toolchain | Backend de E/S | Resultado |
| ---------- | --------- | -------------- | --------- |
| Linux (WSL, Ubuntu) | GCC 15.2 / libstdc++ · Clang 21.1 / libc++ | io_uring | **24/24 PASS** |
| Windows 10 (Pro for Workstations) | MSVC 14.51 (VS 2026) | IOCP | **23/23 PASS** |

Puerta de calidad previa (árbol commiteado, emparejando el CI): GCC **949/949**, Clang/libc++
**949/949**, ASan **949/949**, `clang-tidy` (src/) **0** avisos, `clang-format --dry-run --Werror`
(v18, la del runner) **0** violaciones.

### Qué se ejerció (ambas plataformas)

- **Ciclo de vida:** arranque del daemon, `GET /readyz` → 200, y **apagado limpio** con una conexión
  SSE abierta (Linux: `SIGTERM`; Windows: `SetConsoleCtrlHandler`). Salida sin colgar en ~110 ms
  (Linux) / ~217 ms (Windows), *exit code* 0: el bucle SSE observa el drenaje (`admin_draining_`) y
  cierra, según [ADR-0038](adr/adr-0038-streaming-sse-admin-http.md).
- **Salud/observabilidad (sin auth):** `/healthz`, `/metrics`, y `GET /api/v1/metrics/snapshot`
  (JSON con `"metrics"`).
- **JWT:** `GET /api/v1/topics` sin token → **401**; con un HS256 válido (firmado con `--jwt-secret`)
  → **200**.
- **REST topics:** listado (aparece el topic pre-creado), `POST` → **201**, `describe` trae el objeto
  `config`, `PATCH` de retención → **200** y **surte efecto** (el `GET` posterior refleja
  `retentionMs`), `PATCH` de `segmentBytes` → **400** (create-only).
- **REST grupos/clúster:** `GET /api/v1/groups` → 200, `GET /api/v1/groups/{inexistente}` → **404**,
  `GET /api/v1/cluster` → 200 con JSON de nodos/particiones/Raft.
- **SSE:** `GET /api/v1/stream` emite `event: metrics` + `data: {…}` (`text/event-stream`).
- **CLI real:** `topic list/create/describe/delete` y `group list` con `--token`.
- **TLS (plano de datos):** *handshake* completo contra el puerto de datos (cert `CN=localhost`,
  TLSv1.3).

### Nota sobre la retención (ADR-0036)

El smoke valida que la **config de retención mutable** surte efecto en vivo (`PATCH` → `GET`). El
**recorte físico** de segmentos sellados no se fuerza en el smoke (el `segment.bytes` por defecto son
64 MiB y no se rellenan con un productor de humo): queda cubierto de forma **determinista** por la
suite —`topic_manager_test` invoca `enforce_retention_all` con un **reloj inyectado** y comprueba la
reclamación, que es exactamente el camino que dispara el barrido periódico por núcleo— y por
`partition_log_test`. El smoke confirma además que el barrido está **cableado** y que el daemon lo
ejecuta sin incidencias.

### Reproducir

Los guiones de smoke viven fuera del árbol (scratchpad de la sesión); son `bash` autocontenidos que
usan `curl`/`openssl`. En Windows se corren desde Git Bash contra los `.exe` de `build/windows-msvc`
(rutas de fichero para `openssl`/`nexusd` en formato Windows con barra, `cygpath -m`).
