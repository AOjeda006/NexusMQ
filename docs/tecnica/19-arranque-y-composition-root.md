# 19. Arranque y composition root

> Cómo cobra vida un nodo: `nexusd` como *composition root* donde se cablea todo, su
> configuración 12-factor y sus puertos. El cableado de dependencias ocurre **aquí**, no en
> el dominio (clean architecture: separar construcción de uso).

## 19.1 `nexusd` y el composition root

El ejecutable `nexusd` (`src/server/main.cpp`) es el **composition root**: lee la
configuración, construye el `Proactor` adecuado a la plataforma, levanta el `ReactorPool`
(un reactor por núcleo), instancia el broker, el stack Raft, el ingress y la telemetría, y los
**inyecta por constructor**. Ningún componente de dominio instancia su infraestructura: la
recibe ya construida (DIP, ver
[inyección de dependencias](./06-principios-de-diseno.md)). En C++ esto se materializa con
inyección por **constructor** (interfaz + `unique_ptr`) en los bordes y por **parámetro de
plantilla** (polimorfismo estático, coste cero) en los caminos calientes.

## 19.2 Configuración 12-factor

La configuración entra por **flags de línea de comandos** y variables de entorno (config en el
entorno, no en la imagen — [ADR-0008](../adr/adr-0008-coste-cero.md)), de modo que **el mismo
binario** corre en local o en cloud sin cambios. Los **secretos** (p. ej. el de JWT) van por
entorno, nunca horneados (ver [capítulo 27](./27-seguridad.md)). Flags principales de `nexusd`:

| Flag | Propósito |
| ---- | --------- |
| `--host` | Interfaz de escucha (p. ej. `0.0.0.0`). |
| `--port` | Puerto del **plano de datos** (cliente nativo; `9092` por convención). |
| `--admin-port` | Puerto del **plano de operación** (REST admin + salud + métricas; `9644`). |
| `--kafka-port` | Puerto del **listener Kafka** (subset; opcional). |
| `--data-dir` | Directorio de datos (logs de partición). |
| `--node-id` | Identificador del nodo en el cluster. |
| `--topic` | *Topic* inicial, formato `nombre:particiones` (p. ej. `demo:3`). |
| `--jwt-secret` | Activa la autenticación JWT en `/api/v1/*` (si se omite, REST sin auth). |
| `--tls-cert` | Cadena de certificado PEM del servidor; con `--tls-key` **activa TLS** en el plano de datos ([ADR-0019](../adr/adr-0019-tls-opcional-openssl-bios.md)). |
| `--tls-key` | Clave privada PEM del servidor (pareja de `--tls-cert`). |

> Los nombres y valores por defecto son contrato operativo; el catálogo completo de
> configuración (retención, `segment.bytes`, política de `fsync`, etc.) vive en el
> [capítulo 26](./26-configuracion-y-operacion.md).

## 19.3 Puertos y planos

`nexusd` separa los planos también en puertos (ver [diagrama 15](../diagramas/15-planos-red.md)):

- **Plano de datos / cliente** (`--port`): *produce*/*fetch* por el protocolo binario.
- **Plano Kafka** (`--kafka-port`): subset Kafka, interop `kcat`.
- **Plano de operación** (`--admin-port`): REST `/api/v1`, `/healthz`, `/readyz`, `/metrics`.
- **Plano inter-nodo Raft**: RPC de consenso entre réplicas, por su transporte separado
  ([ADR-0025](../adr/adr-0025-activacion-raft-multireactor-transporte.md)).

## 19.4 Arranque, afinidad y apagado limpio

En el arranque, cada reactor se **fija a su núcleo** (afinidad). El port a Windows hace esto y
las señales **por plataforma** ([ADR-0028](../adr/adr-0028-port-completo-nexusd-windows.md)):
`make_default_proactor` selecciona io_uring o IOCP; la afinidad usa la API de cada SO
(`SetThreadAffinityMask` en Windows). El **apagado limpio** atiende `SIGTERM`/`SIGINT`
(`SetConsoleCtrlHandler` en Windows) para **drenar** el trabajo en curso y cerrar recursos
antes de salir; en el camino crítico solo se usan funciones *async-signal-safe* (despertar el
bucle vía *self-pipe*/`eventfd`).
