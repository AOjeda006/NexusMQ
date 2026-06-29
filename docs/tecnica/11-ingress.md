# 11. Ingress

> La capa de entrada: cómo llegan los clientes al broker. NexusMQ ofrece **dos modos** con
> jerarquía explícita —nativo directo (primario) y proxy/REST (secundario)— en lugar de "un
> ingress que lo hace todo". Decisiones:
> [ADR-0006](../adr/adr-0006-ingress-dos-modos.md),
> [0027](../adr/adr-0027-modo-proxy-upstream-pool.md),
> [0019](../adr/adr-0019-tls-opcional-openssl-bios.md),
> [0018](../adr/adr-0018-rest-admin-puerto-adaptador.md).

## 11.1 Dos modos, un *trade-off* explícito

En un plano de datos de alto rendimiento, interponer un proxy añade un salto y rompe el
*zero-copy*; pero exigir siempre un *smart-client* excluye a clientes simples y a HTTP. La
respuesta son **dos modos** (ver [diagrama 19](../diagramas/19-modo-nativo-vs-proxy.md)):

- **Nativo directo (primario).** Un *smart-client* consulta *metadata* y va **directo al
  líder** de la partición, sin proxy. Es el camino óptimo en latencia; el cliente gestiona el
  descubrimiento de líder y reacciona a `NOT_LEADER_FOR_PARTITION`.
- **Proxy/REST (secundario, *opt-in*).** El *ingress* enruta clientes "tontos" (*consistent
  hashing*) y expone un **gateway REST**, asumiendo el salto extra a sabiendas.

## 11.2 Modo proxy: *upstream pool* por reactor

El cableado del modo proxy ([ADR-0027](../adr/adr-0027-modo-proxy-upstream-pool.md)) mantiene
un **pool de conexiones aguas arriba por reactor** (`UpstreamPool`), coherente con
shared-nothing: cada reactor reutiliza sus propias conexiones al broker destino (resuelto vía
`PeerDirectory`), sin compartir *sockets* entre núcleos. Esto evita reabrir conexión por
petición (el *handshake* TCP+TLS cuesta RTTs) y mantiene la localidad del modelo
*thread-per-core*.

## 11.3 TLS opcional sobre el proactor

La terminación TLS 1.3 se hace con **OpenSSL** mediante un **puente de BIOs de memoria** sobre
el `Proactor` ([ADR-0019](../adr/adr-0019-tls-opcional-openssl-bios.md)): OpenSSL procesa el
*handshake* y el cifrado **en memoria** (memory BIOs), mientras el socket asíncrono mueve los
bytes cifrados; así una librería de TLS síncrona convive con la I/O por *completions* sin
acoplarse a ella (ver [diagrama 20](../diagramas/20-puente-tls-bios.md)). La dependencia es
**opcional**: se compila si `find_package(OpenSSL)` la encuentra (`NEXUS_HAVE_OPENSSL`); si
falta, el nodo **degrada a texto en claro** en lugar de no compilar. Se contempla **mTLS**
intra-cluster (ver [capítulo 27](./27-seguridad.md)).

## 11.4 Gateway REST de administración

El plano de control HTTP (`/api/v1`, salud, métricas) se sirve por un **adaptador**
(`AdminService` en `nexus-ingress`, `AdminApi` en `nexus-server`) en su propio puerto
([ADR-0018](../adr/adr-0018-rest-admin-puerto-adaptador.md)), sin acoplar el núcleo al
framework web. El detalle del contrato está en el
[capítulo 15](./15-api-rest-administracion.md).

## 11.5 Resiliencia

El ingress aplica los patrones de estabilidad clásicos (Nygard):

- **Circuit Breaker** (estados *closed/open/half-open*, ventana deslizante de errores) para no
  insistir contra un destino caído.
- **Bulkhead:** *pools* de conexiones aislados por destino, para que un fallo no agote todo.
- **Retry con *backoff* exponencial + *jitter*** (con tope) sobre operaciones idempotentes.
- **Rate limiting** con *token bucket* por cliente/*topic*.
- **Health checks** activos (ping periódico) y pasivos (errores en tráfico real).
- **Backpressure por créditos** propagado hasta el cliente, en lugar de descartar en silencio
  (ver [capítulo 7](./07-concurrencia.md)).
