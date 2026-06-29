# ADR-0027: Cableado del modo proxy en el plano de datos — *pool* de conexiones aguas arriba por reactor

- **Estado:** aceptado
- **Fecha:** 2026-06-25

## Contexto

ADR-0006 definió el *ingress* en **dos modos**: nativo directo (primario, *smart-client* al líder) y **proxy** (secundario, *opt-in*: enruta clientes "tontos" por *consistent-hashing* y releva sus tramas a un nodo). I18 implementó la lógica enrutable y testeable del proxy (`Proxy::route` sobre el anillo del `LoadBalancer` y `Proxy::forward`, relevo petición/respuesta **a nivel de trama** entre dos `Socket` ya conectados), pero **difirió** al cableado de servidor (D4-3) el *dialado* del nodo aguas arriba y el *pool*/reúso de esas conexiones.

Faltan dos piezas para activar el modo en el plano de datos: (a) **establecer** la conexión al plano de datos del nodo elegido **sin bloquear** el reactor, y (b) **reutilizarla** (un *handshake* TCP por petición cuesta RTTs; la normativa de redes exige *connection pooling*). Además, el `PeerDirectory` (ADR-0025) resuelve solo el **plano inter-nodo (Raft)**, en un puerto **separado** del plano de datos: el proxy no puede reusarlo para dialar el puerto de cliente.

## Decisión

1. **`UpstreamPool` por reactor, REACTOR-LOCAL.** Cada reactor mantiene su propio *pool* —sin locks, coherente con shared-nothing (ADR-0005)—. `acquire(proactor, node)` devuelve una conexión **viva y en préstamo exclusivo** al nodo: reúsa una ociosa de la *free-list* del nodo o **diala una nueva** de forma asíncrona (`Socket::async_connect`) si no hay. `release(node, socket)` la devuelve a la *free-list* para reúso. El préstamo es **exclusivo** mientras dura el relevo de una conexión de cliente (el relevo es petición/respuesta a nivel de trama, I18): así nunca se **intercalan** tramas de dos clientes en la misma conexión aguas arriba (que corrompería el flujo). La *free-list* por nodo está **acotada** (cierra el excedente) para no fugar descriptores ni crecer sin límite (colas acotadas, normativa de concurrencia/datos-distribuidos).

2. **Directorio de direcciones del plano de datos, distinto del de Raft.** El `UpstreamPool` resuelve `NodeId → host:puerto` del **plano de cliente** mediante un `PeerDirectory` poblado con direcciones del plano de datos (instancia **separada** de la del plano Raft de ADR-0025). Se **reutiliza el tipo** `PeerDirectory`/`PeerAddress` (estructura idéntica: `NodeId → host:puerto`, inmutable, thread-safe de solo lectura) en vez de duplicar un tipo casi igual (minimiza entidades, Kent Beck 4); la distinción Raft vs datos es por **instancia y composición**, no por tipo. El *composition root* construye y posee ambos directorios.

3. **Activación opt-in.** El modo proxy del plano de datos es **opt-in** (ADR-0006): sin configurarlo, el servidor atiende en modo nativo directo (cada conexión la sirve `serve_connection` localmente). Con el modo activo, la conexión de cliente se enruta (`Proxy::route`) a un nodo y se releva (`Proxy::forward`) sobre una conexión obtenida del `UpstreamPool`, devuelta al *pool* al cerrar limpiamente el cliente.

## Consecuencias

- (+) El proxy reutiliza conexiones TCP aguas arriba (ahorra *handshakes*/RTTs), cumple la normativa de redes y mantiene shared-nothing (un *pool* por reactor, sin contención).
- (+) El dialado es **asíncrono** (no congela el reactor).
- (+) Préstamo exclusivo ⇒ sin intercalado de tramas ⇒ corrección del relevo.
- (−) Una conexión ociosa reusada puede haber sido cerrada por el par mientras esperaba; el primer uso fallará y el llamante la descarta (un *health-check*/reintento de un salto queda como **mejora futura**, no en este corte).
- (−) Otro estado por reactor, que vive y se destruye en orden con el *pool* de reactores.
- (−) El proxy añade un salto y rompe el *zero-copy* (coste asumido y documentado en ADR-0006).

## Alternativas consideradas

- **Pool global compartido entre reactores (con lock):** rompe shared-nothing (ADR-0005), introduce contención y *cache ping-pong* en el *hot path*; contradice la normativa de concurrencia. Descartada.
- **Dialar una conexión nueva por cada conexión de cliente, sin pool:** simple, pero paga un *handshake* TCP por cliente y no reúsa nada; contradice la normativa de redes (*connection pooling*). Se conserva como **caso degenerado** (free-list vacía), no como diseño. Descartada como único.
- **Reúso concurrente de una conexión aguas arriba entre varios clientes (multiplexado por `correlation_id`):** maximizaría el reúso, pero exige **remapear** `correlation_id` y casar respuestas fuera de orden; mucho más complejo y propenso a errores. El relevo de I18 es por conexión; se mantiene el préstamo exclusivo. Descartada (posible evolución futura).
- **Añadir el puerto de datos al `PeerDirectory` de Raft (un solo directorio con dos puertos):** acopla dos planos con ciclos de vida y semánticas distintas en un mismo tipo; se prefieren **dos instancias** del mismo tipo. Descartada.
