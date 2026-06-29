# ADR-0015: RaftNode como máquina de estados síncrona sin E/S (entradas→salidas)

- **Estado:** aceptado
- **Fecha:** 2026-06-14

## Contexto

Este ADR **refina el desglose** (no cambia ADR-0003 ni la mecánica de Raft): el desglose detallado modela `RaftNode` con `propose`/`replicate_to` como **corrutinas** y un puerto `RaftTransport` (`task<expected<…>> send_append/send_vote`). La decisión reorganiza esa frontera sin alterar el algoritmo.

El objetivo nº1 de Fase 2 es la **correctitud demostrable** de Raft mediante **simulación determinista** con reloj y red virtuales (§8.1). Si `RaftNode` hace E/S por dentro (corrutinas que `co_await` un transporte), el reloj y la red quedan **dentro** del nodo y la prueba determinista exige inyectar un proactor/transport asíncrono y conducir corrutinas — friccionando con FIRST y con la reproducibilidad. El patrón estándar de implementaciones de Raft probadas (etcd/raft, TigerBeetle, ongaro) es un **núcleo sin E/S**: el nodo consume *entradas* (ticks de reloj, RPC recibidos) y produce *salidas* (mensajes a enviar, entradas a aplicar); el tiempo, la red y el disco viven **fuera**.

## Decisión

`RaftNode` (`src/consensus/raft_node.hpp/.cpp`) es una **máquina de estados síncrona y sin E/S**: sin `RaftTransport`, sin corrutinas, sin reloj propio.

- **Entradas:** `tick(now)`, `on_request_vote(now, args) → reply`, `on_append_entries(now, args) → reply`, `on_request_vote_reply(now, from, reply)`, `on_append_entries_reply(now, from, reply)`, `propose(batch)` (C6).
- **Salidas:** una cola de mensajes salientes proactivos (`RaftMessage{from, to, payload}` con `payload` = `variant` de los RPC) drenable con `take_messages()`; las entradas confirmadas se exponen vía `commit_index()` (el broker lee el log hasta ahí).

El **reloj se inyecta** como `MonoTime now` en cada entrada (los *timeouts* de elección/heartbeat son aritmética pura sobre `now`); la **aleatorización** del *election timeout* usa un RNG sembrado (`random_seed + self`), reproducible. En producción, un adaptador del reactor traduce: temporizador→`tick`, RPC recibido→`on_*`, mensajes de `take_messages()`→envíos por `nexus-wire` (se cablea en la integración con el broker, C9). Respecto al desglose: se **añade** `now` a las firmas de los manejadores (determinismo) y se **sustituye** el par `propose: task<…>` + `RaftTransport` por `propose` síncrono + cola de salida (el adaptador asíncrono vive fuera del núcleo).

## Consecuencias

- (+) Simulación **determinista** trivial: el arnés (C8) tiene un reloj virtual y una red virtual (cola de mensajes con retardos/particiones programables) y dirige N `RaftNode` reproduciblemente; cero hilos, cero E/S real en el test del algoritmo.
- (+) El núcleo de Raft se razona y prueba aislado de io_uring/corrutinas.
- (+) Encaja con *shared-nothing*: un `RaftNode` por partición, dirigido por su reactor.
- (−) Hace falta un **adaptador** (reactor↔nodo) que el desglose no nombraba (se añade en C9).
- (−) El emisor debe drenar `take_messages()` tras cada entrada (contrato explícito).

## Alternativas consideradas

- **`RaftNode` con corrutinas + `RaftTransport` (desglose literal):** menos piezas en producción, pero mete reloj/red dentro del nodo y complica la prueba determinista (el objetivo nº1); descartado.
- **Núcleo sin E/S con *callbacks* de envío** (en vez de cola drenable): equivalente, pero los *callbacks* reintroducen acoplamiento y dificultan inspeccionar las salidas en el test; se prefiere la cola explícita.
