# Diagrama 20: Puente TLS — OpenSSL síncrono sobre el proactor asíncrono

NexusMQ termina **TLS 1.3** (y, intra-clúster, **mTLS**) en el *ingress* (§7.9). OpenSSL es la
librería elegida, pero su E/S es **síncrona/bloqueante**, mientras que el broker es
*thread-per-core* **asíncrono** sobre un `Proactor` (io_uring). La solución (ADR-0019) es un **puente
de BIOs de memoria**: OpenSSL nunca toca el descriptor; cifra/descifra contra dos `BIO` en memoria
(`rbio`/`wbio`) y el transporte se hace de forma asíncrona vaciando/alimentando esos BIOs por el
`Proactor`. Así la criptografía corre en línea, pero el socket es no bloqueante. Fuentes:
[`../adr/adr-0019-tls-opcional-openssl-bios.md`](../adr/adr-0019-tls-opcional-openssl-bios.md),
`src/ingress/tls.hpp` (`TlsContext`/`TlsConnection`).

```mermaid
sequenceDiagram
    autonumber
    participant App as App (reactor)<br/>handshake / async_recv / async_send
    participant SSL as OpenSSL + BIOs<br/>(SSL, rbio, wbio)
    participant IO as Socket async (Proactor)<br/>io_uring

    Note over App,IO: Handshake TLS 1.3 (rol fijado por TlsContext::accept/connect)

    App->>SSL: SSL_do_handshake()
    activate SSL
    SSL-->>App: WANT_WRITE (handshake record en wbio)
    deactivate SSL
    App->>SSL: lee ciphertext del wbio (flush_outgoing)
    App->>IO: async_send(ciphertext)
    IO-->>App: enviado
    App->>IO: async_recv() (feed_incoming)
    IO-->>App: ciphertext del par (0 = par cerró)
    App->>SSL: BIO_write(rbio, ciphertext)
    App->>SSL: SSL_do_handshake() (reintenta)
    activate SSL
    SSL-->>App: OK (handshake completo)
    deactivate SSL

    Note over App,IO: Datos de aplicación — async_send (cifra y envía)

    App->>SSL: SSL_write(plaintext)
    activate SSL
    SSL-->>App: ciphertext en wbio
    deactivate SSL
    App->>IO: async_send(ciphertext del wbio)
    IO-->>App: bytes aceptados

    Note over App,IO: Datos de aplicación — async_recv (recibe y descifra)

    App->>IO: async_recv() hacia el rbio
    IO-->>App: ciphertext del par
    App->>SSL: BIO_write(rbio) + SSL_read()
    activate SSL
    SSL-->>App: plaintext descifrado (0 = close_notify/EOF)
    deactivate SSL
```

## Cómo funciona el bucle (qué hace cada pieza)

- **`TlsContext`** (THREAD-SAFE, RAII sobre `SSL_CTX`): fábricas `server`/`client` cargan el material
  PEM (cadena de certificado, clave privada) y, si se da una CA, configuran **mTLS**
  (`SSL_VERIFY_PEER | FAIL_IF_NO_PEER_CERT`). `accept`/`connect` abren una `TlsConnection` fijando el
  rol del *handshake*.
- **`TlsConnection`** (REACTOR-LOCAL, *solo movible*): posee el `SSL*` y el `Socket`. Las primitivas
  OpenSSL (`SSL_do_handshake`/`SSL_read`/`SSL_write`) operan **solo contra los BIOs de memoria**:
  - ante `WANT_WRITE`, `flush_outgoing` **vacía** el `wbio` al socket (`async_send`);
  - ante `WANT_READ`, `feed_incoming` **alimenta** el `rbio` con lo leído del socket (`async_recv`);
    un retorno de `0` byte significa que el par cerró (EOF / `close_notify`).
- **`peer_principal()`**: extrae el **CN** del certificado del par para *authz* (mTLS).

## Acoplamiento opcional (coste cero, ADR-0008)

El build usa `find_package(OpenSSL)`. Si está presente, compila `ingress/tls.{hpp,cpp}` y define
`NEXUS_HAVE_OPENSSL` (toda la cabecera vive bajo `#ifdef NEXUS_HAVE_OPENSSL`); si no, el plano TLS se
**omite por completo** y el broker arranca **en claro**. El CI instala `libssl-dev` para ejercer el
*path* TLS (compilación, tests, sanitizers y clang-tidy).

> Coste asumido: el puente de BIOs añade una copia intermedia *ciphertext ↔ socket*; es aceptable
> porque el throughput TLS lo domina la cifra, no esa copia.
