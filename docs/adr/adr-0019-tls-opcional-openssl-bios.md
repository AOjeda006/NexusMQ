# ADR-0019: TLS opcional vía OpenSSL con puente de BIOs de memoria sobre el `Proactor`

- **Estado:** aceptado
- **Fecha:** 2026-06-19

> **Refina el desglose** (no cambia ninguna decisión previa): el desglose detallado (§4.8) ya situaba `TlsContext`/`TlsConnection` sobre OpenSSL en `nexus-ingress`. Este ADR fija **cómo** se integra OpenSSL (síncrono por diseño) con el modelo proactor asíncrono y **qué grado de dependencia** es OpenSSL en el build.

## Contexto

El plano de ingress (ADR-0006) termina TLS 1.3 y, intra-clúster, mTLS. La librería elegida es OpenSSL (madura, ubicua), pero su API de E/S es **bloqueante/síncrona** y NexusMQ es **thread-per-core asíncrono** sobre un `Proactor` (io_uring, ADR-0005/0002). Además, OpenSSL es una dependencia de sistema pesada que no siempre está presente (por ejemplo, entornos mínimos de CI o builds de desarrollo), y el broker debe poder **arrancar en claro** para pruebas locales y despliegues sin TLS.

## Decisión

Se toman tres decisiones coordinadas:

1. **Acoplamiento opcional.** El build usa `find_package(OpenSSL)`; si está presente, compila `ingress/tls.{hpp,cpp}` y define `NEXUS_HAVE_OPENSSL` (público). Si no, el plano TLS se **omite por completo** (cabecera y `.cpp` guardados con `#ifdef`, tests con `GTEST_SKIP`) y el broker funciona en claro. El CI instala `libssl-dev` para **sí** ejercer el path TLS (compilación, tests, sanitizers y clang-tidy).
2. **Puente de BIOs de memoria.** `TlsConnection` no deja que OpenSSL toque el descriptor; usa dos `BIO` de memoria (`rbio`/`wbio`). El bucle de `handshake`/`async_recv`/`async_send` llama a la primitiva OpenSSL y, ante `WANT_READ`/`WANT_WRITE`, **vacía** el `wbio` al socket y **alimenta** el `rbio` desde el socket mediante el `Proactor` asíncrono. Así la criptografía corre en línea pero el transporte es no bloqueante.
3. **Fábricas en el contexto.** `TlsContext::server`/`client` cargan material PEM y configuran la verificación (mTLS = `SSL_VERIFY_PEER|FAIL_IF_NO_PEER_CERT` + CA); `accept`/`connect` crean la `TlsConnection` fijando el rol del handshake. `peer_principal()` extrae el CN del certificado del par para authz.

## Consecuencias

- (+) El núcleo asíncrono **no se contamina** con E/S bloqueante; TLS encaja tras el mismo `Proactor` que el resto.
- (+) Build y despliegue **sin OpenSSL** siguen siendo válidos (degradación a claro), cumpliendo coste-cero (ADR-0008).
- (+) `TlsContext`/`TlsConnection` se prueban por loopback con certificados autofirmados generados en el test (una vía, mTLS con principal, y fallo de verificación).
- (−) El puente BIO añade una copia intermedia ciphertext↔socket (aceptable; el throughput TLS lo domina la cifra, no la copia).
- (−) La compilación condicional obliga a `#ifdef` en cabecera, `.cpp` y tests.

## Alternativas consideradas

- **`BIO` propio sobre el `Proactor` (custom BIO method):** evita una copia, pero exige implementar un `BIO_METHOD` con semántica de reintento correcta y es mucho más frágil; el puente de BIOs de memoria es el patrón canónico y suficiente.
- **OpenSSL como dependencia obligatoria (vcpkg/sistema):** simplifica el build, pero rompe coste-cero y el arranque en claro para desarrollo; descartado a favor del acoplamiento opcional.
- **Otra librería (BearSSL/mbedTLS/rustls-ffi):** menor huella, pero peor ergonomía/cobertura y menos familiar; OpenSSL ya estaba fijado en el desglose.
