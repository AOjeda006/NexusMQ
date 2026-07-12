# ADR-0031: Cifrado en reposo del log con AES-256-GCM y framing AEAD por bloque

- **Estado:** aceptado
- **Fecha:** 2026-07-12

> **Feature nueva, opcional y con degradación limpia** (patrón TLS de [ADR-0019](adr-0019-tls-opcional-openssl-bios.md) y compresión): sin clave configurada, el broker escribe el log **en claro** y se comporta byte a byte como hasta ahora. Cierra el punto «cifrado en reposo» del [capítulo 30](../tecnica/30-limitaciones-y-trabajo-futuro.md) (trabajo futuro).

## Contexto

El log de partición persiste en disco los `RecordBatch` en claro (segmentos `.log` append-only; cap. 9). En reposo, cualquiera con acceso al volumen —copia de seguridad, disco robado, snapshot de VM, operador sin necesidad de conocer el contenido— lee los mensajes. Falta una capa de **confidencialidad en reposo** que sea opcional (no todos los despliegues la necesitan ni tienen OpenSSL), transparente (el resto del motor no cambia), y compatible con las garantías del log: **lectura por offset con acceso aleatorio**, recuperación de cola *torn*, truncado (resolución de conflictos Raft) y re-append.

El reto criptográfico central de un log append-only cifrado es el **nonce**: AES-GCM es catastróficamente inseguro si se reutiliza un par (clave, nonce). Un contador de nonces es frágil bajo el ciclo truncar→re-append del log (un seguidor Raft descarta su cola divergente y vuelve a escribir en los mismos offsets), porque re-emitiría nonces ya usados con la misma clave.

## Decisión

Cifrado **AEAD AES-256-GCM** sobre el log, con **OpenSSL** como dependencia opcional (`find_package(OpenSSL)` → `NEXUS_HAVE_OPENSSL`; sin ella, `EncryptionKey`/`SegmentCipher` devuelven `Unsupported` y los tests hacen `GTEST_SKIP`). Cinco decisiones coordinadas:

1. **Granularidad por bloque de escritura, jamás por record.** Cada `append` de un `RecordBatch` se cifra como **un bloque** independiente. Nunca se cifra record a record (coste por operación inasumible y fuga de tamaños individuales).

2. **DEK por segmento derivada de la KEK.** La **KEK** (clave maestra de 256 bits) llega por entorno/config (`NEXUS_ENCRYPTION_KEY` o `--encryption-key HEX`) y **jamás** se persiste ni entra en el repo. No cifra datos: solo deriva, vía **HKDF-SHA256(KEK, salt aleatoria de 16 bytes por segmento)**, una **DEK** distinta por segmento. Esto acota el uso de cada clave a un único segmento (aísla el radio de daño) y mantiene el número de cifrados por clave muy por debajo del límite de colisión de nonces aleatorios.

3. **Nonce aleatorio de 96 bits por bloque.** Cada bloque toma un nonce fresco del CSPRNG (`RAND_bytes`). Es **robusto ante truncado/re-append**: no hay contador que rebobinar, cada escritura es un nonce nuevo con probabilidad de colisión despreciable dado el techo de bloques por DEK. **Invariante nº 1: nunca se reutiliza un nonce con la misma clave.**

4. **Framing autodescriptivo con metadatos de traversal autenticados en claro.** El `.log` empieza por una **cabecera de segmento** de 32 bytes (`magic "NXSEG1" | ids de KDF/cipher | flags | salt`) → autodetección cifrado/claro al abrir y logs mixtos. Cada bloque es `version | flags | base_offset | record_count | ct_len | nonce(12) | tag(16) | ciphertext`. `base_offset`/`record_count`/`ct_len` viajan **en claro pero autenticados** (AAD = cabecera del bloque salvo el tag): permiten **recorrer el log y localizar por offset sin descifrar** (los offsets y tamaños no son secretos; las claves/valores de los records sí, van en el ciphertext). Manipular cualquier metadato en claro se detecta al abrir el bloque.

5. **Integridad on-disk = tag GCM; el CRC32C se conserva dentro del plaintext.** El tag GCM autentica cada bloque (supera al CRC para ciphertext). El CRC32C del batch permanece **dentro** del plaintext cifrado como defensa en profundidad y para que el camino en claro sea idéntico al de hoy.

La rotación de claves queda como **trabajo futuro documentado** (la salt por segmento y los ids en la cabecera dejan el formato preparado).

## Consecuencias

- (+) Confidencialidad en reposo real (AES-NI vía OpenSSL) sin tocar el resto del motor: el cifrado vive confinado en `segment_crypto.{hpp,cpp}` y en el framing de `Segment`.
- (+) **Degradación limpia:** sin clave, o sin OpenSSL, el broker escribe en claro y los 789 tests previos siguen verdes (camino en claro byte-idéntico). Cumple coste-cero (ADR-0008).
- (+) Lectura por offset, recuperación de cola *torn*, truncado y re-append siguen funcionando **cifrados**, gracias al framing por bloque con metadatos autenticados en claro.
- (+) La DEK por segmento + nonce aleatorio hace el esquema **seguro ante el ciclo truncar→re-append** del log, sin contadores de nonce frágiles.
- (−) El `.log` cifrado no es byte-idéntico al plano (cabecera de segmento + 46 bytes de cabecera por bloque); el *scan* arranca en `data_start_`. Sobrecoste de espacio ≈ 78 bytes/segmento + 46 bytes/batch.
- (−) La KEK es responsabilidad del operador (gestión de secretos externa). Perderla = perder los datos; filtrarla = perder la confidencialidad. Sin rotación en v1.0.
- (−) Compilación condicional (`#ifdef NEXUS_HAVE_OPENSSL`) en la primitiva cripto y `GTEST_SKIP` en sus tests.

## Alternativas consideradas

- **Cifrado a nivel de sistema de ficheros (LUKS/dm-crypt) o de bloque del SO:** cero código, pero fuera del control del broker, no viaja con el segmento al *tiered storage* (Hito B ofrece el ciphertext tal cual), y no protege backups/snapshots lógicos. Complementario, no sustituto.
- **Nonce por contador (determinista) con DEK global:** el patrón AES-GCM más común, pero **frágil bajo truncar→re-append** del log Raft (reusaría nonces) y concentra todo el tráfico bajo una sola clave. Descartado por la invariante nº 1.
- **AES-GCM-SIV (resistente a reutilización de nonce):** elimina el riesgo de nonce, pero exige dos pasadas sobre el plaintext y no está en toda versión de OpenSSL; el par DEK-por-segmento + nonce aleatorio da el mismo margen de seguridad con AES-GCM estándar y una sola pasada.
- **Cifrar por record:** máxima granularidad, pero coste por operación y sobrecoste de tag por record inasumibles, y filtra el tamaño de cada mensaje. Descartado a favor del bloque = batch.
- **ChaCha20-Poly1305:** excelente sin AES-NI, pero el target tiene AES-NI y OpenSSL ya estaba fijado para TLS (ADR-0019); AES-256-GCM reutiliza la misma dependencia.
