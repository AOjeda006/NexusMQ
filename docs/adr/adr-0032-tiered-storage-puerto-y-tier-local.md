# ADR-0032: Almacenamiento por niveles (tiered storage) — puerto `StorageTier` y tier local

- **Estado:** aceptado
- **Fecha:** 2026-07-12

> **Feature nueva, opcional y con degradación limpia** (patrón TLS de [ADR-0019](adr-0019-tls-opcional-openssl-bios.md) y cifrado de [ADR-0031](adr-0031-cifrado-en-reposo-aes-gcm.md)): sin `--tier-dir`, el broker se comporta byte a byte como hasta ahora. Cierra el punto «tiered storage» del [capítulo 30](../tecnica/30-limitaciones-y-trabajo-futuro.md) (trabajo futuro).

## Contexto

El log de partición retiene los segmentos sellados en disco local hasta que la retención los borra (cap. 9). Para retención larga a bajo coste —el patrón de Pulsar/Kafka *tiered storage*— conviene descargar los segmentos **fríos** (sellados, ya no en el *hot set*) a un almacén de objetos barato (S3 y similares), reclamando el disco local, y **rehidratarlos de forma transparente** cuando un consumidor rezagado los lee. El reto: hacerlo sin acoplar el motor de log a una nube concreta, sin romper las garantías del log (lectura por offset, recuperación, truncado de Raft) y de forma que interopere con el cifrado en reposo (ADR-0031).

## Decisión

Un **puerto** `StorageTier` (inversión de dependencias) más un adaptador local por defecto, integrados en el `PartitionLog` con **opt-in y degradación limpia**. Cinco decisiones:

1. **Puerto orientado a fichero.** `StorageTier` expone `put_file`/`fetch_file`/`contains`/`object_size`/`remove`/`list_segment_bases` sobre una **clave de objeto** `TierObjectKey` (`<topic>/<partition>/<base:020>.<ext>`, determinista y jerárquica). Es orientado a **fichero** porque un segmento **es** un fichero: descargar y rehidratar son copias, sin cargar segmentos enteros en memoria. El `PartitionLog` depende de la interfaz, no de una nube. El adaptador por defecto (`LocalStorageTier`) copia a un **directorio objeto** local (coste cero, ADR-0008); un adaptador S3 (futuro) implementaría el mismo contrato tras `find_package`.

2. **Descargar solo segmentos sellados; reclamar solo tras confirmar.** `offload_sealed_to_tier` sube al tier los segmentos **sellados** (nunca el activo), del más antiguo al más nuevo, y **solo tras confirmar la subida** (`put_file` es atómico e idempotente) borra los ficheros locales y marca el segmento como frío. Un fallo del tier deja el segmento local y se reintenta (idempotente). Nunca se pierde un dato por una reclamación prematura.

3. **El tier es la autoridad; sin manifiesto local.** Al reabrir, el `PartitionLog` reconstruye su **prefijo frío** listando el tier (`list_segment_bases`), sin un fichero manifiesto que pudiera desincronizarse. Las bases que también existen localmente (offload confirmado pero sin reclamar por un *crash*) se descartan del prefijo: el segmento local es autoritativo. La **contigüidad** del log (sin huecos) permite reconstruir el rango de cada segmento frío solo con las bases ordenadas.

4. **Lectura transparente por rehidratación.** `read` sirve el *suffix* caliente localmente y, para un offset del prefijo frío, **rehidrata** el segmento (baja `.log` + `.index` a un temporal, lo abre —descifrándolo si procede, ADR-0031— y sirve el fragmento), limpiando el temporal (RAII). El rango del log (`log_start`/`log_end`) **no cambia** al descargar. Interopera con el cifrado: el tier guarda el **ciphertext tal cual** (sube y baja bytes opacos; la clave nunca sale del broker).

5. **Política de descarga en la rotación; operaciones destructivas *tier-conscientes*.** Con un tier configurado, cada **rotación** de segmento dispara la descarga de los sellados (best-effort: un fallo no aborta la rotación). Las operaciones de Raft se hacen conscientes del prefijo frío: `truncate_prefix_to`/`reset_to` (compactación/snapshot) borran del tier los segmentos que descartan; `truncate_to` rechaza truncar dentro del prefijo frío (historia comprometida, que Raft nunca reescribe); `enforce_retention` no toca el *hot set* local mientras haya prefijo frío (los datos más antiguos ya están en el tier).

La KEK, `--tier-dir`/`NEXUS_TIER_DIR`, `topic`/`partition` forman la identidad; el tier lo posee el *composition root* (`Server`), compartido por el nodo, y se propaga (`TopicCatalog` → `TopicManager` → `LogConfig`) como puntero **no-propietario** a cada `PartitionLog`.

## Consecuencias

- (+) Retención larga a bajo coste sin tocar el resto del motor: el tiering vive en `storage_tier`/`local_storage_tier` y en el framing del `PartitionLog`; índice, retención y Raft no cambian.
- (+) **Degradación limpia:** sin `--tier-dir`, el prefijo frío está siempre vacío y todos los caminos nuevos son *no-ops*; el comportamiento es byte-idéntico al de hoy (858 tests verdes).
- (+) La lectura por offset, la recuperación, el truncado y el snapshot de Raft siguen funcionando con datos fríos, gracias a la rehidratación transparente y a las operaciones *tier-conscientes*.
- (+) Interopera con el cifrado en reposo (sube/baja ciphertext opaco) y prepara un adaptador S3 como evolución del mismo puerto.
- (−) La rehidratación baja el **segmento entero** por cada lectura fría (sin caché local persistente): correcto pero no óptimo. Una caché de rehidratación es trabajo futuro.
- (−) La descarga en la rotación es **síncrona** (copia de fichero en el hilo del reactor con el tier local); el offload asíncrono queda como trabajo futuro. Para el tier local (copia local) el coste es asumible.
- (−) La recuperación tras **pérdida del disco local** (árbol local vacío con objetos en el tier) queda fuera de alcance: se prioriza el caso normal de reinicio, que conserva al menos el segmento activo local.

## Alternativas consideradas

- **Puerto orientado a bytes (`put`/`get` de blobs):** más portable a object stores, pero obliga a cargar segmentos enteros en memoria. Un segmento **es** un fichero; la interfaz de fichero evita la copia en RAM y el adaptador local es una copia trivial. Un adaptador S3 igualmente sube/baja vía fichero temporal (patrón estándar).
- **Manifiesto local de segmentos descargados:** rápido de consultar, pero es estado duplicado que puede desincronizarse del tier ante un *crash* entre la subida y la escritura del manifiesto. Listar el tier (autoridad única) es más simple y robusto.
- **Cifrado a nivel de tier (re-cifrar al subir):** innecesario y peligroso (la clave tendría que viajar al tier). Subir el ciphertext del ADR-0031 tal cual da confidencialidad en el tier sin exponer la KEK.
- **Descarga asíncrona en un hilo de mantenimiento:** mejor para no bloquear el reactor, pero añade un subsistema de tareas de fondo que el motor (Fase 1, E/S bloqueante en storage) aún no tiene; la descarga síncrona en la rotación es coherente con el modelo actual y se deja la versión asíncrona como evolución.
- **Borrar el segmento local sin descargar (solo retención):** es lo que ya hace `enforce_retention`; el tiering **añade** la opción de conservar los datos fríos accesibles a bajo coste en vez de perderlos.
