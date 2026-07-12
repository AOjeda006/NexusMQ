# 26. Configuración y operación

> Operar un nodo en marcha: cómo se configura, qué puertos usa, qué se observa y qué hacer
> cuando algo va mal. La configuración es 12-factor (entorno, no imagen).

## 26.1 Puertos

| Plano | Flag | Puerto (convención) |
| ----- | ---- | ------------------- |
| Datos / cliente nativo | `--port` | 9092 |
| Operación (REST `/api/v1`, `/healthz`, `/readyz`, `/metrics`) | `--admin-port` | 9644 |
| Subset Kafka (interop `kcat`) | `--kafka-port` | opcional |
| Inter-nodo Raft (consenso) | — | transporte interno |

## 26.2 Configuración

Por **flags** y variables de entorno (mismo binario en todos los entornos). Además de los flags
de arranque (`--host`, `--node-id`, `--data-dir`, `--topic nombre:particiones`, `--jwt-secret`;
ver [capítulo 19](./19-arranque-y-composition-root.md)), el comportamiento del log y la
durabilidad se gobierna por **política** (mecanismo vs política): retención
(`retention.ms`/`retention.bytes`), tamaño de segmento (`segment.bytes`/`segment.ms`),
compactación, compresión (none/LZ4/Zstd) y política de `fsync` (cada N mensajes / N ms / por
*commit*). Borrar un *topic* (`DELETE /api/v1/topics/{name}`) elimina sus datos en disco (el
`.log`/`.index` de todas sus particiones) y **recupera el espacio**; no queda estado que
"resucite" al re-declarar el nombre. Los **secretos** (p. ej. el de JWT) van por entorno, nunca
en la imagen ni en el repositorio (ver [capítulo 27](./27-seguridad.md)).

**Cifrado en reposo (opcional).** La KEK de 256 bits se pasa como **64 dígitos hex** por
`NEXUS_ENCRYPTION_KEY` (preferido: no aparece en `ps`) o `--encryption-key HEX` (el flag tiene
prioridad); sin ella, el log se escribe en claro. Si la clave es inválida, el arranque **aborta**
(no degrada en silencio). La KEK **nunca** se persiste ni se commitea; su gestión (generación,
custodia, rotación) es responsabilidad del operador vía gestor de secretos. Ver
[capítulo 9](./09-almacenamiento.md) y [ADR-0031](../adr/adr-0031-cifrado-en-reposo-aes-gcm.md).

**Almacenamiento por niveles (opcional).** Con `--tier-dir RUTA` (o `NEXUS_TIER_DIR`) el broker
**descarga los segmentos sellados fríos** a ese directorio objeto y reclama el disco local, sirviendo
las lecturas frías por rehidratación transparente (ver [capítulo 9](./09-almacenamiento.md),
[ADR-0032](../adr/adr-0032-tiered-storage-puerto-y-tier-local.md)). Sin el flag, el broker no
descarga nada y se comporta como hasta ahora. El directorio del tier debe ser un almacén **durable y
distinto del disco de datos** (un montaje de objetos, un volumen compartido; en producción, el punto
de evolución hacia S3): es la copia autoritativa de los segmentos fríos, así que su pérdida es pérdida
de datos. Si el log va cifrado (ADR-0031), el tier guarda el **ciphertext tal cual** —basta la misma
KEK para leerlo, la clave nunca viaja al tier—.

## 26.3 Observabilidad en operación

- **Métricas:** `GET /metrics` (Prometheus) — throughput, latencias por percentil, *lag* de
  consumidores, estado de Raft, créditos de *backpressure*.
- **Dashboards:** Grafana *self-hosted* con *datasource* Prometheus aprovisionado
  (`deploy/grafana/`), siguiendo las cuatro señales de oro (latencia, tráfico, errores,
  saturación). Ver [capítulo 12](./12-observabilidad.md) y
  [diagrama 23](../diagramas/23-pipeline-observabilidad.md).
- **Salud:** `/healthz` (liveness) y `/readyz` (readiness: disco/Raft/*lag*), usados por los
  *probes* y el `HEALTHCHECK`.

## 26.4 Runbook (esbozo)

- **Arranque/parada:** `docker compose up --build` (local). El apagado atiende
  `SIGTERM`/`SIGINT` y **drena** el trabajo en curso antes de salir.
- **Backup:** un *snapshot* de `--data-dir` por nodo. **Restore:** arrancar desde ese
  `data.dir`. Si el log está **cifrado** (ADR-0031), el backup contiene solo ciphertext: hay que
  restaurar el nodo con **la misma KEK**; sin ella los segmentos son ilegibles (perder la KEK =
  perder los datos). Con **tiered storage** (ADR-0032), el backup debe cubrir **también** el
  `--tier-dir`: los segmentos fríos ya no están en `--data-dir` y el tier es su copia autoritativa.
- **Recuperación tras *crash*:** automática al arrancar (validación de CRC + truncado de la
  cola *torn*); el nodo queda listo en pocos segundos (ver
  [capítulo 9](./09-almacenamiento.md)).
- **Pérdida de quórum (p. ej. 2 de 3 nodos caídos):** por la postura **CP**, las particiones
  afectadas quedan **no disponibles para escritura** y se recuperan al restaurar nodos. Forzar
  progreso sin quórum solo mediante un procedimiento **manual y documentado** de
  *unsafe-recovery* (asume riesgo de pérdida).
- **Rotación de certificados TLS:** recarga en caliente si está disponible, o *rolling restart*.
- **Escalado:** *thread-per-core* escala con el número de núcleos del nodo; el cluster, añadiendo
  nodos/particiones (la asignación partición→núcleo es `partition % N`, ver
  [capítulo 7](./07-concurrencia.md)).
