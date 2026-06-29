# 27. Seguridad

> La postura de seguridad de NexusMQ: cifrado en tránsito, autenticación del plano de control,
> validación en el borde y mínimo privilegio. Se apoya en la normativa de seguridad de la
> `BibliotecaDocumentacion` (OWASP, defensa en profundidad).

## 27.1 Cifrado en tránsito (TLS)

- **TLS 1.3 en el borde:** terminación en la capa de *ingress* mediante OpenSSL con un puente
  de **BIOs de memoria** sobre el proactor ([ADR-0019](../adr/adr-0019-tls-opcional-openssl-bios.md),
  ver [capítulo 11](./11-ingress.md) y [diagrama 20](../diagramas/20-puente-tls-bios.md)). La
  dependencia es **opcional**: si OpenSSL no está, el nodo degrada a texto en claro (útil en
  dev), pero en redes no confiables **TLS es obligatorio** y los certificados se **validan**.
- **mTLS intra-cluster (contemplado):** ambos extremos presentan y validan certificado, dando
  identidad criptográfica a cada nodo del plano inter-nodo. Conviene **rotar** los certificados
  (vida corta, emisión automatizada).

## 27.2 Autenticación y autorización

- **AuthN ≠ AuthZ.** El plano de control (REST `/api/v1`) se autentica con **Bearer JWT**
  (HS256), exigido solo si el nodo arranca con `--jwt-secret`; sin token válido → `401` (ver
  [capítulo 15](./15-api-rest-administracion.md)). Los tokens se validan (firma, expiración).
- **Autorización por recurso** en el servidor, en cada petición (no por ocultar acciones en el
  cliente). El plano de salud/métricas queda sin auth a propósito, para las sondas y el
  *scraper*.

## 27.3 Validación en el borde

**Nunca se confía en la entrada del cliente.** Todo dato externo —frames del protocolo,
RecordBatch, peticiones Kafka, JSON de la REST— se **valida en el borde** (*fail-fast*) contra
límites explícitos antes de entrar al núcleo: tamaño máximo de mensaje/*batch*
(`MESSAGE_TOO_LARGE`), versiones de API soportadas, y **límites de descompresión** para evitar
*decompression bombs* (un *batch* que se expande desmesuradamente). El núcleo asume datos ya
válidos (ver [capítulo 16](./16-modelo-errores-wire-codes.md)).

## 27.4 Mínimo privilegio y secretos

- **Usuario no-root** en la imagen y en Kubernetes (UID/GID `65532`), para limitar el impacto
  de una intrusión (ver [capítulo 25](./25-despliegue.md)).
- **Secretos por variables de entorno** (o gestor de secretos), **nunca** horneados en la imagen
  ni commiteados al repositorio; si un secreto se filtra, se **rota** (no basta con borrarlo).
- **Defensa en profundidad:** varias capas (TLS, JWT, validación, aislamiento de proceso), cada
  componente con los permisos justos, asumiendo que una capa fallará.
- **Mensajes de error sin detalles internos** hacia el exterior (códigos de wire / RFC 7807),
  para no filtrar información sensible.

## 27.5 Fuera de alcance

- **Cifrado en reposo** queda fuera de alcance (solo en tránsito).
- **Multi-tenancy y ACLs avanzadas** no se abordan en las fases actuales (ver
  [capítulo 30](./30-limitaciones-y-trabajo-futuro.md)).
