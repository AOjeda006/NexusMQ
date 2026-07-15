# Diagrama 18: Flujo REST admin con JWT

El plano de administración de NexusMQ es una API REST bajo `/api/v1` servida en el **puerto de
operación** (`nexusd --admin-port <N>`, sin valor por defecto), separada del plano de datos binario.
Las rutas `/api/v1/*` se autentican con **Bearer JWT** (HS256, firmado con el secreto del nodo)
**solo si** el nodo arrancó con `--jwt-secret`; en ese caso devuelven `401` sin un token válido. Los
errores siguen **RFC 7807** (`application/problem+json`). Los *endpoints* de salud/métricas
(`/healthz`, `/readyz`, `/metrics`) van **sin autenticar**. Contrato:
[`../openapi.yaml`](../openapi.yaml).

```mermaid
sequenceDiagram
    autonumber
    participant Cli as Cliente admin
    participant API as REST admin (/api/v1, --admin-port)

    Note over Cli,API: El JWT (HS256) se emite fuera de banda con el secreto del nodo (--jwt-secret)

    Cli->>API: GET /api/v1/topics<br/>Authorization: Bearer <JWT>
    activate API
    alt --jwt-secret activo y token válido (firma, exp)
        API-->>Cli: 200 OK<br/>TopicPage (application/json)
    else token ausente o inválido (firma/exp)
        API-->>Cli: 401 Unauthorized<br/>ProblemDetail (application/problem+json)
    else parámetros inválidos (page/size fuera de rango)
        API-->>Cli: 400 Bad Request<br/>ProblemDetail (application/problem+json)
    end
    deactivate API

    Cli->>API: POST /api/v1/topics<br/>Authorization: Bearer <JWT><br/>CreateTopicRequest (application/json)
    activate API
    alt token válido y cuerpo válido
        API-->>Cli: 201 Created<br/>Location: /api/v1/topics/{name}<br/>TopicSummary
    else token inválido
        API-->>Cli: 401 Unauthorized<br/>ProblemDetail
    else cuerpo inválido (name vacío, etc.)
        API-->>Cli: 400 Bad Request<br/>ProblemDetail
    else método no permitido
        API-->>Cli: 405 Method Not Allowed<br/>ProblemDetail
    end
    deactivate API

    Cli->>API: DELETE /api/v1/topics/{name}<br/>Authorization: Bearer <JWT>
    activate API
    alt token válido y topic existe
        API-->>Cli: 204 No Content
    else topic inexistente
        API-->>Cli: 404 Not Found<br/>ProblemDetail
    end
    deactivate API

    Note over Cli,API: /healthz, /readyz y /metrics NO requieren token (security: [])
    Cli->>API: GET /healthz
    API-->>Cli: 200 HealthStatus / 503 (draining)
```

## Notas del contrato

- **Validación del token en el borde:** la API comprueba la **firma** (HS256 con el secreto del nodo)
  y la **expiración** antes de servir cualquier ruta `/api/v1/*`. Si `--jwt-secret` no está activo, no
  se exige token.
- **Política de errores central (RFC 7807):** toda respuesta de error usa `ProblemDetail`
  (`application/problem+json`) con al menos `title` y `status`. El estándar contempla además `403`
  (no autorizado) para autorización por recurso, aunque el contrato actual detalla `400`/`401`/`404`/`405`.
- **Códigos por operación** (según `openapi.yaml`): `GET /api/v1/topics` → `200`/`400`/`401`;
  `POST /api/v1/topics` → `201`/`400`/`401`/`405`; `GET /api/v1/topics/{name}` → `200`/`401`/`404`;
  `PATCH /api/v1/topics/{name}` → `200`/`400`/`401`/`404`; `DELETE /api/v1/topics/{name}` →
  `204`/`401`/`404`/`405`; `GET /api/v1/groups` → `200`/`400`/`401`/`405`; `GET /api/v1/groups/{id}`
  → `200`/`401`/`404`; `GET /api/v1/cluster` → `200`/`401`.
- **Paginación:** las colecciones aceptan `page` (≥1, def. 1) y `size` (1–100, def. 20).
