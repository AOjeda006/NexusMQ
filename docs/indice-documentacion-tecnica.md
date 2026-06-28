# Documentación técnica de NexusMQ — Índice y plan de contenidos

> **Propósito de este documento.** Define **qué contendrá** la documentación técnica
> **final** de NexusMQ: la que **sustituirá** a `DocumentacionProvisional/` (anteproyecto,
> desgloses, hoja de ruta), la **ampliará en detalle** y se **apoyará en `docs/`** (contratos
> as-built ya escritos) más un conjunto de **diagramas**. Es un **plan para revisar y acordar**
> antes de redactar nada: aquí solo está la **estructura**, el **mapeo a las fuentes** existentes
> y el **catálogo de diagramas**. La redacción de cada capítulo y la generación de diagramas se
> harán **después**; el paso final será compilar todo a **PDF**, borrar `DocumentacionProvisional/`
> y publicar el repositorio.

---

## 1. Forma, idioma y producción

- **Idioma:** español (doc-comments e identificadores en inglés, como en todo el proyecto).
- **Formato fuente:** conjunto de archivos **Markdown numerados** (mantenibles y versionables),
  no un único `.md` gigante. Cada capítulo en su fichero, bajo `docs/tecnica/`.
- **Diagramas:** **Mermaid** (texto versionable; flujo, secuencia, estado, grafos y C4) +
  **tablas/ASCII** para *layouts* de bytes (formatos de wire). Fuente en `docs/diagramas/`,
  render a `docs/diagramas/img/` (SVG/PNG) para el PDF. *(Recomendación; abierta a cambio.)*
- **Producción del PDF:** **pandoc** + motor LaTeX, con render previo de Mermaid (mermaid-cli /
  filtro) y portada/índice automático. *(Recomendación; se concretará al llegar a esa fase.)*
- **Contratos as-built:** `docs/protocol.md`, `docs/kafka.md`, `docs/openapi.yaml` y
  `docs/benchmarks.md` **ya existen, se conservan** y son la **única fuente de verdad** de los
  detalles precisos (byte-layout, ApiKeys/versiones, endpoints, cifras). Conviven dos **tipos**
  de documentación distintos (**Diátaxis**): los contratos son **referencia** (se consultan), y
  los capítulos narrativos son **explicación** (se leen en orden). Por eso los capítulos
  correspondientes **explican y enlazan** (rationale, flujos, diagramas, ejemplos) — **no
  duplican** el contrato. Razones para mantenerlos separados: `openapi.yaml` es un **artefacto de
  tooling** (Swagger UI, *codegen*, *contract testing*), DRY/no-*drift* (una sola fuente
  autoritativa), audiencia y ciclo de vida distintos, y descubribilidad junto al código en el repo.
- **El PDF los incluye por transclusión.** Para que el PDF sea autocontenido sin duplicar, pandoc
  **transcluye** los contratos de `docs/` como **apéndices** al compilar (una sola fuente, PDF
  completo); no se copian a la prosa.

### Disposición de archivos propuesta

```
docs/
├── indice-documentacion-tecnica.md      ← este documento (índice maestro)
├── protocol.md          (existe)  ← contrato protocolo nativo
├── kafka.md             (existe)  ← contrato subset Kafka
├── openapi.yaml         (existe)  ← contrato REST admin
├── benchmarks.md        (existe)  ← metodología + cifras
├── tecnica/                       ← narrativa final (capítulos)
│   ├── 00-prefacio.md
│   ├── 01-resumen-ejecutivo.md
│   ├── …                          ← un fichero por capítulo (ver §2)
│   └── 99-bibliografia.md
├── adr/                           ← ADR-0001..0029 extraídos (uno por archivo)
│   ├── adr-0001-plataforma-y-target.md
│   └── …
└── diagramas/                     ← fuentes Mermaid + img/ renderizado (ver §3)
```

---

## 2. Estructura de la documentación (partes y capítulos)

> Cada capítulo lista su **alcance** en una línea y la **fuente** de la que se nutre
> (sección del anteproyecto, desglose, contrato de `docs/`, ADR o código). "Amplía" = el
> material existe y se profundiza; "Nuevo" = se redacta de cero a partir del código.

### Parte I — Introducción y visión

| # | Capítulo | Alcance | Fuente |
|---|----------|---------|--------|
| 1 | Resumen ejecutivo | Qué es NexusMQ, qué demuestra, resultados en una página | Anteproyecto §1; README |
| 2 | Contexto y motivación | Problema, objetivos de aprendizaje/portfolio, alcance y **no**-alcance | Anteproyecto §1–§4 (RF/RNF) |
| 3 | Estado del arte | Kafka clásico vs frontera actual (Redpanda/Seastar); posicionamiento | Anteproyecto §1, §3.5 |
| 4 | Glosario | Topic, partición, offset, segmento, WAL, Raft, HWM, shared-nothing… | Anteproyecto (glosario final) — Amplía |

### Parte II — Arquitectura

| # | Capítulo | Alcance | Fuente |
|---|----------|---------|--------|
| 5 | Vista de conjunto | 15 librerías `nexus-*` + exes/tools; grafo de dependencias; C4 | Desglose general; ADR-0013/0017/0018 |
| 6 | Principios de diseño | Shared-nothing TPC, Raft/partición, proactor, CP/PACELC, errores por capa | ADR-0005/0003/0002/0007/0009 |
| 7 | Concurrencia | Reactor por núcleo, *pinning*, SPSC cross-core, sharding partición→núcleo / grupo→hash | ADR-0005/0026; `nexus-reactor` |
| 8 | Modelo de I/O | Proactor; io_uring directo (uapi); IOCP; corutinas; durabilidad/`fsync`; checksums | ADR-0002/0012/0021-0023; `nexus-io` |
| 9 | Almacenamiento | Log particionado, segmentos `.log`/`.index`, índice disperso, RecordBatch v2, retención | Anteproyecto §7.1; `nexus-storage` — Amplía |
| 10 | Replicación y consenso | Raft/partición; log=WAL; índice vs offset; FSM sin E/S; snapshot; transporte inter-nodo | ADR-0003/0014/0015/0016/0024/0025; `nexus-consensus`/`cluster` |
| 11 | Ingress | Dos modos (nativo/proxy); *upstream pool*; REST admin; TLS/mTLS | ADR-0006/0027/0018/0019; `nexus-ingress` |
| 12 | Observabilidad | Telemetría, métricas Prometheus, logs estructurados, health/readiness | ADR-0017; `nexus-telemetry` |

### Parte III — Contratos (apoyados en `docs/`)

| # | Capítulo | Alcance | Fuente |
|---|----------|---------|--------|
| 13 | Protocolo binario nativo | Framing 14 B, ApiKeys, versionado, *correlation*, flujo produce/fetch | **docs/protocol.md** — Explica y enlaza |
| 14 | Subconjunto Kafka | Framing `Size:INT32`, APIs/versiones, clásico vs flexible, RecordBatch opaco | **docs/kafka.md**; ADR-0029 — Explica y enlaza |
| 15 | API REST de administración | Endpoints, auth JWT, paginación, RFC 7807 | **docs/openapi.yaml** — Explica y enlaza |
| 16 | Modelo de errores y wire codes | `expected`/excepciones/códigos por capa; traducción en el borde | ADR-0009; `nexus-protocol` |

### Parte IV — Implementación (sustituye al desglose detallado)

| # | Capítulo | Alcance | Fuente |
|---|----------|---------|--------|
| 17 | Mapa de módulos | Grafo de dependencias real (CMake), mapa fase→targets | Desglose general — Amplía |
| 18 | Catálogo por subsistema | Cada `nexus-*`: responsabilidad, tipos clave, afinidad, invariantes | Desglose **detallado** — Amplía |
| 19 | Arranque y composition root | `nexusd`, configuración 12-factor, flags y puertos | `src/server`; ADR-0008/0025/0029 |
| 20 | Herramientas y bindings | `nexus-cli`, `nexus-bench`, `nexus-loadgen`, `wincheck`; binding Python (FFI) | `tools/`; `bindings/`; ADR-0020 |

### Parte V — Calidad

| # | Capítulo | Alcance | Fuente |
|---|----------|---------|--------|
| 21 | Estrategia de pruebas | TDD; unitarias; property-based; fuzzing; **simulación determinista**; chaos; sanitizers | `principios/testing.md`; ADR-0015; `tests/` |
| 22 | Puerta de calidad y CI/CD | Dos compiladores, formato, clang-tidy, sanitizers; GitHub Actions | CLAUDE.md; `.github/`; ADR-0011 |
| 23 | Rendimiento y benchmarks | Metodología (percentiles, *coordinated omission*), resultados | **docs/benchmarks.md** — Explica y enlaza |
| 24 | Portabilidad | Linux/Windows; niveles compile/link/**runtime**-verified | ADR-0021-0023/0028 |

### Parte VI — Operación y despliegue

| # | Capítulo | Alcance | Fuente |
|---|----------|---------|--------|
| 25 | Despliegue | Docker, docker-compose (cluster 3 nodos), Kubernetes (StatefulSet/Service) | `deploy/`; ADR-0008 |
| 26 | Configuración y operación | Puertos, flags, métricas, dashboards Grafana | Anteproyecto §7.x; `deploy/grafana` |
| 27 | Seguridad | TLS/mTLS, JWT, validación en el borde, mínimo privilegio | ADR-0019; `herramientas/seguridad.md` |

### Parte VII — Decisiones y evolución

| # | Capítulo | Alcance | Fuente |
|---|----------|---------|--------|
| 28 | Registro de decisiones (ADR) | ADR-0001..0029, uno por archivo en `docs/adr/` | Anteproyecto §9 — **Extrae** |
| 29 | Historia de desarrollo | Retrospectiva por fases (1→4), hitos y aprendizajes | Hoja de ruta — **Resume** |
| 30 | Limitaciones y trabajo futuro | Cobertura parcial Kafka, deudas acotadas, evolución posible | ADR-0027/0029 (consecuencias) |

### Apéndices

- **A. Bibliografía** — las fuentes canónicas de la BibliotecaDocumentacion (DDIA, Raft paper,
  C++ Concurrency in Action, Systems Performance, CS:APP…).
- **B. Índice de diagramas** — referencia cruzada (ver §3).
- **C. Cómo construir y ejecutar** — *quick start* (puede vivir también en el README).

---

## 3. Catálogo de diagramas

> Tipo recomendado entre paréntesis. Todos en `docs/diagramas/` (fuente) → `img/` (render).

**Arquitectura y módulos**
1. **Contexto C4** — NexusMQ y sus actores (clientes nativos, `kcat`, admin REST, Prometheus). (C4/Mermaid)
2. **Contenedores C4** — `nexusd`, planos (cliente/Raft/admin), almacenamiento. (C4/Mermaid)
3. **Grafo de dependencias** de las 15 librerías `nexus-*`. (graph) — Fuente: desglose general.
4. **Mapa fase→targets** (qué se entregó en cada fase). (graph/tabla)

**Runtime y concurrencia**
5. **Topología thread-per-core** — reactores *pinned*, estado por núcleo, colas SPSC cross-core. (flow)
6. **Sharding** — partición→núcleo (`p % N`) y grupo→`hash(id) % N`. (flow)
7. **Secuencia proactor** — `submit op → completion`; suspensión/reanudación de corutina. (sequence)

**Almacenamiento**
8. **Layout del log** — segmentos `.log`/`.index`, índice disperso, RecordBatch v2. (ASCII/tabla)
9. **Retención y compactación** — borrado por segmentos sellados. (flow)

**Consenso (Raft por partición)**
10. **Estados de un nodo Raft** — follower/candidate/leader y transiciones. (state)
11. **Replicación + commit** — `produce` → quorum → `commitIndex` → high-watermark. (sequence)
12. **Failover / elección de líder** — *timeout* → votos → nuevo líder. (sequence)
13. **Dos espacios de coordenadas** — índice de entrada Raft vs offset por record. (diagram)
14. **Snapshot / InstallSnapshot** — base `(index,term,offset)` + puesta al día del seguidor. (sequence)
15. **Planos de red** — plano de cliente vs plano inter-nodo (Raft), puertos separados. (flow)

**Protocolos**
16. **Frame nativo** — *layout* de la cabecera de 14 bytes. (ASCII/tabla) — Fuente: protocol.md.
17. **Petición/respuesta Kafka** — framing `Size:INT32`, clásico vs flexible. (ASCII/sequence) — kafka.md.
18. **Flujo REST admin + JWT** — auth y rutas `/api/v1`. (sequence) — openapi.yaml.

**Ingress / seguridad**
19. **Modo nativo vs proxy** — *smart-client* al líder vs proxy con *upstream pool*. (flow)
20. **Puente TLS de BIOs** — OpenSSL síncrono sobre el proactor asíncrono. (sequence)

**Operación**
21. **Despliegue docker-compose** — cluster de 3 nodos + Prometheus + Grafana. (flow)
22. **Despliegue Kubernetes** — StatefulSet + Service. (flow)
23. **Pipeline de observabilidad** — métricas → Prometheus → Grafana. (flow)

---

## 4. Migración desde `DocumentacionProvisional/` (qué pasa con cada fuente)

| Fuente provisional | Destino en la documentación final |
|--------------------|-----------------------------------|
| `anteproyecto.md` §1–§8 (visión, arquitectura) | Se reparte y **amplía** en Partes I–II, V–VI |
| `anteproyecto.md` §9 (ADR-0001..0029) | Se **extrae** a `docs/adr/adr-NNNN-*.md` (Parte VII, cap. 28) |
| `anteproyecto.md` (glosario) | Cap. 4 (Glosario) |
| `Desglose/nexusmqdesglose.md` | Cap. 5 y 17 (vista de conjunto, mapa de módulos) |
| `Desglose/nexusmqdesglosedetallado.md` | Cap. 18 (catálogo por subsistema) |
| `hoja-de-ruta.md` | Cap. 29 (historia de desarrollo, en retrospectiva) |

Tras compilar el PDF y verificar que **nada** queda solo en la carpeta provisional, se **borra
`DocumentacionProvisional/`** y se publica el repositorio.

---

## 5. Orden de trabajo sugerido (cuando se apruebe este índice)

1. **Extraer los ADR** a `docs/adr/` (mecánico; cierra el único pendiente de documentación ya
   anotado en la hoja de ruta).
2. **Generar los diagramas** (§3) — son el andamiaje visual sobre el que se apoya la narrativa.
3. **Redactar los capítulos** por partes (I→VII), reutilizando contratos de `docs/` y el código.
4. **Compilar a PDF**, revisar, y **eliminar `DocumentacionProvisional/`**.
5. **Reactivar CI** (GitHub Actions) y **publicar** el repositorio.

> **Decisiones abiertas a tu criterio** (recomendación marcada): formato multi-archivo Markdown
> [recomendado], diagramas en **Mermaid** [recomendado], PDF vía **pandoc** [recomendado], y la
> profundidad del cap. 18 (catálogo por subsistema): ¿nivel "tipos clave + invariantes" o el
> detalle clase-a-clase del desglose detallado actual? Dímelo al revisar este índice.
