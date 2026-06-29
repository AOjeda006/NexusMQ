# ADR-0018: REST admin por puerto/adaptador (`AdminService` en ingress, `AdminApi` en server)

- **Estado:** aceptado
- **Fecha:** 2026-06-18

## Contexto

Este ADR **refina el desglose** (no cambia ninguna decisión de arquitectura previa): el desglose detallado (§4.9) ubicaba `admin_api.{hpp,cpp}` dentro de `nexus-server` y, a la vez (§4.8), hacía que `RestGateway` (en `nexus-ingress`) tuviera un `AdminApi&`. Eso crea un **ciclo de capas** `ingress → server` (la pasarela usa la API) y `server → ingress` (el `Server` posee la `Ingress`). La decisión rompe el ciclo con **inversión de dependencias**, igual que ADR-0017 hizo con la telemetría.

El `RestGateway` (plano de ingress) traduce HTTP↔dominio y necesita ejecutar operaciones de administración (crear/borrar/describir/listar topics y grupos). La lógica de esas operaciones vive sobre `TopicManager`/`GroupCoordinator`, que son del **broker** (capa inferior a server, pero `ingress` **no** depende de broker en el grafo: `ingress → common, io, protocol, wire`). Si el gateway dependiera de un `AdminApi` concreto alojado en `nexus-server`, `ingress` dependería de `server` (que ya depende de `ingress`): ciclo.

## Decisión

Se define el **puerto** `AdminService` (interfaz abstracta) y sus **DTOs** (`CreateTopicSpec`, `TopicSummary`, `PartitionInfo`, `TopicDescription`, `GroupSummary`) en **`nexus-ingress`** (`ingress/admin_service.hpp`), como tipos de datos planos sin dependencia del broker. El `RestGateway` depende solo de `AdminService&`. El **adaptador** concreto `AdminApi` (en **`nexus-server`**, `server/admin_api.{hpp,cpp}`) **implementa** `AdminService` sobre `TopicManager&` y un *group lister* inyectado (`std::function`), traduciendo los tipos del broker a los DTOs del puerto. La enumeración de grupos es **reactor-local** (cada `GroupCoordinator` vive en su reactor), así que se inyecta como función desde el cableado del server (I14), no se acopla al puerto. Se añade a `TopicManager` un accesor de observabilidad `list_metadata()` (control-plane).

## Consecuencias

- (+) Grafo **acíclico**: `ingress` define el puerto (sin nuevas dependencias), `server` implementa el adaptador (ya depende de `ingress` y `broker`).
- (+) `RestGateway` es **testeable** con un doble de `AdminService` sin levantar broker.
- (+) Los DTOs del REST quedan **desacoplados** de los tipos internos (ADR-0009: traducción en el borde).
- (−) Una capa de traducción DTO↔dominio en el adaptador.
- (−) El listado de grupos cross-core real se materializa en el cableado (I14), no en el puerto.

## Alternativas consideradas

- **`AdminApi` concreto en `nexus-server`, referenciado por `ingress` (desglose literal):** crea el ciclo `ingress ↔ server`; descartado.
- **`AdminApi` en `nexus-broker`:** tendría que implementar la interfaz `AdminService` de `ingress` ⇒ `broker → ingress` (dependencia **ascendente**, prohibida); descartado.
- **`RestGateway` como router genérico de *handlers* (`std::function`):** rompe el ciclo, pero diluye el contrato de administración en *callbacks* sin tipado; el puerto explícito documenta mejor la API y es más fiel al desglose.
