# Documentación técnica de NexusMQ

> Documentación técnica **final** de NexusMQ (broker de mensajería distribuido en C++23,
> *shared-nothing thread-per-core*, Raft por partición). Es la **fuente de verdad** del
> proyecto: se apoya en los **contratos as-built** de [`docs/`](../) y en los
> [diagramas](../diagramas/) y [ADR](../adr/).

> Disponible también como **[PDF único](../pdf/NexusMQ-documentacion-tecnica.pdf)** (documentación
> técnica + catálogo de diagramas + registro de ADR). Se regenera con [`docs/pdf/`](../pdf/).

Lectura recomendada en orden; cada capítulo es autocontenido y enlaza con los demás.

- [Prefacio](./00-prefacio.md)

## Parte I — Introducción y visión
- [1. Resumen ejecutivo](./01-resumen-ejecutivo.md)
- [2. Contexto y motivación](./02-contexto-y-motivacion.md)
- [3. Estado del arte](./03-estado-del-arte.md)
- [4. Glosario](./04-glosario.md)

## Parte II — Arquitectura
- [5. Vista de conjunto](./05-vista-de-conjunto.md)
- [6. Principios de diseño](./06-principios-de-diseno.md)
- [7. Concurrencia](./07-concurrencia.md)
- [8. Modelo de I/O](./08-modelo-de-io.md)
- [9. Almacenamiento](./09-almacenamiento.md)
- [10. Replicación y consenso](./10-replicacion-y-consenso.md)
- [11. Ingress](./11-ingress.md)
- [12. Observabilidad](./12-observabilidad.md)

## Parte III — Contratos
- [13. Protocolo binario nativo](./13-protocolo-binario-nativo.md)
- [14. Subconjunto Kafka](./14-subconjunto-kafka.md)
- [15. API REST de administración](./15-api-rest-administracion.md)
- [16. Modelo de errores y códigos de wire](./16-modelo-errores-wire-codes.md)

## Parte IV — Implementación
- [17. Mapa de módulos](./17-mapa-de-modulos.md)
- [18. Catálogo por subsistema](./18-catalogo-por-subsistema.md)
- [19. Arranque y composition root](./19-arranque-y-composition-root.md)
- [20. Herramientas y bindings](./20-herramientas-y-bindings.md)

## Parte V — Calidad
- [21. Estrategia de pruebas](./21-estrategia-de-pruebas.md)
- [22. Puerta de calidad y CI/CD](./22-puerta-de-calidad-y-cicd.md)
- [23. Rendimiento y benchmarks](./23-rendimiento-y-benchmarks.md)
- [24. Portabilidad](./24-portabilidad.md)

## Parte VI — Operación y despliegue
- [25. Despliegue](./25-despliegue.md)
- [26. Configuración y operación](./26-configuracion-y-operacion.md)
- [27. Seguridad](./27-seguridad.md)

## Parte VII — Decisiones y evolución
- [28. Registro de decisiones (ADR)](./28-registro-de-decisiones-adr.md)
- [29. Historia de desarrollo](./29-historia-de-desarrollo.md)
- [30. Limitaciones y trabajo futuro](./30-limitaciones-y-trabajo-futuro.md)

## Apéndices
- [A. Bibliografía](./99-bibliografia.md)
- [B. Índice de diagramas](./98-indice-de-diagramas.md)
- Contratos as-built: [protocol.md](../protocol.md) · [kafka.md](../kafka.md) · [openapi.yaml](../openapi.yaml) · [benchmarks.md](../benchmarks.md)
