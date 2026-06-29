# ADR-0006: Rol del *ingress* (dos modos: nativo directo + proxy/REST)

- **Estado:** aceptado
- **Fecha:** 2026-06-07

## Contexto

En un plano de datos de alto rendimiento, interponer un proxy en el camino caliente añade un salto y rompe el *zero-copy*. Pero exigir siempre un *smart-client* excluye a los clientes simples y a HTTP. Existe, por tanto, una tensión explícita entre rendimiento y conveniencia.

## Decisión

Soportar **dos modos** con una jerarquía explícita:

1. **Nativo directo** (primario): un *smart-client* que va al **líder** de la partición vía *metadata*, sin proxy.
2. **Proxy/REST** (secundario, *opt-in*): el *ingress* enruta clientes "tontos" mediante *consistent hashing* y expone un **gateway REST**, asumiendo el salto extra a sabiendas.

Se documenta como **dos modos con *trade-offs***, no como "un *ingress* que lo hace todo".

## Consecuencias

- (+) Máximo rendimiento por defecto y conveniencia/interoperabilidad bajo demanda; demuestra **criterio** en vez de incoherencia.
- (−) Dos caminos que mantener y documentar; el cliente nativo debe gestionar *metadata* y el descubrimiento de líder.

## Alternativas consideradas

- **Solo proxy:** simple para el cliente, pero penaliza el camino caliente; descartado como único.
- **Solo smart-client:** óptimo en rendimiento, pero excluye a clientes simples y a HTTP; descartado como único.
