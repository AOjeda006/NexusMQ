# ADR-0008: Viabilidad de coste cero (desarrollo y test gratuitos; cloud como *target* opcional)

- **Estado:** aceptado
- **Fecha:** 2026-06-07

## Contexto

El proyecto se **orienta** a despliegue en Azure/AWS, pero su valor formativo y de portfolio no debe depender de gasto recurrente en cloud. Conviene fijar explícitamente que todo el ciclo de trabajo se puede realizar de forma gratuita.

## Decisión

Realizar **todo** el desarrollo, el testing (unitario, de integración y *crash*/chaos) y el *benchmarking* **en local, sin coste**:

- *Toolchain* y dependencias **open source**.
- Cluster de 3 nodos mediante **Docker Compose**.
- *Chaos* con `tc netem` y `cgroups`.
- CI en **GitHub Actions** (gratis en repositorios públicos / *free tier* en privados).
- Observabilidad **Prometheus + Grafana** *self-hosted*.

El despliegue en **cloud** es un **objetivo de diseño** (imagen Docker, principios 12-factor) y, como mucho, una **demo opcional** cubrible con *free tiers* (AWS/Azure/Oracle Always Free), nunca un requisito de gasto. Los *benchmarks* se miden **en local**, ya que los *free tiers* no sirven para obtener cifras serias.

## Consecuencias

- (+) Barrera de entrada económica nula y reproducibilidad del ciclo completo.
- (+) El diseño *cloud-ready* permite una demo desplegada si se desea.
- (−) Una demo en cloud real con *hardware* serio sí costaría dinero (egress, almacenamiento, instancias encendidas); se mitiga con IaC bajo demanda.
- (−) Los *free tiers* no permiten *benchmarks* representativos; se asume y por eso se miden en local.

## Alternativas consideradas

- **Depender de cloud para test/bench:** descartada por el coste y por la peor reproducibilidad de los *benchmarks*.
