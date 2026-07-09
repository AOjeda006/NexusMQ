# ADR-0030: Partición mono-protocolo — guarda cross-protocol nativo/Kafka

- **Estado:** aceptado
- **Fecha:** 2026-07-09

## Contexto

El plano de datos habla dos protocolos sobre el **mismo** almacenamiento: el **binario nativo**
([ADR-0004](adr-0004-protocolo-binario-propio-gateway-rest.md)) y el **subconjunto Kafka**
([ADR-0029](adr-0029-adaptador-kafka-async-cross-core.md)). Ambos anexan al mismo `PartitionLog`,
pero **codifican los records de forma incompatible**:

- El camino **nativo** guarda un `RecordBatch` propio cuyo payload son records nativos.
- El adaptador **Kafka** envuelve el `RecordBatch` v2 de Kafka como **blob opaco** dentro de un
  `RecordBatch` interno: `produce` cuenta sus records para avanzar offsets y `fetch` lo devuelve tal
  cual, reescribiendo el `baseOffset` (ADR-0029).

Nada impedía **mezclar** los dos protocolos en una misma partición. Si un productor nativo y un
consumidor Kafka (o viceversa) tocaban la misma partición, el consumidor recibía bytes que su
decodificador no sabe leer: el lado Kafka intentaba interpretar records nativos como un `RecordBatch`
v2 y, al fallar el *peek*, **devolvía cero records en silencio**; el lado nativo servía el blob de
Kafka como si fueran records nativos. En ambos casos el fallo era **silencioso y corruptor** —el
peor modo de fallo—: ni un error al productor ni al consumidor, solo datos ilegibles.

Un informe de pruebas (hallazgo P2) lo detectó: *native/Kafka cross-protocol incompatibility in the
same topic*. Había que **cerrar la puerta** a la mezcla y hacer el fallo **explícito**.

## Decisión

Una partición es **mono-protocolo**: el **primer** productor que escribe en ella **reclama** su
protocolo, y todo acceso posterior del **otro** protocolo se **rechaza con un error de wire**.

1. **Marca por partición.** `PartitionBase` lleva un `WireProtocol protocol_` (`Unset` → `Native` |
   `Kafka`), REACTOR-LOCAL (sin sincronizar: cada partición se toca solo desde su núcleo dueño,
   [ADR-0026](adr-0026-sharding-por-nucleo.md)). `claim_protocol(proto)` la reclama si está `Unset` o
   confirma que ya es la suya; si la posee el otro protocolo devuelve `InvalidArgument`.

2. **Reclamo en la escritura.** El `produce` de cada borde reclama **antes** de anexar: el
   `RequestRouter` con `Native`, el `KafkaServerBroker` con `Kafka`. Un cruce en produce se traduce a
   `InvalidRequest` en ambos wires (`from_error` en el nativo; `to_kafka_error` en Kafka), un error
   **permanente** (reintentar no ayuda).

3. **Rechazo en la lectura.** El `fetch` de cada borde consulta `protocol()`: si la partición la
   posee el **otro** protocolo, devuelve `InvalidRequest` en vez de servir bytes ilegibles. Esto
   **sustituye el retorno silencioso de cero records** por un error tipado y visible.

La marca es **por partición**, no por topic: cada partición es un log independiente y el cruce solo
corrompe dentro de una misma partición (ADR-0026). No se inventa un código de wire nuevo: se reutiliza
`InvalidRequest`, que ya existe en ambos protocolos, con un mensaje de contexto que nombra el cruce.

## Consecuencias

- (+) **Fin del fallo silencioso:** mezclar protocolos en una partición produce un error explícito
  (`InvalidRequest`) al productor o al consumidor infractor, en lugar de datos corruptos.
- (+) **Coste despreciable:** una comparación de enum en el borde de `produce`/`fetch`, sin E/S ni
  cross-core añadido; la marca vive con la partición en su núcleo dueño.
- (+) **Sin cambio de contrato de wire:** no hay código de error nuevo; los clientes existentes ya
  entienden `InvalidRequest`.
- (−) **Granularidad de proceso (en memoria):** `protocol_` no se persiste. Tras un reinicio arranca
  en `Unset` y la **primera** escritura vuelve a reclamar; el protocolo de los datos **ya presentes en
  disco** no se re-infiere al abrir el log. En la práctica una partición se dedica a un protocolo, así
  que la marca se re-establece con el mismo valor; persistir el protocolo (cabecera de log/metadatos)
  queda **diferido** a cuando exista un caso real de mezcla tras reinicio.
- (−) **No hay transcodificación:** no se convierte entre formatos; el objetivo es aislar, no
  interoperar dentro de una partición. Un topic pensado para ambos mundos usa particiones (o topics)
  separados por protocolo.

## Alternativas consideradas

- **No hacer nada (statu quo):** deja el fallo silencioso y corruptor. Descartada: es justo el
  hallazgo P2.
- **Marca por topic en vez de por partición:** más simple de razonar, pero más gruesa de lo
  necesario —el cruce solo daña dentro de una partición— y desalinea con el sharding partición→núcleo
  (la marca tendría que replicarse/consensuarse entre núcleos). Descartada a favor de por-partición.
- **Persistir el protocolo en el log/metadatos:** resolvería el caso *mezcla tras reinicio*, pero
  exige tocar el formato en disco y la recuperación por una situación que hoy no se da. Diferida.
- **Código de wire dedicado (p. ej. `CrossProtocol`):** más expresivo, pero amplía el contrato de
  errores en ambos protocolos por un caso de borde; `InvalidRequest` + mensaje basta. Descartada.
- **Transcodificar entre formatos al vuelo:** enorme superficie (semántica de records, timestamps,
  headers, compresión) y con pérdidas; contradice el diseño de blob opaco de ADR-0029. Descartada.
