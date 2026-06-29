# Diagrama 16: Layout de la cabecera del frame nativo

El protocolo binario propio de NexusMQ (plano de datos, puerto `9092`) es **longitud-prefijo**: toda
trama empieza con una cabecera de **14 bytes** (`FrameHeader::kEncodedSize`) seguida del payload. Los
enteros son **little-endian** y de ancho fijo. El lector lee 4 bytes (`length`) y luego exactamente
`length` bytes más (resto de cabecera + payload). Contrato: [`../protocol.md`](../protocol.md);
código: [`../../src/protocol/frame.hpp`](../../src/protocol/frame.hpp).

## Byte-layout (little-endian)

```
 0               4       6       8                 12      14
 +---------------+-------+-------+-----------------+-------+===============+
 |   length:u32  |apiKey |apiVer | correlationId   | flags |    payload    |
 |               | :u16  | :u16  |     :u32        | :u16  |   (length-10) |
 +---------------+-------+-------+-----------------+-------+===============+
```

| Campo           | Offset | Tamaño | Tipo  | Descripción                                                                  |
| --------------- | ------ | ------ | ----- | ---------------------------------------------------------------------------- |
| `length`        | 0      | 4      | `u32` | Bytes **tras** este campo (resto de cabecera + payload) = `10 + payload`.    |
| `apiKey`        | 4      | 2      | `u16` | Operación (ver tabla de ApiKeys).                                            |
| `apiVersion`    | 6      | 2      | `u16` | Versión del esquema de esa operación (negociada vía `ApiVersions`).         |
| `correlationId` | 8      | 4      | `u32` | Eco petición↔respuesta; lo elige el cliente.                                 |
| `flags`         | 12     | 2      | `u16` | Bits de control (ver abajo).                                                 |
| `payload`       | 14     | var.   | bytes | Cuerpo específico de la operación (`length - 10` bytes).                     |

La cabecera codificada ocupa 14 bytes; `length` (`= length_for(payload)`) cuenta solo los 10 bytes de
cabecera posteriores al propio campo más el payload. El lector valida una cota inferior (debe caber el
resto de cabecera) y una **cota superior** anti-DoS (`max_frame`, por defecto **16 MiB**).

## Campo `flags`

| Bit      | Nombre          | Significado                                                                        |
| -------- | --------------- | --------------------------------------------------------------------------------- |
| `0x0001` | credit update   | La trama acarrea una actualización de créditos (control de flujo / *backpressure*). |

## ApiKeys

| Valor | ApiKey         | Descripción                                        |
| ----- | -------------- | -------------------------------------------------- |
| 0     | `ApiVersions`  | Negociación de versiones soportadas.               |
| 1     | `Metadata`     | Brokers del clúster y topics/particiones.          |
| 2     | `Produce`      | Publica `RecordBatch` en una partición.            |
| 3     | `Fetch`        | Lee registros desde un offset.                     |
| 4     | `OffsetCommit` | Confirma el offset consumido de un grupo.          |
| 5     | `OffsetFetch`  | Recupera el offset confirmado de un grupo.         |
| 6     | `JoinGroup`    | Une un miembro a un grupo de consumidores.         |
| 7     | `SyncGroup`    | Distribuye la asignación de particiones del grupo. |
| 8     | `Heartbeat`    | Mantiene viva la pertenencia al grupo.             |
| 9     | `LeaveGroup`   | Abandona el grupo.                                 |
| 10    | `CreateTopic`  | Crea un topic.                                     |
| 11    | `DeleteTopic`  | Borra un topic.                                    |
