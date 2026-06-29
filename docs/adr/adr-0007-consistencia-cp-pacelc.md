# ADR-0007: Postura de consistencia (CP / PACELC PC-EC)

- **Estado:** aceptado
- **Fecha:** 2026-06-07

## Contexto

Todo sistema replicado debe elegir, ante una partición de red, entre **consistencia** y **disponibilidad** (CAP); y, en operación normal, entre **latencia** y **consistencia** (PACELC). El proyecto prioriza la correctitud y se apoya en Raft, que requiere quórum para progresar.

## Decisión

NexusMQ es un sistema **CP**: ante una partición de red, una partición de datos que se quede sin **quórum** de su grupo Raft **deja de aceptar escrituras** y, por tanto, no diverge.

En términos de PACELC es **PC/EC**: ante una **P**artición elige **C**onsistencia, y en condiciones normales (**E**lse) también prioriza **C**onsistencia sobre **L**atencia (las escrituras esperan al quórum cuando se usa `acks=quorum`).

La lectura por defecto se sirve desde el **líder**, hasta el *high-watermark*. Las lecturas *stale* desde *followers* son **opt-in** y quedan documentadas.

## Consecuencias

- (+) No hay divergencia ni pérdida de datos *committed*; el comportamiento es coherente con Raft y con el objetivo de *correctitud demostrable*.
- (+) Modelo mental simple para el usuario.
- (−) Una partición minoritaria queda **no disponible** para escritura durante un *split*; se acepta como precio de la consistencia.

## Alternativas consideradas

- **AP (alta disponibilidad, escrituras en minoría con reconciliación posterior):** ofrece mayor disponibilidad pero abre la puerta a la divergencia y a los conflictos; es incoherente con el modelo de log ordenado y con Raft. Descartada.
