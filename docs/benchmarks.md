# Benchmarks de latencia — NexusMQ

> Cifras de rendimiento **end-to-end** (produce sobre la red) del broker. Es uno de los entregables
> declarados del proyecto. La metodología sigue
> `fundamentos/rendimiento/` de la biblioteca: medición **open-loop**, percentiles (no medias) e
> interpretación por el método **USE** (utilización / saturación / errores).

## Qué se mide

La **latencia de produce extremo a extremo**: desde que el cliente nativo (`nexus::Client`) envía un
record hasta que recibe el `ack` del broker, por **TCP** (loopback). El generador de carga
[`nexus-loadgen`](../tools/loadgen) abre N conexiones (una por hilo, *shared-nothing*), publica a una
**tasa fija** y registra cada latencia en un `LatencyHistogram` estilo HdrHistogram (p50/p99/p999/max).

### Por qué open-loop (corrección de *coordinated omission*)

El generador es **open-loop**: las peticiones se programan a instantes fijos (`epoch + i/tasa`) y la
latencia de cada una se mide **contra su instante previsto**, no contra el momento real de envío. Así,
cuando el broker se satura y el generador se retrasa, ese retraso **entra en la cola medida** en lugar
de desaparecer. Un cliente *closed-loop* (que espera cada respuesta antes de enviar la siguiente)
**subestima la cola** bajo saturación: deja de enviar justo las peticiones que más tardarían (Gregg,
*Systems Performance*; Tene, *HdrHistogram*). La divergencia de las dos últimas filas de la tabla es
exactamente lo que un cliente closed-loop habría ocultado.

## Entorno de medición

| Parámetro        | Valor                                                              |
|------------------|-------------------------------------------------------------------|
| CPU              | 4× Intel Xeon @ 2.80 GHz (VM en contenedor, sin aislamiento)      |
| RAM              | 15 GiB                                                             |
| Kernel           | Linux 6.18.5 (E/S del broker sobre `io_uring`)                     |
| Build            | `linux-gcc-release` (GCC, `-O3 -DNDEBUG`)                          |
| Topología        | **un** nodo, **una** partición, sin réplica (Fase 1b)             |
| Transporte       | TCP sobre **loopback** (sin RTT de red real)                      |
| Payload          | 256 B por record                                                  |
| Conexiones       | 4 (= hilos del generador)                                         |
| Duración/nivel   | 8 s (calentamiento de 1 s descartado antes de medir)             |

> **Honestidad sobre las cifras:** es una VM compartida en la nube, **no** *bare metal* aislado, y la
> medición es sobre loopback. Los **valores absolutos** (sobre todo la cola p999/max) incluyen ruido
> del planificador y de vecinos; lo que el experimento demuestra de forma robusta es la **metodología**
> (open-loop) y el **comportamiento relativo**: dónde está el codo de saturación y cómo diverge la
> latencia al cruzarlo. No es un benchmark competitivo contra otros brokers.

## Cómo reproducir

```sh
cmake --preset linux-gcc-release -DNEXUS_BUILD_TESTS=OFF
cmake --build --preset linux-gcc-release
scripts/bench/latency_campaign.sh            # barrido por defecto
# o, parametrizado:
DURATION=8 RATES="1000 2000 5000 10000 15000 30000 0" \
  scripts/bench/latency_campaign.sh
```

El script arranca un `nexusd` efímero (puerto y `data-dir` temporales), espera a que acepte
conexiones, barre las tasas y, al terminar, mata el broker y borra los datos.

## Resultados

Campaña con el barrido de arriba (payload 256 B, 4 conexiones):

| Tasa objetivo     | Throughput logrado | Errores | p50      | p99      | p999     | max      |
|-------------------|--------------------|---------|----------|----------|----------|----------|
| 1 000 req/s       | 1 000 req/s        | 0       | 217 µs   | 324 µs   | 5.9 ms   | 10.6 ms  |
| 2 000 req/s       | 2 000 req/s        | 0       | 211 µs   | 373 µs   | 7.5 ms   | 9.9 ms   |
| 5 000 req/s       | 5 000 req/s        | 0       | 188 µs   | 4.2 ms   | 9.2 ms   | 11.0 ms  |
| 10 000 req/s      | 10 000 req/s       | 0       | 174 µs   | 5.6 ms   | 7.9 ms   | 9.9 ms   |
| 15 000 req/s      | 15 000 req/s       | 0       | 242 µs   | 7.8 ms   | 9.0 ms   | 11.8 ms  |
| 30 000 req/s      | ~18 200 req/s      | 0       | diverge †| diverge †| diverge †| diverge †|
| máx (sin ritmo)   | **~18 700 req/s**  | 0       | diverge †| diverge †| diverge †| diverge †|

† **Saturado.** Al pedir más de lo que el broker sostiene, el *backlog* open-loop crece sin cota y la
latencia medida depende de cuánto dure la prueba (no es una latencia de servicio). La cifra útil de
esas filas es el **throughput tope** (~18.7k req/s), no el percentil.

## Interpretación (método USE)

- **Región sana (≤ 2 000 req/s):** p50 ~210 µs y **p99 sub-milisegundo** (≤ 373 µs). Es el punto de
  operación con buen SLO de cola.
- **Codo de saturación (5 000–15 000 req/s):** el throughput aún **iguala** al objetivo (sin errores,
  sin pérdida), pero el **p99 sube a varios ms**: empieza el encolado. La mediana se mantiene baja
  (~175–240 µs) — la cola crece antes que el centro, justo lo que los percentiles revelan y la media
  ocultaría.
- **Saturación (> ~18 700 req/s):** el throughput logrado se **estanca** en ~18.7k req/s aunque se pida
  más, y la latencia open-loop **diverge**. Ese plató es el techo de produce de **una partición** en
  este entorno; escalar pasa por **más particiones** (paralelismo *thread-per-core*, Fase 1b+) más que
  por exprimir una sola.
- **Errores:** 0 en todos los niveles (la columna E del método USE) — el broker no descarta ni falla
  bajo carga; degrada en latencia, no en pérdida.

## Conclusión

El camino de produce sostiene ~**18.7k req/s** por partición con **p50 ~200 µs**, manteniendo **p99
sub-milisegundo hasta ~2k req/s** y de pocos ms hasta el codo. La medición open-loop hace visible la
cola que un cliente closed-loop habría borrado, y deja un **arnés reproducible** (`scripts/bench/`) para
repetir la campaña tras cada cambio de rendimiento (línea base → cambio → re-medir).
