# 20. Herramientas y bindings

> Lo que rodea al servidor: la CLI de operación, los generadores de carga y *benchmark*, el
> arnés de verificación de Windows y el *binding* de Python. Son piezas de soporte, no del
> camino caliente, pero esenciales para operar, medir y demostrar el sistema.

## 20.1 `nexus-cli`

CLI de administración y diagnóstico. Habla con el **plano de operación** (REST `/api/v1`) y/o
el plano de datos: crea/lista/describe/borra *topics*, lista grupos de consumidores y produce/
consume de forma puntual. Acepta un *token* Bearer (`--token`) cuando el nodo arranca con
`--jwt-secret`. Es el camino humano equivalente a las llamadas REST del
[capítulo 15](./15-api-rest-administracion.md).

## 20.2 `nexus-bench`

*Microbenchmarks* reproducibles (Google Benchmark) sobre el núcleo —principalmente el
*storage engine*— para medir el coste de operaciones concretas (append, lectura, CRC32C,
codificación). Se usa para establecer **líneas base** y comparar antes/después de una
optimización, con la metodología del [capítulo 23](./23-rendimiento-y-benchmarks.md).

## 20.3 `nexus-loadgen`

Generador de carga *end-to-end* contra un nodo/cluster vivo, sobre el cliente nativo
(`nexus-client`). Parámetros típicos: `--connections`, `--rate`, `--count`, `--payload`,
`--warmup`, `--topic`, `--partition`. Es **open-loop** (tasa fija, independiente de las
respuestas) para **evitar la *coordinated omission*** y medir la latencia de cola real bajo
una carga representativa (ver [capítulo 23](./23-rendimiento-y-benchmarks.md)).

## 20.4 `wincheck`

Arnés de verificación **Windows-only** que confirma el backend IOCP en runtime
([ADR-0023](../adr/adr-0023-iocp-runtime-msvc.md)): comprueba `File` (bloqueante y directa),
un eco IOCP por *loopback* end-to-end con cierre limpio, y `submit_timer`. Los cuatro casos
pasan con MSVC (VS 2026, `/W4 /WX`); fue la herramienta que cerró la deuda de runtime del port
a Windows.

## 20.5 Binding de Python (`nexus-ffi` + `ctypes`)

El *binding* de Python no usa pybind11 sino una **ABI C estable**
([ADR-0020](../adr/adr-0020-binding-python-abi-c.md)): `nexus-ffi` expone una API en C
(`nexus_ffi.h`) compilada como librería **SHARED**, y el lado Python la carga con **`ctypes`**
(en `bindings/python/`). Ventajas: el binding **no necesita `python3-dev`** ni recompilarse
contra cada versión de Python (que era el motivo de descartar pybind11), y la frontera C es
estable y portable. La ABI C envuelve al cliente nativo (`nexus-client`), de modo que Python
produce y consume hablando el mismo protocolo que un cliente C++.
