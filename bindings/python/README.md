# Binding de Python para NexusMQ (F9)

*Binding* de Python sobre la **ABI C estable** de NexusMQ (`nexus-ffi`), cargada con
[`ctypes`](https://docs.python.org/3/library/ctypes.html). Se eligió esta vía en lugar de
*pybind11* (**ADR-0020**): no requiere las cabeceras de desarrollo de Python ni una *toolchain* de
extensiones, así que el *binding* se compila con el compilador de C++ del proyecto y se verifica con
cualquier intérprete de Python presente.

La frontera C (`src/ffi/nexus_ffi.h`) es reutilizable desde otros lenguajes con FFI (Rust, Go, Node…).

## Qué expone

| Python (`NexusMQ`)            | ABI C                        | Descripción                                       |
| ----------------------------- | ---------------------------- | ------------------------------------------------- |
| `version()`                   | `nexus_version`              | Cadena de versión de NexusMQ.                     |
| `crc32c(data)`                | `nexus_crc32c`               | CRC32C (Castagnoli) que el broker usa en records. |
| `format_traceparent(...)`     | `nexus_traceparent_format`   | Contexto de traza → encabezado W3C `traceparent`. |
| `parse_traceparent(header)`   | `nexus_traceparent_parse`    | `traceparent` W3C → componentes de la traza.      |

## Uso

```bash
# 1) Compilar la librería compartida (cualquier preset sirve).
cmake --preset linux-gcc && cmake --build --preset linux-gcc --target nexus-ffi

# 2) Apuntar el binding a la .so y ejecutar la prueba de humo.
NEXUS_FFI_LIBRARY=build/linux-gcc/src/ffi/libnexus-ffi.so \
    python3 bindings/python/smoke_test.py
```

```python
from nexusmq import NexusMQ

nx = NexusMQ("build/linux-gcc/src/ffi/libnexus-ffi.so")  # o NexusMQ() con NEXUS_FFI_LIBRARY
print(nx.version())
print(hex(nx.crc32c(b"hola mundo")))

tp = nx.format_traceparent(0x0af7651916cd43dd, 0x8448eb211c80319c, 0xb7ad6b7169203331)
ctx = nx.parse_traceparent(tp)
assert ctx.sampled
```

`NexusMQ()` sin argumentos busca la librería en `NEXUS_FFI_LIBRARY` y luego en las rutas del
*loader* del sistema (`libnexus-ffi.so` / `.dylib` / `.dll`).
