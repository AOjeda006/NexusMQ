"""Prueba de humo del *binding* de Python (ctypes) contra la librería compilada — F9.

Localiza ``libnexus-ffi`` (vía ``NEXUS_FFI_LIBRARY`` o las rutas habituales) y verifica
la versión, el CRC32C (contra el vector estándar de Castagnoli) y el *round-trip* del
``traceparent`` W3C. Imprime ``OK`` y sale con 0 si todo pasa; lanza si algo falla.

Ejecutar **tras compilar** ``nexus-ffi``, p. ej.::

    NEXUS_FFI_LIBRARY=build/linux-gcc/src/ffi/libnexus-ffi.so \\
        python3 bindings/python/smoke_test.py
"""

from __future__ import annotations

import sys

from nexusmq import NexusMQ

# Valor de comprobación estándar de CRC-32C (Castagnoli) para la cadena "123456789".
_CRC32C_CHECK = 0xE3069283
_EXAMPLE_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


def main() -> int:
    nexus = NexusMQ()

    assert nexus.version(), "la versión no debería estar vacía"

    assert nexus.crc32c(b"123456789") == _CRC32C_CHECK, hex(nexus.crc32c(b"123456789"))
    assert nexus.crc32c(b"") == 0, "el CRC de la entrada vacía es 0"

    ctx = nexus.parse_traceparent(_EXAMPLE_TRACEPARENT)
    assert ctx.trace_hi == 0x0AF7651916CD43DD, hex(ctx.trace_hi)
    assert ctx.trace_lo == 0x8448EB211C80319C, hex(ctx.trace_lo)
    assert ctx.span_id == 0xB7AD6B7169203331, hex(ctx.span_id)
    assert ctx.sampled, "el ejemplo está muestreado (flags=01)"

    roundtrip = nexus.format_traceparent(
        ctx.trace_hi, ctx.trace_lo, ctx.span_id, sampled=ctx.sampled
    )
    assert roundtrip == _EXAMPLE_TRACEPARENT, roundtrip

    try:
        nexus.parse_traceparent("no-es-un-traceparent")
    except ValueError:
        pass
    else:
        raise AssertionError("parse_traceparent debería rechazar entradas inválidas")

    print(f"nexusmq binding OK · NexusMQ {nexus.version()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
