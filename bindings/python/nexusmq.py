"""Binding de Python para NexusMQ a través de su ABI C estable (``nexus-ffi``) — F9.

Carga la librería compartida ``libnexus-ffi`` con :mod:`ctypes` y expone una API
*pythónica* sobre el subconjunto transversal del núcleo: la versión, el checksum
**CRC32C** que el broker aplica a los records y el codec de **contexto de traza
W3C** (``traceparent``) para propagar trazas distribuidas (ver F8).

Se eligió ``ctypes`` sobre una frontera C en lugar de *pybind11* (ADR-0020): no
requiere cabeceras de desarrollo de Python ni una *toolchain* de extensiones, de
modo que el *binding* se compila con el propio compilador de C++ y se verifica con
cualquier intérprete de Python presente.

Uso::

    from nexusmq import NexusMQ
    nx = NexusMQ()                       # busca libnexus-ffi en rutas habituales
    nx = NexusMQ("build/linux-gcc/src/ffi/libnexus-ffi.so")  # o ruta explícita
    print(nx.version())
    print(hex(nx.crc32c(b"hola")))
    tp = nx.format_traceparent(0x0af7651916cd43dd, 0x8448eb211c80319c,
                               0xb7ad6b7169203331, sampled=True)
    ctx = nx.parse_traceparent(tp)
"""

from __future__ import annotations

import ctypes
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SpanContext:
    """Contexto de traza W3C ya parseado (las dos mitades del trace-id, el span y los flags)."""

    trace_hi: int
    trace_lo: int
    span_id: int
    flags: int

    @property
    def sampled(self) -> bool:
        """¿Está la traza marcada como muestreada (bit 0 de las trace-flags)?"""
        return bool(self.flags & 0x01)


def _load_library(path: str | None) -> ctypes.CDLL:
    candidates: list[str] = []
    if path:
        candidates.append(path)
    env = os.environ.get("NEXUS_FFI_LIBRARY")
    if env:
        candidates.append(env)
    candidates += ["libnexus-ffi.so", "libnexus-ffi.dylib", "nexus-ffi.dll"]

    last_error: OSError | None = None
    for candidate in candidates:
        try:
            return ctypes.CDLL(candidate)
        except OSError as error:  # noqa: PERF203 - se prueban varias rutas a propósito
            last_error = error
    raise OSError(
        "no se pudo cargar libnexus-ffi; pásala por ruta o exporta NEXUS_FFI_LIBRARY "
        f"(último error: {last_error})"
    )


class NexusMQ:
    """Fachada *pythónica* sobre la ABI C de NexusMQ (``nexus-ffi``)."""

    _TRACEPARENT_BUFFER = 56  # 55 chars + NUL

    def __init__(self, library_path: str | None = None) -> None:
        self._lib = _load_library(library_path)
        self._configure_signatures()

    def _configure_signatures(self) -> None:
        lib = self._lib
        lib.nexus_version.restype = ctypes.c_char_p
        lib.nexus_version.argtypes = []

        lib.nexus_crc32c.restype = ctypes.c_uint32
        lib.nexus_crc32c.argtypes = [ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t]

        lib.nexus_traceparent_format.restype = ctypes.c_int
        lib.nexus_traceparent_format.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint8,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]

        lib.nexus_traceparent_parse.restype = ctypes.c_int
        lib.nexus_traceparent_parse.argtypes = [
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint8),
        ]

    def version(self) -> str:
        """Cadena de versión de NexusMQ."""
        return self._lib.nexus_version().decode("utf-8")

    def crc32c(self, data: bytes) -> int:
        """CRC32C (Castagnoli) de ``data``; idéntico al que usa el broker en los records."""
        if not data:
            return int(self._lib.nexus_crc32c(None, 0))
        buffer = (ctypes.c_uint8 * len(data)).from_buffer_copy(data)
        return int(self._lib.nexus_crc32c(buffer, len(data)))

    def format_traceparent(
        self, trace_hi: int, trace_lo: int, span_id: int, *, sampled: bool = True
    ) -> str:
        """Serializa un contexto de traza al encabezado W3C ``traceparent`` (versión ``00``)."""
        out = ctypes.create_string_buffer(self._TRACEPARENT_BUFFER)
        flags = 0x01 if sampled else 0x00
        code = self._lib.nexus_traceparent_format(
            trace_hi, trace_lo, span_id, flags, out, self._TRACEPARENT_BUFFER
        )
        if code != 0:
            raise ValueError("no se pudo formatear el traceparent")
        return out.value.decode("ascii")

    def parse_traceparent(self, header: str) -> SpanContext:
        """Parsea un encabezado W3C ``traceparent``; lanza ``ValueError`` si es inválido."""
        trace_hi = ctypes.c_uint64()
        trace_lo = ctypes.c_uint64()
        span_id = ctypes.c_uint64()
        flags = ctypes.c_uint8()
        code = self._lib.nexus_traceparent_parse(
            header.encode("ascii"),
            ctypes.byref(trace_hi),
            ctypes.byref(trace_lo),
            ctypes.byref(span_id),
            ctypes.byref(flags),
        )
        if code != 0:
            raise ValueError(f"traceparent inválido: {header!r}")
        return SpanContext(trace_hi.value, trace_lo.value, span_id.value, flags.value)
