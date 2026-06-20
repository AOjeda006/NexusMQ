/// @file   ffi/nexus_ffi.h
/// @brief  ABI C estable de NexusMQ para *bindings* de otros lenguajes (Python/ctypes, …) — F9.
/// @ingroup ffi
///
/// @details Expone un subconjunto **puro y transversal** del núcleo a través de una frontera C
///   (`extern "C"`, sin nombres mangled ni tipos C++): la versión, el checksum **CRC32C** que el
///   broker usa para la integridad de los records y el codec de **contexto de traza W3C** para
///   propagar trazas distribuidas (F8). Pensado para cargarse con `ctypes`/FFI desde Python u otros
///   lenguajes. Mantener la ABI **estable**: no cambiar firmas existentes; añadir, no romper.

#ifndef NEXUS_FFI_H
#define NEXUS_FFI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// Cadena de versión de NexusMQ (terminada en NUL; propiedad de la librería, no liberar).
const char* nexus_version(void);

/// @brief CRC32C (Castagnoli) de `[data, data+len)`; el mismo que NexusMQ aplica a los records.
/// @param data Puntero a los bytes (puede ser NULL si `len == 0`).
/// @return El checksum de 32 bits (0 para una entrada vacía, con semilla 0).
uint32_t nexus_crc32c(const uint8_t* data, size_t len);

/// @brief Formatea un contexto de traza al encabezado **W3C `traceparent`** (versión `00`).
/// @param out     Búfer de salida (recibe 55 chars + NUL).
/// @param out_cap Capacidad de @p out; debe ser >= 56.
/// @return 0 en éxito; -1 si @p out es NULL o @p out_cap es insuficiente.
int nexus_traceparent_format(uint64_t trace_hi, uint64_t trace_lo, uint64_t span_id, uint8_t flags,
                             char* out, size_t out_cap);

/// @brief Parsea un encabezado **W3C `traceparent`** (versión `00`) a sus componentes.
/// @param header  Cadena terminada en NUL a parsear.
/// @param trace_hi,trace_lo,span_id,flags Punteros de salida (cualquiera puede ser NULL).
/// @return 0 en éxito; -1 si @p header es NULL o el encabezado es inválido.
int nexus_traceparent_parse(const char* header, uint64_t* trace_hi, uint64_t* trace_lo,
                            uint64_t* span_id, uint8_t* flags);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // NEXUS_FFI_H
