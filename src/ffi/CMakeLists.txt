# nexus-ffi — ABI C estable (F9) para *bindings* de otros lenguajes (Python/ctypes, …). Se construye
# como librería COMPARTIDA para poder cargarse en runtime (dlopen/ctypes). Tras una frontera
# extern "C" expone un subconjunto puro del núcleo: versión, CRC32C y contexto de traza W3C.
add_library(nexus-ffi SHARED
  nexus_ffi.cpp)
add_library(nexus::ffi ALIAS nexus-ffi)

target_link_libraries(nexus-ffi PUBLIC nexus::common nexus::telemetry nexus_options)

# Para enlazar los objetos de los archivos estáticos del núcleo dentro de una .so deben ser PIC.
set_target_properties(nexus-common nexus-telemetry PROPERTIES POSITION_INDEPENDENT_CODE ON)
