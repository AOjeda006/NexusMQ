# nexus-ffi — ABI C estable (F9) para *bindings* de otros lenguajes (Python/ctypes, …). Se construye
# como librería COMPARTIDA para poder cargarse en runtime (dlopen/ctypes). Tras una frontera
# extern "C" expone un subconjunto puro del núcleo: versión, CRC32C y contexto de traza W3C.
add_library(nexus-ffi SHARED
  nexus_ffi.cpp)
add_library(nexus::ffi ALIAS nexus-ffi)

target_link_libraries(nexus-ffi PUBLIC nexus::common nexus::telemetry nexus_options)

# Para enlazar los objetos de los archivos estáticos del núcleo dentro de una .so deben ser PIC.
set_target_properties(nexus-common nexus-telemetry PROPERTIES POSITION_INDEPENDENT_CODE ON)

# En Windows, una DLL solo exporta los símbolos marcados con __declspec(dllexport); como la ABI C
# se declara `extern "C"` sin esa anotación, el enlazador no genera la *import library*
# (`nexus-ffi.lib`) y cualquier consumidor (p. ej. la suite de tests) falla con LNK1104. La
# propiedad auto-exporta todos los símbolos y produce la .lib; además hace que la FFI sea de verdad
# cargable por ctypes/FFI en Windows. En ELF (Linux) los símbolos ya se exportan por defecto: la
# propiedad es inerte allí (W3-B, ADR-0028). (No usamos `__declspec` en la cabecera para no acoplar
# la ABI pública a un compilador.)
if(WIN32)
  set_target_properties(nexus-ffi PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif()
