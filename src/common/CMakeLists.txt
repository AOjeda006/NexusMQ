# nexus-common — tipos transversales, bytes, CRC32C, error, config, log, reloj.
# Por ahora (M1) solo expone la versión del proyecto.

add_library(nexus-common
  version.cpp
  crc32c.cpp
  bytes.cpp
  record.cpp
  record_codec.cpp
  control_record.cpp
  compression.cpp
  varint.cpp
  sha256.cpp
  base64.cpp)
add_library(nexus::common ALIAS nexus-common)

# La raíz de inclusión es src/, de modo que se incluye como "common/version.hpp".
# PUBLIC: lo heredan los consumidores (tests, otros targets) sin repetirlo.
target_include_directories(nexus-common PUBLIC ${PROJECT_SOURCE_DIR}/src)

# C++20 + avisos como errores (vía el target de opciones del proyecto).
target_link_libraries(nexus-common PUBLIC nexus_options)

# Inyecta la versión desde project(VERSION ...) como literal de cadena:
# única fuente de verdad (DRY). version.cpp devuelve NEXUSMQ_VERSION.
target_compile_definitions(nexus-common PRIVATE NEXUSMQ_VERSION="${PROJECT_VERSION}")

# Compresión (LZ4/Zstd): opcional, como el plano TLS (ADR-0019). Si la librería está disponible,
# compression.cpp activa el códec correspondiente vía NEXUS_HAVE_LZ4 / NEXUS_HAVE_ZSTD; si no, ese
# códec queda como `Unsupported` (None siempre funciona). Las rutas pueden forzarse con
# -DLZ4_INCLUDE_DIR/-DLZ4_LIBRARY/-DZSTD_INCLUDE_DIR/-DZSTD_LIBRARY (build sin paquete del sistema).
find_path(LZ4_INCLUDE_DIR NAMES lz4.h)
find_library(LZ4_LIBRARY NAMES lz4 liblz4)
if(LZ4_INCLUDE_DIR AND LZ4_LIBRARY)
  target_compile_definitions(nexus-common PUBLIC NEXUS_HAVE_LZ4)
  target_include_directories(nexus-common SYSTEM PRIVATE ${LZ4_INCLUDE_DIR})
  target_link_libraries(nexus-common PRIVATE ${LZ4_LIBRARY})
  message(STATUS "NexusMQ: LZ4 habilitado (${LZ4_LIBRARY})")
else()
  message(STATUS "NexusMQ: LZ4 no encontrado; códec Lz4 deshabilitado")
endif()

find_path(ZSTD_INCLUDE_DIR NAMES zstd.h)
find_library(ZSTD_LIBRARY NAMES zstd libzstd)
if(ZSTD_INCLUDE_DIR AND ZSTD_LIBRARY)
  target_compile_definitions(nexus-common PUBLIC NEXUS_HAVE_ZSTD)
  target_include_directories(nexus-common SYSTEM PRIVATE ${ZSTD_INCLUDE_DIR})
  target_link_libraries(nexus-common PRIVATE ${ZSTD_LIBRARY})
  message(STATUS "NexusMQ: Zstd habilitado (${ZSTD_LIBRARY})")
else()
  message(STATUS "NexusMQ: Zstd no encontrado; códec Zstd deshabilitado")
endif()
