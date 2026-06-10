# nexus-common — tipos transversales, bytes, CRC32C, error, config, log, reloj.
# Por ahora (M1) solo expone la versión del proyecto.

add_library(nexus-common
  version.cpp)
add_library(nexus::common ALIAS nexus-common)

# La raíz de inclusión es src/, de modo que se incluye como "common/version.hpp".
# PUBLIC: lo heredan los consumidores (tests, otros targets) sin repetirlo.
target_include_directories(nexus-common PUBLIC ${PROJECT_SOURCE_DIR}/src)

# C++20 + avisos como errores (vía el target de opciones del proyecto).
target_link_libraries(nexus-common PUBLIC nexus_options)

# Inyecta la versión desde project(VERSION ...) como literal de cadena:
# única fuente de verdad (DRY). version.cpp devuelve NEXUSMQ_VERSION.
target_compile_definitions(nexus-common PRIVATE NEXUSMQ_VERSION="${PROJECT_VERSION}")
