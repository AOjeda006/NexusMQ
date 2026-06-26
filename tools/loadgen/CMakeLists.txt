# nexus-loadgen — generador de carga open-loop sobre la red (L1): mide la latencia end-to-end de
# produce a tasa fija sin coordinated omission. El motor (LoadGenerator) + el runner sobre el
# Client nativo viven en una librería para poder testearlos; el ejecutable solo cablea argv.

# Librería del motor (orquestación + runner sobre el Client). Reutiliza LatencyHistogram de
# tools/bench (header-only) → su directorio en el include-path.
add_library(nexus-loadgen
  load_generator.cpp
  produce_runner.cpp)
add_library(nexus::loadgen ALIAS nexus-loadgen)

target_include_directories(nexus-loadgen PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/tools/bench)

target_link_libraries(nexus-loadgen PUBLIC
  nexus::common
  nexus::client
  nexus_options)

# Ejecutable: parseo de argv + cableado.
add_executable(nexus-loadgen-cli main.cpp)
set_target_properties(nexus-loadgen-cli PROPERTIES OUTPUT_NAME nexus-loadgen)
target_link_libraries(nexus-loadgen-cli PRIVATE nexus::loadgen)
