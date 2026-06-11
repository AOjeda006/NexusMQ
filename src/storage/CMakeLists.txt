# nexus-storage — motor de log append-only (segmentos, índice, partición, retención,
# recuperación). Fase 1: E/S bloqueante (sobre nexus::io). Núcleo de aprendizaje del proyecto.
add_library(nexus-storage
  index.cpp)
add_library(nexus::storage ALIAS nexus-storage)

target_include_directories(nexus-storage PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-storage PUBLIC nexus::common nexus::io nexus_options)
