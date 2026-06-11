# nexus-io — abstracción de E/S. Fase 1: File bloqueante. Único lugar con código
# específico de plataforma (en 1b/4 entran io_uring e IOCP tras el mismo puerto).
add_library(nexus-io
  file.cpp)
add_library(nexus::io ALIAS nexus-io)

target_include_directories(nexus-io PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-io PUBLIC nexus::common nexus_options)
