# nexus-io — abstracción de E/S. Fase 1: File bloqueante. Único lugar con código
# específico de plataforma (en 1b/4 entran io_uring e IOCP tras el mismo puerto).
add_library(nexus-io
  file.cpp
  aligned_buffer.cpp
  block_reader.cpp
  socket.cpp)
add_library(nexus::io ALIAS nexus-io)

# Backend io_uring: solo donde está el uapi del kernel (Linux). Habla por syscalls
# directos (sin liburing, ADR-0012); define NEXUS_HAVE_IOURING para que el smoke-test
# se compile y, en tiempo de ejecución, se omita si io_uring no está disponible.
include(CheckIncludeFileCXX)
check_include_file_cxx("linux/io_uring.h" NEXUS_HAS_IOURING_HEADER)
if(NEXUS_HAS_IOURING_HEADER)
  target_sources(nexus-io PRIVATE io_uring_backend.cpp)
  target_compile_definitions(nexus-io PUBLIC NEXUS_HAVE_IOURING)
endif()

target_include_directories(nexus-io PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-io PUBLIC nexus::common nexus_options)
