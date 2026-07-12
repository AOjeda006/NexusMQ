# nexus-storage — motor de log append-only (segmentos, índice, partición, retención,
# recuperación). Fase 1: E/S bloqueante (sobre nexus::io). Núcleo de aprendizaje del proyecto.
add_library(nexus-storage
  index.cpp
  segment.cpp
  segment_crypto.cpp
  storage_tier.cpp
  local_storage_tier.cpp
  partition_log.cpp
  log_compactor.cpp)
add_library(nexus::storage ALIAS nexus-storage)

target_include_directories(nexus-storage PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-storage PUBLIC nexus::common nexus::io nexus_options)

# Cifrado en reposo (OpenSSL): opcional (ADR-0031). Si OpenSSL está disponible, el motor de log
# cifra los segmentos con AES-256-GCM (segment_crypto.cpp) y define NEXUS_HAVE_OPENSSL; si no, el
# broker arranca en claro (degradación limpia) y los tests de cifrado hacen GTEST_SKIP. Solo se
# necesita libcrypto (KDF/AEAD/CSPRNG), no libssl.
find_package(OpenSSL)
if(OpenSSL_FOUND)
  target_compile_definitions(nexus-storage PUBLIC NEXUS_HAVE_OPENSSL)
  target_link_libraries(nexus-storage PUBLIC OpenSSL::Crypto)
endif()
