# nexus-protocol — protocolo binario propio (ADR-0004): framing, codec, mensajes, errores.
# Capa: depende solo de nexus-common (puro encode/decode, sin E/S ni async).
add_library(nexus-protocol
  codec.cpp
  error_code.cpp
  frame.cpp)
add_library(nexus::protocol ALIAS nexus-protocol)

target_include_directories(nexus-protocol PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-protocol PUBLIC nexus::common nexus_options)
