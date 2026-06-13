# nexus-wire — framing sobre conexión (ADR-0013): lee/escribe tramas del protocolo
# sobre un Socket async. Compone protocolo (codec/frame) + io (Socket/Proactor), de
# modo que nexus-protocol queda PURO (encode/decode, sin E/S ni async).
add_library(nexus-wire
  frame_io.cpp)
add_library(nexus::wire ALIAS nexus-wire)

target_include_directories(nexus-wire PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-wire PUBLIC nexus::common nexus::io nexus::protocol nexus_options)
