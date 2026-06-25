# nexus-wire — framing sobre conexión (ADR-0013): lee/escribe tramas del protocolo
# sobre un flujo async (Socket en claro o TlsConnection cifrado). Compone protocolo
# (codec/frame) + io (Proactor), de modo que nexus-protocol queda PURO (encode/decode,
# sin E/S ni async). Header-only: FrameReader/FrameWriter son plantillas sobre el
# concept ByteStream (DI estática en el hot-path), así que el target es INTERFACE.
add_library(nexus-wire INTERFACE)
add_library(nexus::wire ALIAS nexus-wire)

target_include_directories(nexus-wire INTERFACE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-wire INTERFACE nexus::common nexus::io nexus::protocol nexus_options)
