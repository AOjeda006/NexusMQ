# nexus-client — cliente nativo del broker (Fase 1b, mono-nodo): conexión TCP bloqueante +
# petición/respuesta enmarcada, con las fachadas Producer/Consumer. No depende de broker/consensus
# (es un cliente puro). El smart-client multi-broker (metadata/pool/grupos) llega en Fase 2.
add_library(nexus-client
  client.cpp
  producer.cpp
  consumer.cpp
  dead_letter.cpp)
add_library(nexus::client ALIAS nexus-client)

target_include_directories(nexus-client PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-client PUBLIC
  nexus::common
  nexus::io
  nexus::protocol
  nexus_options)
