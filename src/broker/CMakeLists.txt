# nexus-broker — núcleo del broker (mono-nodo en Fase 1b; Raft en Fase 2): topics,
# particiones, idempotencia, offsets, grupos, backpressure. INTERFACE por ahora
# (ProducerSession es header-only); pasará a STATIC al llegar la primera unidad .cpp.
add_library(nexus-broker INTERFACE)
add_library(nexus::broker ALIAS nexus-broker)

target_include_directories(nexus-broker INTERFACE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-broker INTERFACE nexus::common nexus_options)
