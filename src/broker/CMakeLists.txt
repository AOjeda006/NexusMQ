# nexus-broker — núcleo del broker (mono-nodo en Fase 1b; Raft en Fase 2): topics,
# particiones, idempotencia, offsets, grupos, backpressure. STATIC desde Partition
# (primera unidad .cpp); irá ganando dependencias (reactor, protocol, wire) por hito.
add_library(nexus-broker
  partition.cpp
  topic_manager.cpp
  offset_manager.cpp
  request_router.cpp)
add_library(nexus::broker ALIAS nexus-broker)

target_include_directories(nexus-broker PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-broker PUBLIC
  nexus::common
  nexus::storage
  nexus::protocol
  nexus_options)
