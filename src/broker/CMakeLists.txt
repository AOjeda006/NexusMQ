# nexus-broker — núcleo del broker (mono-nodo en Fase 1b; Raft en Fase 2): topics,
# particiones, idempotencia, offsets, grupos, backpressure. STATIC desde Partition
# (primera unidad .cpp); irá ganando dependencias (reactor, protocol, wire) por hito.
add_library(nexus-broker
  partition.cpp
  replicated_partition.cpp
  consumer_group.cpp
  group_coordinator.cpp
  topic_manager.cpp
  topic_catalog.cpp
  topic_cluster.cpp
  offset_manager.cpp
  group_catalog.cpp
  transaction_coordinator.cpp
  partition_txn_index.cpp
  request_router.cpp)
add_library(nexus::broker ALIAS nexus-broker)

target_include_directories(nexus-broker PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-broker PUBLIC
  nexus::common
  nexus::storage
  nexus::protocol
  nexus::consensus
  nexus::reactor
  nexus::telemetry
  nexus_options)
