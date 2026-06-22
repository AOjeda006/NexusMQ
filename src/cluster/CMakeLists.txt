# nexus-cluster — plano inter-nodo (ADR-0025): directorio de peers y transporte de Raft.
# Capa: resuelve y conecta nodos del clúster; compone common + io + consensus + wire.
add_library(nexus-cluster
  peer_directory.cpp
  raft_link.cpp)
add_library(nexus::cluster ALIAS nexus-cluster)

target_include_directories(nexus-cluster PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-cluster
  PUBLIC nexus::common nexus::io nexus::consensus nexus::protocol nexus_options)
