# nexus-consensus — Raft por partición (ADR-0003): estado, RPC, log replicado, elección.
# Capa: depende de common, storage y protocol (deps del desglose §4.6).
add_library(nexus-consensus
  raft_state.cpp)
add_library(nexus::consensus ALIAS nexus-consensus)

target_include_directories(nexus-consensus PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-consensus
  PUBLIC nexus::common nexus::storage nexus::protocol nexus_options)
