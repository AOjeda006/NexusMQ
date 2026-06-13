# nexus-reactor — reactor thread-per-core: corrutinas (task), scheduler, colas SPSC/MPMC,
# allocator de arena, cross-core mailbox, reactor y pool. Fase 1b. STATIC desde R6b (los
# .cpp del reactor); el resto (task, scheduler, colas, allocator) sigue siendo header-only.
add_library(nexus-reactor
  cross_core.cpp
  reactor.cpp)
add_library(nexus::reactor ALIAS nexus-reactor)

target_include_directories(nexus-reactor PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-reactor PUBLIC nexus::common nexus::io nexus_options)
