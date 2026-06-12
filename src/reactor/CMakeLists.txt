# nexus-reactor — reactor thread-per-core: corrutinas (task), scheduler, colas SPSC/MPMC,
# allocator de arena, pool. Fase 1b. Por ahora header-only (task<T>); pasa a STATIC cuando
# entren los .cpp (scheduler, cross-core, allocator, reactor).
add_library(nexus-reactor INTERFACE)
add_library(nexus::reactor ALIAS nexus-reactor)

target_include_directories(nexus-reactor INTERFACE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-reactor INTERFACE nexus::common nexus_options)
