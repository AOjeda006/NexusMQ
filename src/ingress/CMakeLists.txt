# nexus-ingress — capa de ingress (ADR-0006): TLS, rate-limit, circuit-breaker, balanceo,
# health-checks y gateway REST de administración. Fase 3. Arranca como INTERFACE (TokenBucket es
# header-only); pasará a STATIC con la primera unidad .cpp.
add_library(nexus-ingress INTERFACE)
add_library(nexus::ingress ALIAS nexus-ingress)

target_include_directories(nexus-ingress INTERFACE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-ingress INTERFACE nexus::common nexus_options)
