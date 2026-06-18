# nexus-ingress — capa de ingress (ADR-0006): TLS, rate-limit, circuit-breaker, balanceo,
# health-checks y gateway REST de administración. Fase 3. STATIC desde CircuitBreaker (primera
# unidad .cpp); irá ganando dependencias (io, protocol, wire) por hito.
add_library(nexus-ingress
  circuit_breaker.cpp
  load_balancer.cpp
  http.cpp
  json.cpp
  json_value.cpp
  problem_detail.cpp
  pagination.cpp)
add_library(nexus::ingress ALIAS nexus-ingress)

target_include_directories(nexus-ingress PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-ingress PUBLIC nexus::common nexus_options)
