# nexus-ingress — capa de ingress (ADR-0006): TLS, rate-limit, circuit-breaker, balanceo,
# health-checks y gateway REST de administración. Fase 3. STATIC desde CircuitBreaker (primera
# unidad .cpp); irá ganando dependencias (io, protocol, wire) por hito.
add_library(nexus-ingress
  circuit_breaker.cpp
  load_balancer.cpp
  http.cpp
  json.cpp
  json_value.cpp
  jwt.cpp
  rest_gateway.cpp
  health_monitor.cpp
  problem_detail.cpp
  pagination.cpp)
add_library(nexus::ingress ALIAS nexus-ingress)

target_include_directories(nexus-ingress PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-ingress PUBLIC nexus::common nexus_options)

# TLS (OpenSSL): opcional. Si OpenSSL está disponible, compila tls.cpp (TlsContext/TlsConnection,
# TLS 1.3 + mTLS) y define NEXUS_HAVE_OPENSSL para que el plano TLS y sus tests se activen; si no,
# el plano TLS se omite (el broker arranca en claro y los tests TLS hacen GTEST_SKIP). El TLS habla
# E/S asíncrona por el Proactor, de ahí la dependencia de nexus::io.
find_package(OpenSSL)
if(OpenSSL_FOUND)
  target_sources(nexus-ingress PRIVATE tls.cpp)
  target_compile_definitions(nexus-ingress PUBLIC NEXUS_HAVE_OPENSSL)
  target_link_libraries(nexus-ingress PUBLIC nexus::io OpenSSL::SSL OpenSSL::Crypto)
endif()
