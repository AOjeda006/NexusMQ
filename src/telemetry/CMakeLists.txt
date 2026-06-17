# nexus-telemetry — observabilidad (ADR-0017): registro de métricas con exposición Prometheus y,
# más adelante, logger JSON estructurado. Capa baja: depende solo de nexus-common, de modo que
# cualquier capa (broker, reactor, ingress, server) pueda registrar o exponer sin ciclos.
add_library(nexus-telemetry
  metrics.cpp
  logging.cpp)
add_library(nexus::telemetry ALIAS nexus-telemetry)

target_include_directories(nexus-telemetry PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-telemetry PUBLIC nexus::common nexus_options)
