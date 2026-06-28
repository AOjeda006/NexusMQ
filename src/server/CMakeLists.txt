# nexus-server — daemon del broker mono-nodo (Fase 1b): un reactor (io_uring), listener,
# bucle de aceptación y `serve_connection`. La librería es testeable (e2e); el ejecutable
# `nexusd` solo añade el main (parseo de args + señales).
add_library(nexus-server
  server.cpp
  kafka_adapter.cpp
  admin_api.cpp
  admin_router.cpp
  admin_http.cpp)
add_library(nexus::server ALIAS nexus-server)

target_include_directories(nexus-server PUBLIC ${PROJECT_SOURCE_DIR}/src)
# nexus::ingress aporta el puerto AdminService (ADR-0018), que AdminApi implementa, y la pasarela
# REST/health; nexus::telemetry, el registro de métricas que expone /metrics (ADR-0017).
target_link_libraries(nexus-server PUBLIC
  nexus::common
  nexus::io
  nexus::reactor
  nexus::wire
  nexus::broker
  nexus::cluster
  nexus::ingress
  nexus::kafka
  nexus::telemetry
  nexus_options)

add_executable(nexusd main.cpp)
target_link_libraries(nexusd PRIVATE nexus::server)
