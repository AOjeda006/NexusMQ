# nexus-server — daemon del broker mono-nodo (Fase 1b): un reactor (io_uring), listener,
# bucle de aceptación y `serve_connection`. La librería es testeable (e2e); el ejecutable
# `nexusd` solo añade el main (parseo de args + señales).
add_library(nexus-server
  server.cpp
  connection.cpp)
add_library(nexus::server ALIAS nexus-server)

target_include_directories(nexus-server PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-server PUBLIC
  nexus::common
  nexus::io
  nexus::reactor
  nexus::wire
  nexus::broker
  nexus_options)

add_executable(nexusd main.cpp)
target_link_libraries(nexusd PRIVATE nexus::server)
