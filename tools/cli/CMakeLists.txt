# nexus-cli — CLI de administración (Fase 3): habla el REST admin del puerto de operación.
# La lógica de los subcomandos vive en una librería (nexus::cli) para poder probarla con un doble
# de AdminClient (sin red); el ejecutable solo añade el main (argv → run_cli).
add_library(nexus-cli-core
  cli.cpp
  http_admin_client.cpp
  topic_commands.cpp
  group_commands.cpp
  partition_commands.cpp
  admin_commands.cpp)
add_library(nexus::cli ALIAS nexus-cli-core)

# "cli/..." resuelve bajo tools/; "common/..."/"ingress/..." vienen de las libs enlazadas.
target_include_directories(nexus-cli-core PUBLIC ${PROJECT_SOURCE_DIR}/tools)
target_link_libraries(nexus-cli-core PUBLIC
  nexus::common
  nexus::ingress)

add_executable(nexus-cli main.cpp)
target_link_libraries(nexus-cli PRIVATE nexus::cli)
