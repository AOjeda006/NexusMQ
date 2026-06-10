# nexus-bench — generador de carga open-loop + microbenchmarks (§8.2).
# M1: esqueleto que solo valida que el arnés enlaza y corre.

include(${PROJECT_SOURCE_DIR}/cmake/Dependencies.cmake)
nexus_require_benchmark()

add_executable(nexus-bench
  main.cpp)

target_link_libraries(nexus-bench PRIVATE
  nexus::common
  benchmark::benchmark)
