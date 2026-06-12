# nexus-bench — benchmark del motor de log: append (throughput + percentiles), impacto de
# fsync (§8.2). LatencyHistogram + BenchConfig (header-only) sobre el motor de nexus::storage.
add_executable(nexus-bench
  main.cpp)

# El propio directorio en el include path: main.cpp incluye "latency_histogram.hpp" / "bench_config.hpp".
target_include_directories(nexus-bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(nexus-bench PRIVATE
  nexus::common
  nexus::storage)
