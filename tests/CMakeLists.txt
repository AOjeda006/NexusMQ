# Suite de pruebas (GoogleTest). Crece con la matriz de testing (§8.1).

include(${PROJECT_SOURCE_DIR}/cmake/Dependencies.cmake)
nexus_require_googletest()

include(GoogleTest)  # provee gtest_discover_tests()

add_executable(nexus-tests
  unit/common/version_test.cpp
  unit/common/types_test.cpp
  unit/common/crc32c_test.cpp
  unit/common/bytes_test.cpp
  unit/common/error_test.cpp
  unit/common/record_test.cpp
  unit/common/varint_test.cpp
  unit/common/task_test.cpp
  unit/io/file_test.cpp
  unit/io/socket_test.cpp
  unit/io/fake_proactor_test.cpp
  unit/io/awaitable_test.cpp
  unit/io/io_uring_backend_test.cpp
  unit/storage/index_test.cpp
  unit/storage/segment_test.cpp
  unit/storage/partition_log_test.cpp
  unit/bench/latency_histogram_test.cpp
  unit/protocol/codec_test.cpp
  unit/protocol/error_code_test.cpp
  unit/protocol/frame_test.cpp
  unit/protocol/versioning_test.cpp
  unit/protocol/messages_test.cpp
  unit/reactor/allocator_test.cpp
  unit/reactor/cross_core_test.cpp
  unit/reactor/reactor_test.cpp
  unit/reactor/reactor_pool_test.cpp
  unit/reactor/scheduler_test.cpp
  unit/reactor/spsc_queue_test.cpp
  unit/reactor/mpmc_queue_test.cpp
  unit/wire/frame_io_test.cpp
  unit/broker/producer_session_test.cpp
  unit/broker/partition_test.cpp
  unit/broker/topic_test.cpp
  unit/broker/topic_manager_test.cpp
  unit/broker/offset_manager_test.cpp
  unit/broker/credit_window_test.cpp
  unit/broker/request_router_test.cpp
  unit/consensus/raft_state_test.cpp
  unit/consensus/raft_rpc_test.cpp
  e2e/broker_e2e_test.cpp
  e2e/client_e2e_test.cpp
  crash/partition_crash_test.cpp)

# LatencyHistogram es header-only en tools/bench; el test lo incluye directamente.
# tests/ se añade al include-path para los dobles de test compartidos (support/).
target_include_directories(nexus-tests PRIVATE
  ${PROJECT_SOURCE_DIR}/tools/bench
  ${PROJECT_SOURCE_DIR}/tests)

# nexus::common aporta C++20, los avisos-como-errores (vía nexus_options, PUBLIC)
# y la cabecera; gtest_main aporta un main() que corre todos los TEST(...).
target_link_libraries(nexus-tests PRIVATE
  nexus::common
  GTest::gtest_main
  nexus::io
  nexus::storage
  nexus::protocol
  nexus::wire
  nexus::reactor
  nexus::consensus
  nexus::broker
  nexus::client
  nexus::server)

# Registra cada TEST(...) como un test de CTest (los descubre del binario).
gtest_discover_tests(nexus-tests)
