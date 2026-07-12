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
  unit/common/record_codec_test.cpp
  unit/common/compression_test.cpp
  unit/common/varint_test.cpp
  unit/common/sha256_test.cpp
  unit/common/base64_test.cpp
  unit/common/task_test.cpp
  unit/common/fnv1a_test.cpp
  unit/io/file_test.cpp
  unit/io/aligned_buffer_test.cpp
  unit/io/block_reader_test.cpp
  unit/io/fake_proactor_test.cpp
  unit/io/awaitable_test.cpp
  unit/io/io_uring_backend_test.cpp
  unit/storage/index_test.cpp
  unit/storage/segment_test.cpp
  unit/storage/segment_crypto_test.cpp
  unit/storage/storage_tier_test.cpp
  unit/storage/local_storage_tier_test.cpp
  unit/storage/partition_log_test.cpp
  unit/storage/log_compactor_test.cpp
  unit/bench/latency_histogram_test.cpp
  unit/loadgen/load_schedule_test.cpp
  unit/loadgen/load_generator_test.cpp
  unit/protocol/codec_test.cpp
  unit/protocol/error_code_test.cpp
  unit/protocol/frame_test.cpp
  unit/protocol/versioning_test.cpp
  unit/protocol/messages_test.cpp
  unit/reactor/allocator_test.cpp
  unit/reactor/cross_core_test.cpp
  unit/reactor/periodic_timers_test.cpp
  unit/reactor/reactor_test.cpp
  unit/reactor/reactor_pool_test.cpp
  unit/reactor/scheduler_test.cpp
  unit/reactor/spsc_queue_test.cpp
  unit/reactor/mpmc_queue_test.cpp
  unit/reactor/cross_core_call_test.cpp
  unit/reactor/partition_router_test.cpp
  unit/broker/producer_session_test.cpp
  unit/broker/partition_test.cpp
  unit/broker/replicated_partition_test.cpp
  unit/broker/consumer_group_test.cpp
  unit/broker/group_coordinator_test.cpp
  unit/broker/topic_test.cpp
  unit/broker/topic_manager_test.cpp
  unit/broker/topic_catalog_test.cpp
  unit/broker/offset_manager_test.cpp
  unit/broker/group_catalog_test.cpp
  unit/broker/credit_window_test.cpp
  unit/broker/request_router_test.cpp
  unit/server/admin_api_test.cpp
  unit/server/admin_router_test.cpp
  unit/server/daemon_args_test.cpp
  unit/server/kafka_adapter_test.cpp
  unit/consensus/raft_state_test.cpp
  unit/ingress/rate_limiter_test.cpp
  unit/ingress/circuit_breaker_test.cpp
  unit/ingress/load_balancer_test.cpp
  unit/ingress/health_check_test.cpp
  unit/ingress/http_test.cpp
  unit/ingress/json_test.cpp
  unit/ingress/json_value_test.cpp
  unit/ingress/jwt_test.cpp
  unit/ingress/rest_gateway_test.cpp
  unit/ingress/health_monitor_test.cpp
  unit/ingress/problem_detail_test.cpp
  unit/ingress/pagination_test.cpp
  unit/ingress/tls_test.cpp
  unit/ingress/connection_state_test.cpp
  unit/ingress/proxy_test.cpp
  unit/ingress/upstream_pool_test.cpp
  unit/telemetry/metrics_test.cpp
  unit/telemetry/logging_test.cpp
  unit/telemetry/tracing_test.cpp
  unit/cli/topic_commands_test.cpp
  unit/cli/cli_options_test.cpp
  unit/cli/group_commands_test.cpp
  unit/consensus/raft_rpc_test.cpp
  unit/consensus/raft_wire_test.cpp
  unit/consensus/raft_carrier_test.cpp
  unit/consensus/deferred_message_sink_test.cpp
  unit/consensus/raft_log_test.cpp
  unit/consensus/raft_state_store_test.cpp
  unit/consensus/raft_node_test.cpp
  unit/cluster/peer_directory_test.cpp
  unit/cluster/raft_transport_test.cpp
  unit/cluster/raft_transport_loopback_test.cpp
  sim/raft_sim_test.cpp
  unit/client/dead_letter_test.cpp
  unit/kafka/codec_test.cpp
  unit/kafka/messages_test.cpp
  unit/kafka/metadata_test.cpp
  unit/kafka/produce_fetch_test.cpp
  unit/kafka/record_batch_test.cpp
  unit/kafka/gateway_test.cpp
  unit/ffi/ffi_test.cpp
  e2e/client_e2e_test.cpp
  e2e/loadgen_e2e_test.cpp
  e2e/cli_admin_e2e_test.cpp
  e2e/tls_e2e_test.cpp
  e2e/tls_server_e2e_test.cpp
  e2e/proxy_server_e2e_test.cpp)

# Tests con arnés POSIX-only: sockets BSD crudos (`::socket(AF_INET, ...)`, `sockaddr_in`, `htons`)
# o `fork()` para inyección de crash. NO portados a Winsock/CreateProcess: se EXCLUYEN en Windows
# (W3-B, ADR-0028). La funcionalidad portable que cubren se valida en Windows por `tools/wincheck`
# (runtime IOCP: File/Socket/Listener/timer) y el smoke de `nexusd` (produce/fetch por IOCP); en
# Linux corren en la puerta canónica. (`io_uring_backend_test.cpp` NO va aquí: ya se autoexcluye por
# `#ifdef NEXUS_HAVE_IOURING`.)
if(NOT WIN32)
  target_sources(nexus-tests PRIVATE
    unit/io/socket_test.cpp
    unit/wire/frame_io_test.cpp
    unit/cluster/raft_link_test.cpp
    unit/cluster/raft_receiver_test.cpp
    e2e/broker_e2e_test.cpp
    e2e/admin_http_e2e_test.cpp
    e2e/cluster_e2e_test.cpp
    crash/partition_crash_test.cpp)
endif()

# LatencyHistogram es header-only en tools/bench; el test lo incluye directamente.
# tests/ se añade al include-path para los dobles de test compartidos (support/).
target_include_directories(nexus-tests PRIVATE
  ${PROJECT_SOURCE_DIR}/tools/bench
  ${PROJECT_SOURCE_DIR}/tools/loadgen
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
  nexus::cluster
  nexus::broker
  nexus::ingress
  nexus::telemetry
  nexus::client
  nexus::loadgen
  nexus::kafka
  nexus::ffi
  nexus::server
  nexus::cli)

if(MSVC)
  # std::fopen y otras funciones estándar de la CRT disparan C4996 ("inseguras", sugiere las *_s) en
  # MSVC; son C++ estándar y la puerta canónica (Linux) no las marca. Se silencian SOLO en el target
  # de tests (W3-B, ADR-0028), que además usa utilidades de la CRT en sus arneses (p. ej. los certs
  # de prueba con std::fopen). El árbol de producción no usa estas funciones (E/S vía nexus-io).
  target_compile_definitions(nexus-tests PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

# En Windows, las DLL dependientes deben estar junto al .exe para cargarse en runtime. La suite
# enlaza `nexus::ffi`, que es una librería COMPARTIDA (nexus-ffi.dll); sin esto, ejecutar el binario
# —incluido el paso de descubrimiento de gtest— falla con 0xC0000135 (STATUS_DLL_NOT_FOUND). Copiamos
# todas las DLL de las que depende el target junto a él (patrón canónico de CMake para Windows);
# debe ir ANTES de gtest_discover_tests para que su POST_BUILD se ejecute primero. Inerte en Linux
# (W3-B, ADR-0028): en ELF las .so se resuelven por rpath/SONAME y el bloque está guardado por WIN32.
if(WIN32)
  add_custom_command(TARGET nexus-tests POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_RUNTIME_DLLS:nexus-tests> $<TARGET_FILE_DIR:nexus-tests>
    COMMAND_EXPAND_LISTS)
endif()

# Registra cada TEST(...) como un test de CTest (los descubre del binario).
gtest_discover_tests(nexus-tests)
