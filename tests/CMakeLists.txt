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
  unit/io/file_test.cpp)

# nexus::common aporta C++20, los avisos-como-errores (vía nexus_options, PUBLIC)
# y la cabecera; gtest_main aporta un main() que corre todos los TEST(...).
target_link_libraries(nexus-tests PRIVATE
  nexus::common
  GTest::gtest_main
  nexus::io)

# Registra cada TEST(...) como un test de CTest (los descubre del binario).
gtest_discover_tests(nexus-tests)
