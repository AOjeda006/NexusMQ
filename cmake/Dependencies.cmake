# Adquisición de dependencias externas que no son siempre resolubles por vcpkg.
# Diseño: vcpkg.json es el manifiesto canónico (ADR-0001). Pero en entornos sin
# vcpkg (este contenedor, CI sin bootstrap) caemos a FetchContent, para que el
# árbol compile y testee igual en todas partes.

# Garantiza que GoogleTest está disponible como targets GTest::gtest / GTest::gtest_main.
# Idempotente: si ya existe el target (vcpkg ya lo trajo), no hace nada.
function(nexus_require_googletest)
  if(TARGET GTest::gtest_main)
    return()
  endif()

  # 1) Intento por find_package (lo provee vcpkg o un GTest del sistema).
  find_package(GTest CONFIG QUIET)
  if(GTest_FOUND)
    message(STATUS "GoogleTest: resuelto vía find_package (vcpkg/sistema).")
    return()
  endif()

  # 2) Fallback: descarga reproducible por FetchContent (requiere red).
  message(STATUS "GoogleTest: no encontrado; descargando con FetchContent.")
  include(FetchContent)
  FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.17.0
    GIT_SHALLOW    TRUE)
  FetchContent_MakeAvailable(googletest)
endfunction()

# Garantiza Google Benchmark como target benchmark::benchmark. Misma estrategia.
function(nexus_require_benchmark)
  if(TARGET benchmark::benchmark)
    return()
  endif()

  find_package(benchmark CONFIG QUIET)
  if(benchmark_FOUND)
    message(STATUS "Google Benchmark: resuelto vía find_package (vcpkg/sistema).")
    return()
  endif()

  message(STATUS "Google Benchmark: no encontrado; descargando con FetchContent.")
  include(FetchContent)
  # No construir las pruebas internas de benchmark (arrastrarían su propio gtest).
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.9.5
    GIT_SHALLOW    TRUE)
  FetchContent_MakeAvailable(benchmark)
endfunction()
