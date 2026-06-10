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
    GIT_TAG        v1.15.2
    GIT_SHALLOW    TRUE)
  FetchContent_MakeAvailable(googletest)
endfunction()
