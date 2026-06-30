# NexusMQ — árbol de build raíz (una sola "solución" CMake, multiplataforma).
# Diseño: docs/tecnica/ (arquitectura y mapa de módulos).

cmake_minimum_required(VERSION 3.25)

# Integración OPCIONAL con vcpkg (modo manifest). Si VCPKG_ROOT está definido y no
# se ha fijado ya un toolchain, usamos el de vcpkg (manifiesto canónico, ADR-0001);
# si no existe, los targets que necesiten dependencias caen a un fallback FetchContent
# (ver tests/, tools/bench/). Así el árbol configura igual con vcpkg (máquina del autor,
# VS+WSL2) que sin él (este contenedor, CI). Debe ir ANTES de project().
if(DEFINED ENV{VCPKG_ROOT} AND NOT DEFINED CMAKE_TOOLCHAIN_FILE)
  set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
      CACHE STRING "Toolchain de vcpkg (modo manifest)")
endif()

project(NexusMQ
  VERSION 0.1.0
  DESCRIPTION "Broker de mensajería distribuido de alto rendimiento (C++23)"
  LANGUAGES CXX)

# Prohíbe la build in-source: ensucia el árbol y mezcla artefactos con el código.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
  message(FATAL_ERROR "Build in-source prohibida. Usa un preset: cmake --preset linux-gcc")
endif()

option(NEXUS_SANITIZERS "Compila con AddressSanitizer + UBSan (build de pruebas)" OFF)
option(NEXUS_TSAN "Compila con ThreadSanitizer (valida las colas lock-free)" OFF)

# Exporta compile_commands.json (lo consumen clang-tidy, clangd y los editores).
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Hilos POSIX (el reactor thread-per-core de Fase 1b: std::thread + afinidad de CPU).
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Requisitos comunes de compilación, propagados a TODOS los targets vía un target
# INTERFACE (CMake moderno basado en targets). C++23 (necesario para std::expected,
# modelo de errores del núcleo, ADR-0009) y "avisos como errores".
add_library(nexus_options INTERFACE)
add_library(nexus::options ALIAS nexus_options)
target_compile_features(nexus_options INTERFACE cxx_std_23)

if(MSVC)
  # /W4 /WX es la puerta en MSVC/clang-cl (Windows). Dos avisos de /W4 NO los exige la puerta
  # canónica de Linux (-Wall -Wextra -Wpedantic) y marcan código intencional/idiomático, así que se
  # desactivan aquí (decisión de proyecto, espejo de los checks apagados en .clang-tidy):
  #   C4324: «struct rellenada por el especificador de alineación». El `alignas(kCacheLineSize)` de
  #          las colas SPSC/MPMC es DELIBERADO (evita false sharing productor/consumidor); el padding
  #          es el objetivo, no un defecto.
  #   C4456: «la declaración oculta una local anterior». Salta en cadenas if/else-if con variables de
  #          condición reutilizadas (p. ej. `args`/`reply` en raft_carrier); cada `else if` anida su
  #          ámbito. Es idiomático y Linux no lo exige (-Wshadow no está en -Wall/-Wextra).
  target_compile_options(nexus_options INTERFACE /W4 /WX /permissive- /wd4324 /wd4456)
else()
  target_compile_options(nexus_options INTERFACE -Wall -Wextra -Wpedantic -Werror)
endif()

# Sanitizers bajo demanda para la build de pruebas. ASan/UBSan y TSan son **mutuamente
# excluyentes** (no se pueden combinar en el mismo binario); cada uno tiene su preset.
# TSan valida las colas lock-free de Fase 1b (SPSC/MPMC) frente a carreras de datos.
if(NEXUS_SANITIZERS AND NEXUS_TSAN)
  message(FATAL_ERROR "NEXUS_SANITIZERS y NEXUS_TSAN son incompatibles: usa un preset u otro.")
endif()
if(NEXUS_SANITIZERS AND NOT MSVC)
  target_compile_options(nexus_options INTERFACE
    -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(nexus_options INTERFACE -fsanitize=address,undefined)
endif()
if(NEXUS_TSAN AND NOT MSVC)
  target_compile_options(nexus_options INTERFACE
    -fsanitize=thread -fno-omit-frame-pointer)
  target_link_options(nexus_options INTERFACE -fsanitize=thread)
endif()

enable_testing()

# Tests y benchmarks son dev-only y arrastran GoogleTest/Benchmark (FetchContent). Se pueden
# desactivar para builds de release/empaquetado (p. ej. la imagen Docker, que solo necesita
# nexusd + nexus-cli y no quiere depender de la red en `configure`).
option(NEXUS_BUILD_TESTS "Compila la suite de tests y los benchmarks (GoogleTest/Benchmark)" ON)

# Los subdirectorios se incorporan a medida que existen: el árbol crece por hito
# (M1 cimientos → M2 common/storage → … ). Mientras una carpeta no tenga su
# CMakeLists, se omite sin romper la configuración. La biblioteca (src), el CLI y el generador de
# carga (nexus-loadgen) van siempre: son herramientas de entrega que solo dependen del núcleo (sin
# FetchContent). Tests y benchmarks (GoogleTest/Benchmark) solo con NEXUS_BUILD_TESTS.
set(_nexus_subdirs src tools/cli tools/wincheck tools/loadgen)
if(NEXUS_BUILD_TESTS)
  list(APPEND _nexus_subdirs tools/bench tests)
endif()
foreach(_dir IN LISTS _nexus_subdirs)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${_dir}/CMakeLists.txt")
    add_subdirectory(${_dir})
  endif()
endforeach()

message(STATUS "NexusMQ ${PROJECT_VERSION} · C++23 · asan=${NEXUS_SANITIZERS} · tsan=${NEXUS_TSAN}")
