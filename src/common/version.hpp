/**
 * @file   version.hpp
 * @brief  Interfaz pública para consultar la versión de NexusMQ en tiempo de ejecución.
 * @ingroup common
 *
 * La cadena de versión sigue el esquema SemVer (MAJOR.MINOR.PATCH) y es
 * inyectada en tiempo de compilación por CMake mediante la macro
 * @c NEXUSMQ_VERSION, definida en el target @c nexus-common a través de
 * @c target_compile_definitions.  De este modo existe una única fuente de
 * verdad: el campo @c VERSION de la instrucción @c project() en el
 * @c CMakeLists.txt raíz.
 *
 * @par Ejemplo de uso
 * @code{.cpp}
 * #include "common/version.hpp"
 * #include <iostream>
 *
 * int main() {
 *     std::cout << "NexusMQ " << nexus::version() << '\n';
 * }
 * @endcode
 */

#pragma once

#include <string_view>

namespace nexus {

/**
 * @brief  Devuelve la versión de NexusMQ como una vista de cadena de solo lectura.
 *
 * La vista apunta a almacenamiento estático con duración de programa, por lo
 * que es seguro retenerla más allá del punto de llamada sin copiar la cadena.
 *
 * @return Vista no propietaria sobre la cadena de versión (p. ej. @c "0.1.0").
 *
 * @note   La función no lanza excepciones y el compilador emite una advertencia
 *         si el valor de retorno se descarta.
 */
[[nodiscard]] std::string_view version() noexcept;

}  // namespace nexus
