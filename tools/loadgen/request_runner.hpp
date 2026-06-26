/// @file   tools/loadgen/request_runner.hpp
/// @brief  RequestRunner: una petición bloqueante que el generador de carga repite y cronometra.
/// @ingroup loadgen

#pragma once

#include <functional>
#include <memory>

#include "common/error.hpp"

namespace nexus::loadgen {

/// @brief Ejecuta **una** petición bloqueante (produce/fetch) contra el broker. Afinidad:
///   REACTOR-LOCAL (cada runner posee su propia conexión; no se comparte entre hilos).
/// @details Abstracción (DIP) que desacopla el motor del generador del tipo de carga y del
///   transporte: el motor solo sabe «dispara una petición y dime si fue bien», cronometrando él la
///   latencia. Producción inyecta un runner sobre el `Client` nativo; los tests inyectan un doble
///   determinista. Un runner por conexión (un hilo): así el bucle caliente no comparte estado.
class RequestRunner {
public:
    RequestRunner() = default;
    RequestRunner(const RequestRunner&) = delete;
    RequestRunner& operator=(const RequestRunner&) = delete;
    RequestRunner(RequestRunner&&) = delete;
    RequestRunner& operator=(RequestRunner&&) = delete;
    virtual ~RequestRunner() = default;

    /// @brief Ejecuta una petición y espera su respuesta. @return éxito, o el error traducido.
    [[nodiscard]] virtual expected<void> run_once() = 0;
};

/// @brief Crea el runner de la conexión @p worker_id (cada uno abre su propio socket). El motor la
///   invoca una vez por hilo antes del bucle. @return el runner o el error de conexión/arranque.
using RunnerFactory = std::function<expected<std::unique_ptr<RequestRunner>>(int worker_id)>;

}  // namespace nexus::loadgen
