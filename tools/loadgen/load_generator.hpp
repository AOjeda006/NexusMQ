/// @file   tools/loadgen/load_generator.hpp
/// @brief  LoadGenerator: motor de carga open-loop multi-conexión sobre la red.
/// @ingroup loadgen

#pragma once

#include "common/error.hpp"
#include "loadgen_config.hpp"
#include "loadgen_report.hpp"
#include "request_runner.hpp"

namespace nexus::loadgen {

/// @brief Genera carga **open-loop** contra el broker y mide la latencia sin *coordinated
///   omission*. Afinidad: CROSS-CORE (orquesta varios hilos; cada conexión es REACTOR-LOCAL).
/// @details Reparte `config.op_count` (+ calentamiento) peticiones entre `config.connections`
///   hilos (uno por conexión, shared-nothing). Cada hilo, para cada petición que le toca, espera
///   hasta su **instante previsto** (`OpenLoopSchedule`) antes de dispararla — así la tasa de
///   llegada es independiente de las respuestas (open-loop) — y registra la latencia contra ese
///   instante previsto en su **propio** `LatencyHistogram`. Al terminar, fusiona los histogramas y
///   calcula percentiles y throughput real. El reparto por-hilo evita estado compartido en el
///   bucle caliente (normativa de concurrencia); el `RunnerFactory` inyectado abstrae el transporte
///   (DIP), de modo que los tests usan un runner determinista sin red.
/// @invariant `run()` no muta `config_` ni `factory_`; es reejecutable.
class LoadGenerator {
public:
    /// @brief Construye el generador con @p config y la fábrica de runners @p factory (un runner
    ///   por conexión). @p factory debe ser invocable de forma concurrente desde varios hilos.
    LoadGenerator(LoadGenConfig config, RunnerFactory factory);

    /// @brief Ejecuta la campaña: lanza las conexiones, mide y agrega.
    /// @return el informe agregado, o el error de arranque (p. ej. fallo al crear una conexión).
    [[nodiscard]] expected<LoadGenReport> run();

private:
    LoadGenConfig config_;
    RunnerFactory factory_;
};

}  // namespace nexus::loadgen
