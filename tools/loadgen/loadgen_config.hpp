/// @file   tools/loadgen/loadgen_config.hpp
/// @brief  LoadGenConfig: parámetros del generador de carga open-loop sobre la red.
/// @ingroup loadgen

#pragma once

#include <cstddef>
#include <cstdint>

namespace nexus::loadgen {

/// @brief Parámetros de una campaña del generador de carga open-loop. Afinidad: INMUTABLE.
/// @details El generador ofrece `target_rate` peticiones/segundo (0 = a máxima velocidad,
///   closed-loop) repartidas entre `connections` conexiones (un hilo y un socket por conexión:
///   el `Client` no es thread-safe, shared-nothing). `warmup_ops` peticiones iniciales se envían
///   pero **no** se registran (descartan el arranque en frío); luego se miden `op_count`.
struct LoadGenConfig {
    std::size_t op_count = 100'000;   ///< Peticiones medidas (tras el calentamiento).
    std::size_t warmup_ops = 10'000;  ///< Peticiones de calentamiento (se envían pero no se miden).
    double target_rate =
        0.0;              ///< Tasa objetivo agregada (req/s); `0` = sin ritmo (máx. throughput).
    int connections = 1;  ///< Conexiones concurrentes (hilos), cada una con su propio socket.
    std::size_t payload_size = 256;  ///< Bytes de payload por petición produce.
};

}  // namespace nexus::loadgen
