/// @file   tools/bench/bench_config.hpp
/// @brief  BenchConfig: parámetros del benchmark del motor de log.
/// @ingroup bench

#pragma once

#include <cstddef>
#include <cstdint>

#include "storage/log_config.hpp"

namespace nexus::bench {

/// @brief Parámetros de un benchmark de `append` sobre el motor de log. Afinidad: INMUTABLE.
struct BenchConfig {
    std::size_t op_count = 100000;                 ///< Operaciones medidas.
    std::size_t warmup_ops = 10000;                ///< Operaciones de calentamiento (se descartan).
    std::size_t payload_size = 256;                ///< Bytes de payload por batch.
    std::int32_t batch_records = 1;                ///< Records por batch.
    FsyncPolicy fsync_policy = FsyncPolicy::None;  ///< Política de durabilidad a medir.
};

}  // namespace nexus::bench
