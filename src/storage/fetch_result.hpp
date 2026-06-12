/// @file   storage/fetch_result.hpp
/// @brief  FetchResult: bytes devueltos por una lectura del log.
/// @ingroup storage

#pragma once

#include "common/bytes.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Resultado de una lectura (`read`/`fetch`) del log. Afinidad: REACTOR-LOCAL.
/// @details 'batches' contiene cero o más `RecordBatch` **completos** y consecutivos en
///   formato de disco (el consumidor los decodifica y filtra por offset); 'next_offset' es
///   el offset por el que continuar la siguiente lectura. El 'high_watermark' (frontera
///   visible bajo replicación) se añadirá en la capa Partition cuando exista (Fase 1b/2).
struct FetchResult {
    Buffer batches;          ///< Batches completos en bytes de disco.
    Offset next_offset = 0;  ///< Offset por el que continuar.
};

}  // namespace nexus
