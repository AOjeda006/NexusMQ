/// @file   storage/log_compactor.hpp
/// @brief  LogCompactor: compactación por clave (conserva el último record por clave + tombstones).
/// @ingroup storage

#pragma once

#include <cstddef>
#include <span>
#include <vector>

#include "common/error.hpp"
#include "common/record_codec.hpp"

namespace nexus {

class PartitionLog;

/// @brief Métricas de una pasada de compactación. Afinidad: INMUTABLE.
struct CompactionStats {
    std::size_t records_in = 0;          ///< Records examinados.
    std::size_t records_kept = 0;        ///< Records emitidos (último por clave + sin clave).
    std::size_t records_superseded = 0;  ///< Records con clave reemplazados por uno posterior.
    std::size_t tombstones_dropped = 0;  ///< Tombstones descartados (si no se retienen).
};

/// @brief Compactación por clave estilo Kafka. Afinidad: REACTOR-LOCAL (sin estado mutable).
/// @details Conserva, por clave, **solo el record más reciente** (la última aparición en orden de
///   offset); los anteriores se descartan. Un **tombstone** (record con `value` nulo) marca la
///   clave para borrado: supera a los anteriores y, salvo que se pidan retener, se descarta él
///   también (la clave desaparece del log compactado). Los records **sin clave** se conservan
///   siempre (la compactación solo colapsa por clave). Los offsets originales se preservan (pueden
///   quedar huecos), igual que en Kafka.
class LogCompactor {
public:
    /// @param retain_tombstones Si es `true`, los tombstones se conservan (ventana de borrado);
    ///   si es `false` (por defecto), se descartan tras colapsar su clave.
    explicit LogCompactor(bool retain_tombstones = false) noexcept
        : retain_tombstones_(retain_tombstones) {}

    /// @brief Compacta @p records (en orden de offset). @p stats recibe métricas si no es nulo.
    /// @return Los records conservados, en orden, con sus offsets originales.
    [[nodiscard]] std::vector<Record> compact(std::span<const Record> records,
                                              CompactionStats* stats = nullptr) const;

    /// @brief Lee **todos** los records de @p log y los compacta (offsets absolutos preservados).
    /// @return Los records compactados, o un error de E/S / `Corrupt` si el log está dañado.
    [[nodiscard]] expected<std::vector<Record>> compact_log(const PartitionLog& log,
                                                            CompactionStats* stats = nullptr) const;

private:
    bool retain_tombstones_;
};

}  // namespace nexus
