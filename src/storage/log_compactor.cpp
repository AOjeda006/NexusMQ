/// @file   storage/log_compactor.cpp
/// @brief  Implementación de LogCompactor (compactación por clave).
/// @ingroup storage

#include "storage/log_compactor.hpp"

#include <cstring>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/bytes.hpp"
#include "common/record.hpp"
#include "storage/partition_log.hpp"

namespace nexus {
namespace {

/// Tope de bytes por lectura al recorrer el log (4 MiB).
constexpr std::size_t kReadChunkBytes = std::size_t{4} * 1024 * 1024;

/// Clave de mapa a partir de los bytes de una clave de record (copia opaca).
std::string key_of(const std::vector<std::byte>& bytes) {
    std::string out(bytes.size(), '\0');
    if (!bytes.empty()) {
        std::memcpy(out.data(), bytes.data(), bytes.size());
    }
    return out;
}

}  // namespace

std::vector<Record> LogCompactor::compact(std::span<const Record> records,
                                          CompactionStats* stats) const {
    // Pasada 1: índice de la última aparición de cada clave (orden = orden de offset).
    std::unordered_map<std::string, std::size_t> last_index;
    for (std::size_t i = 0; i < records.size(); ++i) {
        const std::optional<std::vector<std::byte>>& key = records[i].key;
        if (key.has_value()) {
            last_index[key_of(*key)] = i;
        }
    }

    // Pasada 2: emite los records sin clave y la última aparición de cada clave (filtrando
    // tombstones salvo que se pidan retener).
    CompactionStats counters;
    counters.records_in = records.size();
    std::vector<Record> out;
    out.reserve(records.size());
    for (std::size_t i = 0; i < records.size(); ++i) {
        const Record& rec = records[i];
        const std::optional<std::vector<std::byte>>& key = rec.key;
        if (!key.has_value()) {
            out.push_back(rec);  // los records sin clave se conservan siempre.
            ++counters.records_kept;
            continue;
        }
        if (last_index[key_of(*key)] != i) {
            ++counters.records_superseded;  // hay una aparición posterior de esta clave.
            continue;
        }
        const bool is_tombstone = !rec.value.has_value();
        if (is_tombstone && !retain_tombstones_) {
            ++counters.tombstones_dropped;  // la clave desaparece del log compactado.
            continue;
        }
        out.push_back(rec);
        ++counters.records_kept;
    }
    if (stats != nullptr) {
        *stats = counters;
    }
    return out;
}

expected<std::vector<Record>> LogCompactor::compact_log(const PartitionLog& log,
                                                        CompactionStats* stats) const {
    std::vector<Record> all;
    Offset pos = log.log_start_offset();
    const Offset end = log.log_end_offset();
    while (pos < end) {
        const expected<FetchResult> fetched = log.read(pos, kReadChunkBytes);
        if (!fetched) {
            return std::unexpected(fetched.error());
        }
        ByteSpan remaining = fetched->batches.as_span();
        if (remaining.empty()) {
            break;  // nada más legible (defensa contra bucle infinito).
        }
        while (!remaining.empty()) {
            const expected<RecordBatchView> view = RecordBatch::peek(remaining);
            if (!view || view->encoded_size > remaining.size()) {
                break;
            }
            const expected<RecordBatch> batch =
                RecordBatch::decode(remaining.subspan(0, view->encoded_size));
            if (!batch) {
                return std::unexpected(batch.error());
            }
            expected<std::vector<Record>> records = decode_records(*batch);
            if (!records) {
                return std::unexpected(records.error());
            }
            for (Record& rec : *records) {
                all.push_back(std::move(rec));
            }
            remaining = remaining.subspan(view->encoded_size);
        }
        if (fetched->next_offset <= pos) {
            break;  // sin progreso: evita un bucle infinito ante datos inconsistentes.
        }
        pos = fetched->next_offset;
    }
    return compact(all, stats);
}

}  // namespace nexus
