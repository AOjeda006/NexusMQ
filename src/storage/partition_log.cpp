#include "storage/partition_log.hpp"

#include <algorithm>
#include <charconv>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace nexus {
namespace {

// Extrae el offset base del nombre de un .log (`<base:020>.log`). nullopt si no encaja.
[[nodiscard]] expected<Offset> parse_base_offset(const std::filesystem::path& log_path) {
    const std::string stem = log_path.stem().string();
    Offset base = 0;
    const auto* first = stem.data();
    const auto* last = stem.data() + stem.size();
    const auto [ptr, ec] = std::from_chars(first, last, base);
    if (ec != std::errc{} || ptr != last) {
        return make_error(ErrorCode::InvalidArgument, "nombre de segmento no numérico: " + stem);
    }
    return base;
}

}  // namespace

PartitionLog::PartitionLog(std::filesystem::path dir, LogConfig cfg,
                           std::vector<std::unique_ptr<Segment>> segments, Offset log_start,
                           Offset log_end)
    : dir_(std::move(dir)),
      cfg_(cfg),
      segments_(std::move(segments)),
      log_start_offset_(log_start),
      log_end_offset_(log_end) {}

expected<PartitionLog> PartitionLog::open(std::filesystem::path dir, LogConfig cfg) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        return make_error(ErrorCode::IoError, "create_directories: " + ec.message());
    }

    // Descubre los segmentos existentes por sus ficheros .log y los ordena por offset base.
    std::vector<Offset> bases;
    std::filesystem::directory_iterator it(dir, ec);
    if (ec) {
        return make_error(ErrorCode::IoError, "directory_iterator: " + ec.message());
    }
    for (const std::filesystem::directory_iterator end; it != end; it.increment(ec)) {
        if (ec) {
            return make_error(ErrorCode::IoError, "directory_iterator: " + ec.message());
        }
        if (it->path().extension() != ".log") {
            continue;
        }
        auto base = parse_base_offset(it->path());
        if (!base) {
            continue;  // ignora ficheros .log ajenos al esquema de nombres.
        }
        bases.push_back(*base);
    }
    std::ranges::sort(bases);

    std::vector<std::unique_ptr<Segment>> segments;
    Offset log_start = 0;
    Offset log_end = 0;

    if (bases.empty()) {
        auto seg = Segment::create(dir, /*base_offset=*/0, cfg.index_interval_bytes);
        if (!seg) {
            return std::unexpected(seg.error());
        }
        segments.push_back(std::make_unique<Segment>(std::move(*seg)));
    } else {
        segments.reserve(bases.size());
        for (const Offset base : bases) {
            auto seg = Segment::open(dir, base, cfg.index_interval_bytes);
            if (!seg) {
                return std::unexpected(seg.error());
            }
            segments.push_back(std::make_unique<Segment>(std::move(*seg)));
        }
        log_start = bases.front();
        // Recupera el segmento activo (el último): valida CRC y trunca la cola torn.
        auto last_valid = segments.back()->recover();
        if (!last_valid) {
            return std::unexpected(last_valid.error());
        }
        log_end = *last_valid + 1;
    }

    return PartitionLog{std::move(dir), cfg, std::move(segments), log_start, log_end};
}

expected<Offset> PartitionLog::append(const RecordBatch& batch) {
    if (active()->size_bytes() > 0 && active()->is_full(cfg_.segment_bytes)) {
        if (const auto rolled = roll_segment(); !rolled) {
            return std::unexpected(rolled.error());
        }
    }
    // El log asigna el offset base autoritativo (no cubierto por el CRC).
    RecordBatchHeader header = batch.header();
    header.base_offset = log_end_offset_;
    const ByteSpan records = batch.records();
    const RecordBatch rebased{header, std::vector<std::byte>{records.begin(), records.end()}};

    auto last = active()->append(rebased);
    if (!last) {
        return std::unexpected(last.error());
    }
    log_end_offset_ = *last + 1;
    return *last;
}

expected<void> PartitionLog::roll_segment() {
    if (const auto sealed = active()->seal(); !sealed) {
        return sealed;
    }
    auto seg = Segment::create(dir_, log_end_offset_, cfg_.index_interval_bytes);
    if (!seg) {
        return std::unexpected(seg.error());
    }
    segments_.push_back(std::make_unique<Segment>(std::move(*seg)));
    return {};
}

}  // namespace nexus
