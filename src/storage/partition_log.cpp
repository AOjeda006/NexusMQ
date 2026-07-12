#include "storage/partition_log.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <format>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "storage/storage_tier.hpp"

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

// Ruta de un fichero de segmento (`<base:020><ext>`) dentro del directorio de la partición.
[[nodiscard]] std::filesystem::path seg_path(const std::filesystem::path& dir, Offset base,
                                             std::string_view ext) {
    return dir / std::format("{:020d}{}", base, ext);
}

// Clave de objeto del tier (ADR-0032) para el segmento de base @p base y pieza @p kind.
[[nodiscard]] TierObjectKey tier_key(const LogConfig& cfg, Offset base, SegmentFileKind kind) {
    return TierObjectKey{.topic = cfg.tier_topic,
                         .partition = cfg.tier_partition,
                         .base_offset = base,
                         .kind = kind};
}

// Reconstruye el prefijo **frío** (tier) al abrir: lista el tier (autoridad) y descarta las bases
// que ya existen localmente (offload confirmado pero sin reclamar por un crash: el local manda).
// Solo cuando hay segmentos locales (el reinicio normal conserva al menos el activo); si el árbol
// local está vacío, ignora el tier (recuperación de disco perdido queda fuera de alcance).
[[nodiscard]] expected<std::vector<Offset>> list_cold_prefix(
    const LogConfig& cfg, const std::vector<std::unique_ptr<Segment>>& segments) {
    std::vector<Offset> tiered;
    if (cfg.tier == nullptr || segments.empty()) {
        return tiered;
    }
    auto listed = cfg.tier->list_segment_bases(cfg.tier_topic, cfg.tier_partition);
    if (!listed) {
        return std::unexpected(listed.error());
    }
    for (const Offset base : *listed) {
        const bool local = std::ranges::any_of(segments, [base](const std::unique_ptr<Segment>& s) {
            return s->base_offset() == base;
        });
        if (!local) {
            tiered.push_back(base);
        }
    }
    std::ranges::sort(tiered);
    return tiered;
}

}  // namespace

PartitionLog::PartitionLog(std::filesystem::path dir, LogConfig cfg,
                           std::vector<std::unique_ptr<Segment>> segments,
                           std::vector<Offset> tiered_bases, Offset log_start, Offset log_end)
    : dir_(std::move(dir)),
      cfg_(std::move(cfg)),
      segments_(std::move(segments)),
      tiered_bases_(std::move(tiered_bases)),
      log_start_offset_(log_start),
      log_end_offset_(log_end),
      recovery_point_(log_end) {}  // lo abierto/recuperado ya está en disco.

expected<PartitionLog> PartitionLog::open(std::filesystem::path dir, const LogConfig& cfg) {
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

    const EncryptionKey* key = cfg.encryption_key.get();
    if (bases.empty()) {
        auto seg = Segment::create(dir, /*base_offset=*/0, cfg.index_interval_bytes, key);
        if (!seg) {
            return std::unexpected(seg.error());
        }
        segments.push_back(std::make_unique<Segment>(std::move(*seg)));
    } else {
        segments.reserve(bases.size());
        for (const Offset base : bases) {
            auto seg = Segment::open(dir, base, cfg.index_interval_bytes, key);
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

    // Reconstruye el prefijo **frío** (tier, ADR-0032): el tier es la autoridad de qué segmentos se
    // descargaron. El prefijo frío extiende `log_start` por debajo del primer segmento local.
    auto tiered_bases = list_cold_prefix(cfg, segments);
    if (!tiered_bases) {
        return std::unexpected(tiered_bases.error());
    }
    if (!tiered_bases->empty()) {
        log_start = std::min(log_start, tiered_bases->front());
    }

    return PartitionLog{std::move(dir),           cfg,       std::move(segments),
                        std::move(*tiered_bases), log_start, log_end};
}

expected<Offset> PartitionLog::append(const RecordBatch& batch) {
    if (!active()->is_empty() && active()->is_full(cfg_.segment_bytes)) {
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
    if (const auto synced = maybe_sync(rebased.encoded_size()); !synced) {
        return std::unexpected(synced.error());
    }
    return *last;
}

expected<void> PartitionLog::truncate_to(Offset offset) {
    if (offset == log_end_offset_) {
        return {};  // nada que truncar.
    }
    if (offset > log_end_offset_) {
        return make_error(ErrorCode::OutOfRange, "truncate_to por encima de log_end_offset");
    }
    if (offset < log_start_offset_) {
        return make_error(ErrorCode::OutOfRange, "truncate_to por debajo de log_start_offset");
    }
    // Nunca dentro del prefijo frío: solo se descargan segmentos sellados (historia comprometida),
    // y Raft no trunca historia comprometida. Guarda honesta ante un uso imprevisto.
    if (!tiered_bases_.empty() && offset < first_local_base()) {
        return make_error(ErrorCode::Unsupported,
                          "truncate_to dentro de datos ya descargados al tier");
    }
    // Segmento que contiene `offset`: el último con base_offset <= offset.
    const auto upper = std::ranges::upper_bound(
        segments_, offset, std::ranges::less{},
        [](const std::unique_ptr<Segment>& seg) { return seg->base_offset(); });
    const auto target = std::prev(upper);

    // Borra los segmentos posteriores al objetivo (cierra fds al borrar del vector, luego
    // ficheros).
    std::vector<Offset> dropped;
    for (auto seg = std::next(target); seg != segments_.end(); ++seg) {
        dropped.push_back((*seg)->base_offset());
    }
    segments_.erase(std::next(target), segments_.end());
    for (const Offset base : dropped) {
        std::error_code ec;
        std::filesystem::remove(seg_path(dir_, base, ".log"), ec);
        std::filesystem::remove(seg_path(dir_, base, ".index"), ec);
    }
    // Trunca dentro del segmento objetivo, que queda como activo.
    if (const auto truncated = segments_.back()->truncate_to(offset); !truncated) {
        return std::unexpected(truncated.error());
    }
    log_end_offset_ = offset;
    recovery_point_ = std::min(recovery_point_, offset);
    bytes_since_sync_ = 0;
    return {};
}

expected<void> PartitionLog::truncate_prefix_to(Offset offset) {
    if (offset <= log_start_offset_) {
        return {};  // nada que recortar (idempotente).
    }
    if (offset > log_end_offset_) {
        return make_error(ErrorCode::OutOfRange, "truncate_prefix_to por encima de log_end_offset");
    }
    // Primero el prefijo **frío** (tier): borra del tier los segmentos cuyo rango entero queda por
    // debajo de `offset` (el sucesor —el siguiente frío o el primer local— empieza en <= offset).
    while (!tiered_bases_.empty()) {
        const Offset successor = tiered_bases_.size() > 1 ? tiered_bases_[1] : first_local_base();
        if (successor > offset) {
            break;  // este segmento frío contiene `offset`: se conserva.
        }
        if (const auto removed = remove_tiered(tiered_bases_.front()); !removed) {
            return std::unexpected(removed.error());
        }
        tiered_bases_.erase(tiered_bases_.begin());
    }
    // Luego los segmentos locales sellados cuyo rango entero queda por debajo de `offset`. La
    // guarda `size() > 1` preserva siempre el activo.
    while (segments_.size() > 1 && segments_[1]->base_offset() <= offset) {
        const Offset base = segments_.front()->base_offset();
        segments_.erase(segments_.begin());  // cierra los fd del segmento (RAII).
        std::error_code ec;
        std::filesystem::remove(seg_path(dir_, base, ".log"), ec);
        std::filesystem::remove(seg_path(dir_, base, ".index"), ec);
    }
    recompute_log_start();
    return {};
}

expected<void> PartitionLog::reset_to(Offset offset) {
    if (offset < 0) {
        return make_error(ErrorCode::InvalidArgument, "reset_to: offset negativo");
    }
    // Descarta también el prefijo frío del tier (el seguidor adopta un snapshot: su log divergente,
    // frío incluido, deja de ser válido).
    for (const Offset base : tiered_bases_) {
        if (const auto removed = remove_tiered(base); !removed) {
            return std::unexpected(removed.error());
        }
    }
    tiered_bases_.clear();
    // Borra todos los segmentos (cierra los fd vía RAII) y luego sus ficheros.
    std::vector<Offset> bases;
    bases.reserve(segments_.size());
    for (const auto& seg : segments_) {
        bases.push_back(seg->base_offset());
    }
    segments_.clear();
    for (const Offset base : bases) {
        std::error_code ec;
        std::filesystem::remove(seg_path(dir_, base, ".log"), ec);
        std::filesystem::remove(seg_path(dir_, base, ".index"), ec);
    }
    // Crea un segmento nuevo y vacío en la base del snapshot.
    auto seg = Segment::create(dir_, offset, cfg_.index_interval_bytes, cfg_.encryption_key.get());
    if (!seg) {
        return std::unexpected(seg.error());
    }
    segments_.push_back(std::make_unique<Segment>(std::move(*seg)));
    log_start_offset_ = offset;
    log_end_offset_ = offset;
    recovery_point_ = offset;
    bytes_since_sync_ = 0;
    return {};
}

expected<void> PartitionLog::sync() {
    if (const auto synced = active()->sync(); !synced) {
        return synced;
    }
    recovery_point_ = log_end_offset_;
    bytes_since_sync_ = 0;
    return {};
}

expected<void> PartitionLog::enforce_retention(const RetentionPolicy& policy) {
    std::int64_t total = 0;
    for (const auto& seg : segments_) {
        total += static_cast<std::int64_t>(seg->size_bytes());
    }
    const auto now = std::filesystem::file_time_type::clock::now();

    // Borra el segmento local más antiguo mientras sea elegible, preservando siempre el activo. Si
    // hay prefijo **frío** (tier), no se toca el hot set local: los datos más antiguos ya están en
    // el tier (retención larga), y borrar un local dejaría un hueco sobre el prefijo frío.
    while (tiered_bases_.empty() && segments_.size() > 1) {
        const Offset base = segments_.front()->base_offset();
        const auto seg_bytes = static_cast<std::int64_t>(segments_.front()->size_bytes());

        bool eligible = policy.retention_bytes >= 0 && total > policy.retention_bytes;
        if (!eligible && policy.retention_ms >= 0) {
            std::error_code ec;
            const auto mtime = std::filesystem::last_write_time(seg_path(dir_, base, ".log"), ec);
            if (!ec) {
                const auto age =
                    std::chrono::duration_cast<std::chrono::milliseconds>(now - mtime).count();
                eligible = age > policy.retention_ms;
            }
        }
        if (!eligible) {
            break;  // el más antiguo ya no es elegible: el resto tampoco.
        }

        segments_.erase(segments_.begin());  // cierra los fd del segmento (RAII).
        std::error_code ec;
        std::filesystem::remove(seg_path(dir_, base, ".log"), ec);
        std::filesystem::remove(seg_path(dir_, base, ".index"), ec);
        total -= seg_bytes;
        log_start_offset_ = segments_.front()->base_offset();
    }
    return {};
}

expected<void> PartitionLog::maybe_sync(std::size_t appended_bytes) {
    switch (cfg_.fsync_policy) {
        case FsyncPolicy::None:
            return {};
        case FsyncPolicy::Commit:
            return sync();
        case FsyncPolicy::Interval:
            bytes_since_sync_ += appended_bytes;
            if (bytes_since_sync_ >= cfg_.fsync_interval_bytes) {
                return sync();
            }
            return {};
    }
    return {};  // inalcanzable (enum cerrado); satisface el análisis de flujo.
}

const Segment* PartitionLog::segment_for(Offset offset) const noexcept {
    // Primer segmento con base_offset > offset; el anterior es el que contiene el offset.
    const auto it = std::ranges::upper_bound(
        segments_, offset, std::ranges::less{},
        [](const std::unique_ptr<Segment>& seg) { return seg->base_offset(); });
    if (it == segments_.begin()) {
        return nullptr;  // offset por debajo del primer segmento.
    }
    return std::prev(it)->get();
}

expected<FetchResult> PartitionLog::read_local(Offset offset, std::size_t max_bytes) const {
    const Segment* seg = segment_for(offset);
    if (seg == nullptr) {
        return FetchResult{};  // por debajo del primer segmento local: nada que leer aquí.
    }
    return seg->read(offset, max_bytes);
}

expected<FetchResult> PartitionLog::read(Offset offset, std::size_t max_bytes) const {
    if (offset < log_start_offset_) {
        return make_error(ErrorCode::OutOfRange, "offset por debajo de log_start_offset");
    }
    FetchResult result;
    result.next_offset = offset;

    Offset cursor = offset;
    while (cursor < log_end_offset_ && result.batches.size() < max_bytes) {
        const std::size_t remaining = max_bytes - result.batches.size();
        // Prefijo frío (tier): rehidratación transparente. Suffix caliente: lectura local directa.
        auto fragment = (!tiered_bases_.empty() && cursor < first_local_base())
                            ? read_tiered(cursor, remaining)
                            : read_local(cursor, remaining);
        if (!fragment) {
            return std::unexpected(fragment.error());
        }
        if (fragment->batches.empty()) {
            break;  // nada más legible desde el cursor.
        }
        result.batches.append(fragment->batches.as_span());
        result.next_offset = fragment->next_offset;
        cursor = fragment->next_offset;
    }
    return result;
}

expected<FetchResult> PartitionLog::read_tiered(Offset offset, std::size_t max_bytes) const {
    // Segmento frío que contiene offset: el de mayor base ≤ offset.
    const auto it = std::ranges::upper_bound(tiered_bases_, offset);
    if (it == tiered_bases_.begin()) {
        return FetchResult{};  // por debajo del prefijo frío (no debería ocurrir).
    }
    const Offset base = *std::prev(it);

    // Rehidrata `.log` + `.index` a un directorio temporal único y lo limpia al salir (RAII). Es un
    // efecto de caché: `read` es lógicamente const (no cambia el estado lógico del log).
    const std::filesystem::path tmp = dir_ / ".rehydrate" / std::to_string(rehydrate_seq_++);
    std::error_code ec;
    std::filesystem::remove_all(tmp, ec);
    std::filesystem::create_directories(tmp, ec);
    if (ec) {
        return make_error(ErrorCode::IoError, "rehidratación: create_directories: " + ec.message());
    }
    struct TmpGuard {
        std::filesystem::path path;
        explicit TmpGuard(std::filesystem::path p) : path(std::move(p)) {}
        ~TmpGuard() {
            std::error_code cleanup;
            std::filesystem::remove_all(path, cleanup);
        }
        TmpGuard(const TmpGuard&) = delete;
        TmpGuard& operator=(const TmpGuard&) = delete;
        TmpGuard(TmpGuard&&) = delete;
        TmpGuard& operator=(TmpGuard&&) = delete;
    } const guard{tmp};

    if (const auto fetched = cfg_.tier->fetch_file(tier_key(cfg_, base, SegmentFileKind::Log),
                                                   seg_path(tmp, base, ".log"));
        !fetched) {
        return std::unexpected(fetched.error());
    }
    if (const auto fetched = cfg_.tier->fetch_file(tier_key(cfg_, base, SegmentFileKind::Index),
                                                   seg_path(tmp, base, ".index"));
        !fetched) {
        return std::unexpected(fetched.error());
    }

    auto seg = Segment::open(tmp, base, cfg_.index_interval_bytes, cfg_.encryption_key.get());
    if (!seg) {
        return std::unexpected(seg.error());
    }
    return seg->read(offset, max_bytes);  // el FetchResult copia los bytes; sobrevive al temporal.
}

expected<void> PartitionLog::roll_segment() {
    if (const auto sealed = active()->seal(); !sealed) {
        return sealed;
    }
    recovery_point_ = log_end_offset_;  // el segmento sellado queda durable hasta aquí.
    bytes_since_sync_ = 0;
    auto seg = Segment::create(dir_, log_end_offset_, cfg_.index_interval_bytes,
                               cfg_.encryption_key.get());
    if (!seg) {
        return std::unexpected(seg.error());
    }
    segments_.push_back(std::make_unique<Segment>(std::move(*seg)));

    // Política de tiering (ADR-0032): tras rotar, descarga los sellados al tier. Es **best-effort**
    // y no aborta la rotación: si el tier falla, los segmentos siguen locales y se reintentan en la
    // próxima rotación (offload idempotente). Con el tier local es una copia de fichero síncrona;
    // el offload asíncrono queda como trabajo futuro.
    if (cfg_.tier != nullptr) {
        [[maybe_unused]] const auto offloaded = offload_sealed_to_tier();
    }
    return {};
}

void PartitionLog::recompute_log_start() noexcept {
    log_start_offset_ =
        tiered_bases_.empty() ? segments_.front()->base_offset() : tiered_bases_.front();
}

expected<void> PartitionLog::remove_tiered(Offset base) {
    if (const auto removed = cfg_.tier->remove(tier_key(cfg_, base, SegmentFileKind::Log));
        !removed) {
        return removed;
    }
    if (const auto removed = cfg_.tier->remove(tier_key(cfg_, base, SegmentFileKind::Index));
        !removed) {
        return removed;
    }
    return {};
}

expected<std::size_t> PartitionLog::offload_sealed_to_tier() {
    if (cfg_.tier == nullptr) {
        return std::size_t{0};  // sin tier: no-op (comportamiento por defecto).
    }
    std::size_t offloaded = 0;
    // Los sellados locales son todos menos el activo (`segments_.back()`); del más antiguo al más
    // nuevo, preservando la contigüidad del prefijo frío.
    while (segments_.size() > 1) {
        const Offset base = segments_.front()->base_offset();
        // Sube `.log` y `.index` (idempotente y atómico). Si falla, NO se reclama: se reintenta.
        if (const auto put = cfg_.tier->put_file(tier_key(cfg_, base, SegmentFileKind::Log),
                                                 seg_path(dir_, base, ".log"));
            !put) {
            return std::unexpected(put.error());
        }
        if (const auto put = cfg_.tier->put_file(tier_key(cfg_, base, SegmentFileKind::Index),
                                                 seg_path(dir_, base, ".index"));
            !put) {
            return std::unexpected(put.error());
        }
        // Subida confirmada: ahora sí, reclama el espacio local (cierra los fd vía RAII).
        segments_.erase(segments_.begin());
        std::error_code ec;
        std::filesystem::remove(seg_path(dir_, base, ".log"), ec);
        std::filesystem::remove(seg_path(dir_, base, ".index"), ec);
        tiered_bases_.push_back(base);  // el prefijo frío queda ordenado (bases crecientes).
        ++offloaded;
    }
    recompute_log_start();  // inocuo: los datos siguen legibles, el prefijo frío avanza el
                            // arranque.
    return offloaded;
}

}  // namespace nexus
