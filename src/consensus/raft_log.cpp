/// @file   consensus/raft_log.cpp
/// @brief  Implementación de RaftLog (vista (term,index) sobre PartitionLog, ADR-0014).
/// @ingroup consensus

#include "consensus/raft_log.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "common/bytes.hpp"
#include "common/crc32c.hpp"
#include "common/record.hpp"

namespace nexus {
namespace {

// (De)serialización de un registro del sidecar (tamaño fijo, little-endian como el resto del wire).
void encode_meta(Term term, Offset base, Offset last, RaftEntryType type,
                 std::array<std::byte, 25>& out) {
    store_le<std::int64_t>(term, MutByteSpan{out.data(), 8});
    store_le<std::int64_t>(base, MutByteSpan{out.data() + 8, 8});
    store_le<std::int64_t>(last, MutByteSpan{out.data() + 16, 8});
    out[24] = static_cast<std::byte>(type);
}

// Desplazamientos del sidecar de snapshot: `crc:u32 | index:i64 | term:i64 | offset:i64`.
constexpr std::size_t kSnapCrcOffset = 0;
constexpr std::size_t kSnapPayloadOffset = 4;  // el CRC cubre desde el índice hasta el final.

}  // namespace

RaftLog::RaftLog(PartitionLog& log, File meta, File snapshot, std::vector<EntryMeta> entries,
                 Index snapshot_index, Term snapshot_term, Offset snapshot_last_offset)
    : log_(log),
      meta_file_(std::move(meta)),
      snapshot_file_(std::move(snapshot)),
      entries_(std::move(entries)),
      snapshot_index_(snapshot_index),
      snapshot_term_(snapshot_term),
      snapshot_last_offset_(snapshot_last_offset) {}

namespace {

// Lee la base de snapshot del sidecar (vacío = sin snapshot); `Corrupt` si trunca o el CRC no casa.
struct SnapshotBase {
    Index index = 0;
    Term term = 0;
    Offset last_offset = 0;
};

expected<SnapshotBase> load_snapshot_base(const File& file) {
    const auto file_size = file.size();
    if (!file_size) {
        return std::unexpected(file_size.error());
    }
    if (*file_size == 0) {
        return SnapshotBase{};  // sin snapshot todavía.
    }
    constexpr std::size_t kSize = 28;  // crc:u32 + index/term/offset:i64.
    if (*file_size < kSize) {
        return make_error(ErrorCode::Corrupt, "raft-snapshot: registro truncado");
    }
    std::array<std::byte, kSize> record{};
    const auto read = file.read_at(MutByteSpan{record}, 0);
    if (!read) {
        return std::unexpected(read.error());
    }
    if (*read < kSize) {
        return make_error(ErrorCode::Corrupt, "raft-snapshot: lectura corta");
    }
    const ByteSpan view{record};
    const auto stored_crc = load_le<std::uint32_t>(view.subspan(kSnapCrcOffset, 4));
    const auto computed_crc = crc32c(view.subspan(kSnapPayloadOffset, kSize - kSnapPayloadOffset));
    if (stored_crc != computed_crc) {
        return make_error(ErrorCode::Corrupt, "raft-snapshot: CRC32C no casa");
    }
    return SnapshotBase{.index = load_le<std::int64_t>(view.subspan(4, 8)),
                        .term = load_le<std::int64_t>(view.subspan(12, 8)),
                        .last_offset = load_le<std::int64_t>(view.subspan(20, 8))};
}

}  // namespace

expected<RaftLog> RaftLog::open(PartitionLog& log, const std::string& meta_path) {
    auto file = File::open(meta_path, File::Mode::ReadWrite);
    if (!file) {
        return std::unexpected(file.error());
    }
    auto snap_file = File::open(meta_path + ".snap", File::Mode::ReadWrite);
    if (!snap_file) {
        return std::unexpected(snap_file.error());
    }
    const auto snapshot = load_snapshot_base(*snap_file);
    if (!snapshot) {
        return std::unexpected(snapshot.error());
    }
    const auto file_size = file->size();
    if (!file_size) {
        return std::unexpected(file_size.error());
    }
    if (*file_size % kMetaRecordSize != 0) {
        return make_error(ErrorCode::Corrupt,
                          "sidecar de RaftLog: tamaño no múltiplo del registro");
    }

    const auto count = static_cast<std::size_t>(*file_size / kMetaRecordSize);
    std::vector<EntryMeta> entries;
    entries.reserve(count);
    std::array<std::byte, kMetaRecordSize> record{};
    for (std::size_t i = 0; i < count; ++i) {
        const auto read = file->read_at(record, static_cast<std::uint64_t>(i) * kMetaRecordSize);
        if (!read) {
            return std::unexpected(read.error());
        }
        if (*read < kMetaRecordSize) {
            return make_error(ErrorCode::Corrupt, "sidecar de RaftLog truncado");
        }
        const ByteSpan view{record};
        entries.push_back(EntryMeta{
            .term = load_le<std::int64_t>(view.subspan(0, 8)),
            .base_offset = load_le<std::int64_t>(view.subspan(8, 8)),
            .last_offset = load_le<std::int64_t>(view.subspan(16, 8)),
            .type = static_cast<RaftEntryType>(std::to_integer<std::uint8_t>(record[24]))});
    }

    // Coherencia con el PartitionLog. La primera entrada viva arranca justo tras el snapshot (si lo
    // hay); el final del log de partición casa con la última entrada (o con la base de snapshot si
    // no quedan entradas vivas tras compactar).
    if (snapshot->index > 0 && !entries.empty() &&
        entries.front().base_offset != snapshot->last_offset + 1) {
        return make_error(ErrorCode::Corrupt,
                          "RaftLog: primera entrada incoherente con el snapshot");
    }
    Offset expected_end = log.log_start_offset();
    if (!entries.empty()) {
        expected_end = entries.back().last_offset + 1;
    } else if (snapshot->index > 0) {
        expected_end = snapshot->last_offset + 1;
    }
    if (expected_end != log.log_end_offset()) {
        return make_error(ErrorCode::Corrupt, "RaftLog: sidecar incoherente con el PartitionLog");
    }
    return RaftLog{log,
                   std::move(*file),
                   std::move(*snap_file),
                   std::move(entries),
                   snapshot->index,
                   snapshot->term,
                   snapshot->last_offset};
}

expected<void> RaftLog::persist_meta(const EntryMeta& meta) {
    std::array<std::byte, kMetaRecordSize> record{};
    encode_meta(meta.term, meta.base_offset, meta.last_offset, meta.type, record);
    const auto position = static_cast<std::uint64_t>(entries_.size()) * kMetaRecordSize;
    return meta_file_.write_at(ByteSpan{record}, position);
}

expected<Index> RaftLog::append(std::span<const RaftLogEntry> entries) {
    for (const RaftLogEntry& entry : entries) {
        const expected<RecordBatch> batch = RecordBatch::decode(ByteSpan{entry.payload});
        if (!batch) {
            return std::unexpected(batch.error());
        }
        const Offset base = log_.log_end_offset();
        const expected<Offset> last = log_.append(*batch);
        if (!last) {
            return std::unexpected(last.error());
        }
        const EntryMeta meta{
            .term = entry.term, .base_offset = base, .last_offset = *last, .type = entry.type};
        if (const auto persisted = persist_meta(meta); !persisted) {
            return std::unexpected(persisted.error());
        }
        entries_.push_back(meta);
    }
    return last_index();
}

expected<void> RaftLog::truncate_from(Index index) {
    if (index < 1) {
        return make_error(ErrorCode::InvalidArgument, "truncate_from: índice < 1");
    }
    if (index <= snapshot_index_) {
        return make_error(ErrorCode::InvalidArgument, "truncate_from: índice ya compactado");
    }
    if (index > last_index()) {
        return {};  // nada que truncar.
    }
    const Offset base = entries_[slot_of(index)].base_offset;
    if (const auto truncated = log_.truncate_to(base); !truncated) {
        return std::unexpected(truncated.error());
    }
    const auto kept = static_cast<std::uint64_t>(slot_of(index)) * kMetaRecordSize;
    if (const auto truncated = meta_file_.truncate(kept); !truncated) {
        return std::unexpected(truncated.error());
    }
    entries_.resize(slot_of(index));
    return {};
}

expected<Term> RaftLog::term_at(Index index) const {
    if (index == 0) {
        return Term{0};  // centinela "antes de la primera entrada".
    }
    if (index == snapshot_index_) {
        return snapshot_term_;  // frontera del snapshot: su término sobrevive a la compactación.
    }
    if (index < snapshot_index_) {
        return make_error(ErrorCode::OutOfRange, "term_at: índice compactado");
    }
    if (index > last_index()) {
        return make_error(ErrorCode::OutOfRange, "term_at: índice fuera de rango");
    }
    return entries_[slot_of(index)].term;
}

expected<std::pair<Offset, Offset>> RaftLog::offsets_at(Index index) const {
    if (index <= snapshot_index_ || index > last_index()) {
        return make_error(ErrorCode::OutOfRange, "offsets_at: índice fuera de rango");
    }
    const EntryMeta& meta = entries_[slot_of(index)];
    return std::pair<Offset, Offset>{meta.base_offset, meta.last_offset};
}

expected<std::vector<std::byte>> RaftLog::read_payload(const EntryMeta& entry) const {
    // `max_bytes = 1` devuelve exactamente el batch que empieza en `base_offset` (siempre ≥ 1).
    const auto fragment = log_.read(entry.base_offset, 1);
    if (!fragment) {
        return std::unexpected(fragment.error());
    }
    const ByteSpan bytes = fragment->batches.as_span();
    if (bytes.empty()) {
        return make_error(ErrorCode::Corrupt, "RaftLog: entrada ausente en el PartitionLog");
    }
    return std::vector<std::byte>{bytes.begin(), bytes.end()};
}

expected<std::vector<RaftLogEntry>> RaftLog::entries_from(Index index, std::size_t max) const {
    if (index < 1) {
        return make_error(ErrorCode::InvalidArgument, "entries_from: índice < 1");
    }
    if (index <= snapshot_index_) {
        // Las entradas pedidas están cubiertas por el snapshot: el llamante (líder) debe poner al
        // día al seguidor con `InstallSnapshot`, no con `AppendEntries` (ADR-0024).
        return make_error(ErrorCode::OutOfRange,
                          "entries_from: índice compactado (requiere snapshot)");
    }
    std::vector<RaftLogEntry> result;
    for (Index i = index; i <= last_index() && result.size() < max; ++i) {
        const EntryMeta& meta = entries_[slot_of(i)];
        auto payload = read_payload(meta);
        if (!payload) {
            return std::unexpected(payload.error());
        }
        result.push_back(RaftLogEntry{
            .term = meta.term, .index = i, .type = meta.type, .payload = std::move(*payload)});
    }
    return result;
}

expected<void> RaftLog::persist_snapshot() {
    constexpr std::size_t kSize = kSnapshotRecordSize;
    std::array<std::byte, kSize> record{};
    const MutByteSpan out{record};
    store_le<std::int64_t>(snapshot_index_, out.subspan(4, 8));
    store_le<std::int64_t>(snapshot_term_, out.subspan(12, 8));
    store_le<std::int64_t>(snapshot_last_offset_, out.subspan(20, 8));
    const auto crc =
        crc32c(ByteSpan{record}.subspan(kSnapPayloadOffset, kSize - kSnapPayloadOffset));
    store_le<std::uint32_t>(crc, out.subspan(kSnapCrcOffset, 4));
    if (const auto written = snapshot_file_.write_at(ByteSpan{record}, 0); !written) {
        return std::unexpected(written.error());
    }
    return snapshot_file_.sync();
}

expected<void> RaftLog::rewrite_meta() {
    // Reescribe el sidecar con exactamente las entradas vivas, posicional desde 0.
    for (std::size_t k = 0; k < entries_.size(); ++k) {
        std::array<std::byte, kMetaRecordSize> record{};
        encode_meta(entries_[k].term, entries_[k].base_offset, entries_[k].last_offset,
                    entries_[k].type, record);
        if (const auto written = meta_file_.write_at(ByteSpan{record}, k * kMetaRecordSize);
            !written) {
            return std::unexpected(written.error());
        }
    }
    if (const auto truncated = meta_file_.truncate(entries_.size() * kMetaRecordSize); !truncated) {
        return std::unexpected(truncated.error());
    }
    return meta_file_.sync();
}

expected<void> RaftLog::compact_to(Index up_to_index) {
    if (up_to_index <= snapshot_index_) {
        return {};  // ya compactado hasta aquí: no-op.
    }
    if (up_to_index > last_index()) {
        return make_error(ErrorCode::InvalidArgument, "compact_to: índice más allá del log");
    }
    // Captura la base de snapshot antes de descartar nada.
    const EntryMeta& boundary = entries_[slot_of(up_to_index)];
    const Term new_term = boundary.term;
    const Offset new_last_offset = boundary.last_offset;

    // Recorta el prefijo físico del PartitionLog (best-effort por segmentos enteros): conserva
    // desde el primer offset posterior al snapshot.
    if (const auto trimmed = log_.truncate_prefix_to(new_last_offset + 1); !trimmed) {
        return std::unexpected(trimmed.error());
    }
    // Descarta el prefijo lógico (las entradas 1..up_to_index relativas a la base previa).
    const std::size_t drop = slot_of(up_to_index) + 1;
    entries_.erase(entries_.begin(), entries_.begin() + static_cast<std::ptrdiff_t>(drop));
    snapshot_index_ = up_to_index;
    snapshot_term_ = new_term;
    snapshot_last_offset_ = new_last_offset;

    // Persiste la nueva base (durable) y reescribe el sidecar de metadatos con las entradas vivas.
    if (const auto persisted = persist_snapshot(); !persisted) {
        return std::unexpected(persisted.error());
    }
    return rewrite_meta();
}

expected<void> RaftLog::install_snapshot(Index index, Term term, Offset last_offset) {
    if (index <= snapshot_index_) {
        return {};  // snapshot obsoleto respecto al que ya tenemos.
    }
    // Si ya tenemos la entrada (index, term), basta con compactar hasta ella y conservar la cola
    // consistente (optimización del §7 del paper de Raft).
    if (index <= last_index()) {
        const auto existing = term_at(index);
        if (existing && *existing == term) {
            return compact_to(index);
        }
    }
    // Si no, descarta todo el log y reabre el PartitionLog vacío en la base del snapshot; la cola
    // posterior llegará por `AppendEntries`.
    if (const auto reset = log_.reset_to(last_offset + 1); !reset) {
        return std::unexpected(reset.error());
    }
    entries_.clear();
    snapshot_index_ = index;
    snapshot_term_ = term;
    snapshot_last_offset_ = last_offset;
    if (const auto persisted = persist_snapshot(); !persisted) {
        return std::unexpected(persisted.error());
    }
    return rewrite_meta();
}

}  // namespace nexus
