/// @file   consensus/raft_log.cpp
/// @brief  Implementación de RaftLog (vista (term,index) sobre PartitionLog, ADR-0014).
/// @ingroup consensus

#include "consensus/raft_log.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "common/bytes.hpp"
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

}  // namespace

RaftLog::RaftLog(PartitionLog& log, File meta, std::vector<EntryMeta> entries)
    : log_(log), meta_file_(std::move(meta)), entries_(std::move(entries)) {}

expected<RaftLog> RaftLog::open(PartitionLog& log, const std::string& meta_path) {
    auto file = File::open(meta_path, File::Mode::ReadWrite);
    if (!file) {
        return std::unexpected(file.error());
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

    // El sidecar debe concordar con el final del log de partición.
    const Offset expected_end =
        entries.empty() ? log.log_start_offset() : entries.back().last_offset + 1;
    if (expected_end != log.log_end_offset()) {
        return make_error(ErrorCode::Corrupt, "RaftLog: sidecar incoherente con el PartitionLog");
    }
    return RaftLog{log, std::move(*file), std::move(entries)};
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
    if (index > last_index()) {
        return {};  // nada que truncar.
    }
    const Offset base = entries_[static_cast<std::size_t>(index - 1)].base_offset;
    if (const auto truncated = log_.truncate_to(base); !truncated) {
        return std::unexpected(truncated.error());
    }
    const auto kept = static_cast<std::uint64_t>(index - 1) * kMetaRecordSize;
    if (const auto truncated = meta_file_.truncate(kept); !truncated) {
        return std::unexpected(truncated.error());
    }
    entries_.resize(static_cast<std::size_t>(index - 1));
    return {};
}

expected<Term> RaftLog::term_at(Index index) const {
    if (index == 0) {
        return Term{0};  // centinela "antes de la primera entrada".
    }
    if (index < 0 || index > last_index()) {
        return make_error(ErrorCode::OutOfRange, "term_at: índice fuera de rango");
    }
    return entries_[static_cast<std::size_t>(index - 1)].term;
}

expected<std::pair<Offset, Offset>> RaftLog::offsets_at(Index index) const {
    if (index < 1 || index > last_index()) {
        return make_error(ErrorCode::OutOfRange, "offsets_at: índice fuera de rango");
    }
    const EntryMeta& meta = entries_[static_cast<std::size_t>(index - 1)];
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
    std::vector<RaftLogEntry> result;
    for (Index i = index; i <= last_index() && result.size() < max; ++i) {
        const EntryMeta& meta = entries_[static_cast<std::size_t>(i - 1)];
        auto payload = read_payload(meta);
        if (!payload) {
            return std::unexpected(payload.error());
        }
        result.push_back(RaftLogEntry{
            .term = meta.term, .index = i, .type = meta.type, .payload = std::move(*payload)});
    }
    return result;
}

}  // namespace nexus
