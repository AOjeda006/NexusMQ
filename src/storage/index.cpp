#include "storage/index.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <functional>
#include <limits>
#include <utility>

#include "common/bytes.hpp"

namespace nexus {
namespace {

// Posiciones de los dos u32 little-endian de una entrada en disco.
constexpr std::size_t kRelativeOffsetPos = 0;
constexpr std::size_t kFilePositionPos = 4;

void encode_entry(const IndexEntry& entry, MutByteSpan out) noexcept {
    store_le<std::uint32_t>(entry.relative_offset, out.subspan(kRelativeOffsetPos));
    store_le<std::uint32_t>(entry.file_position, out.subspan(kFilePositionPos));
}

[[nodiscard]] IndexEntry decode_entry(ByteSpan in) noexcept {
    return IndexEntry{
        .relative_offset = load_le<std::uint32_t>(in.subspan(kRelativeOffsetPos)),
        .file_position = load_le<std::uint32_t>(in.subspan(kFilePositionPos)),
    };
}

}  // namespace

SparseIndex::SparseIndex(File file, Offset base_offset, std::size_t index_interval_bytes,
                         std::vector<IndexEntry> entries)
    : file_(std::move(file)),
      base_offset_(base_offset),
      interval_(index_interval_bytes),
      entries_(std::move(entries)),
      flushed_(entries_.size()) {}

expected<SparseIndex> SparseIndex::open(const std::string& path, Offset base_offset,
                                        std::size_t index_interval_bytes) {
    auto file = File::open(path, File::Mode::ReadWrite);
    if (!file) {
        return std::unexpected(file.error());
    }
    const auto bytes = file->size();
    if (!bytes) {
        return std::unexpected(bytes.error());
    }

    // Solo se leen entradas completas; una cola parcial (escritura interrumpida) se ignora:
    // el índice es una estructura derivada que la recuperación del log reconstruye (M4).
    const std::size_t count = static_cast<std::size_t>(*bytes) / kEntrySize;
    std::vector<IndexEntry> entries;
    entries.reserve(count);

    if (count > 0) {
        std::vector<std::byte> raw(count * kEntrySize);
        const auto read = file->read_at(raw, 0);
        if (!read) {
            return std::unexpected(read.error());
        }
        if (*read != raw.size()) {
            return make_error(ErrorCode::Corrupt, "índice .index truncado al leer");
        }
        const ByteSpan all{raw};
        for (std::size_t i = 0; i < count; ++i) {
            const IndexEntry entry = decode_entry(all.subspan(i * kEntrySize, kEntrySize));
            if (i > 0 && entry.relative_offset <= entries.back().relative_offset) {
                return make_error(ErrorCode::Corrupt, "índice .index no estrictamente creciente");
            }
            entries.push_back(entry);
        }
    }
    return SparseIndex{std::move(*file), base_offset, index_interval_bytes, std::move(entries)};
}

void SparseIndex::maybe_append(Offset offset, std::uint32_t file_position, std::size_t batch_len) {
    if (bytes_since_ >= interval_) {
        const auto relative = static_cast<std::uint32_t>(offset - base_offset_);
        entries_.push_back(IndexEntry{.relative_offset = relative, .file_position = file_position});
        bytes_since_ = 0;
    }
    bytes_since_ += batch_len;
}

IndexEntry SparseIndex::floor(Offset target) const noexcept {
    if (target < base_offset_ || entries_.empty()) {
        return IndexEntry{};  // {0,0}: barre desde el inicio del segmento.
    }
    // El offset relativo cabe en u32 dentro de un segmento ('segment_bytes' lo acota); si
    // excediera, se satura para que todas las anclas queden por debajo (devuelve la última).
    constexpr Offset kMaxRelative = std::numeric_limits<std::uint32_t>::max();
    const auto rel = static_cast<std::uint32_t>(std::min(target - base_offset_, kMaxRelative));
    // Primera ancla con relative_offset > rel; la anterior es el floor buscado.
    const auto it =
        std::ranges::upper_bound(entries_, rel, std::ranges::less{}, &IndexEntry::relative_offset);
    if (it == entries_.begin()) {
        return IndexEntry{};  // target anterior a la primera ancla.
    }
    return *(it - 1);
}

expected<void> SparseIndex::flush() {
    if (flushed_ < entries_.size()) {
        Buffer buf{(entries_.size() - flushed_) * kEntrySize};
        std::array<std::byte, kEntrySize> tmp{};
        for (std::size_t i = flushed_; i < entries_.size(); ++i) {
            encode_entry(entries_[i], tmp);
            buf.append(tmp);
        }
        if (const auto written = file_.write_at(buf.as_span(), flushed_ * kEntrySize); !written) {
            return written;
        }
        flushed_ = entries_.size();
    }
    return file_.sync();
}

}  // namespace nexus
