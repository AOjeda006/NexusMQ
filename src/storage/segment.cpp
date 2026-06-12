#include "storage/segment.hpp"

#include <format>
#include <string>
#include <string_view>
#include <utility>

#include "common/bytes.hpp"

namespace nexus {
namespace {

// Nombre de fichero del segmento: offset base a 20 dígitos (orden lexicográfico = orden de
// offset) + extensión. 20 dígitos cubren el rango de Offset (int64) no negativo.
std::string segment_filename(Offset base_offset, std::string_view extension) {
    return std::format("{:020d}{}", base_offset, extension);
}

}  // namespace

Segment::Segment(Offset base_offset, File log, SparseIndex index, std::size_t size_bytes)
    : base_offset_(base_offset),
      log_(std::move(log)),
      index_(std::move(index)),
      size_bytes_(size_bytes) {}

expected<Segment> Segment::create(const std::filesystem::path& dir, Offset base_offset,
                                  std::size_t index_interval_bytes) {
    auto log =
        File::open((dir / segment_filename(base_offset, ".log")).string(), File::Mode::ReadWrite);
    if (!log) {
        return std::unexpected(log.error());
    }
    auto index = SparseIndex::open((dir / segment_filename(base_offset, ".index")).string(),
                                   base_offset, index_interval_bytes);
    if (!index) {
        return std::unexpected(index.error());
    }
    return Segment{base_offset, std::move(*log), std::move(*index), 0};
}

expected<Segment> Segment::open(const std::filesystem::path& dir, Offset base_offset,
                                std::size_t index_interval_bytes) {
    auto log =
        File::open((dir / segment_filename(base_offset, ".log")).string(), File::Mode::ReadWrite);
    if (!log) {
        return std::unexpected(log.error());
    }
    const auto size = log->size();
    if (!size) {
        return std::unexpected(size.error());
    }
    auto index = SparseIndex::open((dir / segment_filename(base_offset, ".index")).string(),
                                   base_offset, index_interval_bytes);
    if (!index) {
        return std::unexpected(index.error());
    }
    return Segment{base_offset, std::move(*log), std::move(*index),
                   static_cast<std::size_t>(*size)};
}

expected<Offset> Segment::append(const RecordBatch& batch) {
    if (state_ == State::Closed) {
        return make_error(ErrorCode::InvalidArgument, "append sobre un segmento sellado");
    }
    Buffer encoded;
    batch.encode(encoded);

    const std::size_t position = size_bytes_;
    if (const auto written = log_.write_at(encoded.as_span(), position); !written) {
        return std::unexpected(written.error());
    }
    index_.maybe_append(batch.header().base_offset, static_cast<std::uint32_t>(position),
                        encoded.size());
    size_bytes_ += encoded.size();
    return batch.last_offset();
}

expected<void> Segment::seal() {
    if (const auto flushed = index_.flush(); !flushed) {
        return flushed;
    }
    if (const auto synced = log_.sync(); !synced) {
        return synced;
    }
    state_ = State::Closed;
    return {};
}

}  // namespace nexus
