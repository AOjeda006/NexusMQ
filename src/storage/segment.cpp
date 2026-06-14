#include "storage/segment.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <format>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

expected<FetchResult> Segment::read(Offset offset, std::size_t max_bytes) const {
    FetchResult result;
    result.next_offset = offset;

    std::size_t position = index_.floor(offset).file_position;
    std::array<std::byte, RecordBatch::kHeaderSize> header{};
    while (position < size_bytes_) {
        const auto read_header = log_.read_at(header, position);
        if (!read_header) {
            return std::unexpected(read_header.error());
        }
        if (*read_header < header.size()) {
            break;  // cola incompleta (no debería ocurrir en un segmento íntegro)
        }
        const auto view = RecordBatch::peek(header);
        if (!view) {
            break;  // cabecera ilegible: fin de lo recuperable
        }
        const std::size_t total = view->encoded_size;
        if (position + total > size_bytes_) {
            break;  // batch truncado en disco
        }
        if (view->last_offset() < offset) {
            position += total;  // batch enteramente anterior al offset pedido
            continue;
        }
        if (!result.batches.empty() && result.batches.size() + total > max_bytes) {
            break;  // límite de tamaño (siempre se devuelve al menos un batch)
        }
        std::vector<std::byte> body(total);
        const auto read_body = log_.read_at(body, position);
        if (!read_body) {
            return std::unexpected(read_body.error());
        }
        if (*read_body < total) {
            break;
        }
        result.batches.append(body);
        result.next_offset = view->last_offset() + 1;
        position += total;
    }
    return result;
}

expected<Offset> Segment::recover() {
    if (const auto cleared = index_.reset(); !cleared) {
        return std::unexpected(cleared.error());
    }
    std::size_t valid_end = 0;
    Offset last_valid = base_offset_ - 1;  // segmento vacío: aún nada válido.
    std::array<std::byte, RecordBatch::kHeaderSize> header{};

    while (valid_end < size_bytes_) {
        const auto read_header = log_.read_at(header, valid_end);
        if (!read_header) {
            return std::unexpected(read_header.error());
        }
        if (*read_header < header.size()) {
            break;  // cabecera incompleta: cola torn.
        }
        const auto view = RecordBatch::peek(header);
        if (!view) {
            break;  // cabecera inconsistente.
        }
        const std::size_t total = view->encoded_size;
        if (valid_end + total > size_bytes_) {
            break;  // batch truncado al final.
        }
        std::vector<std::byte> body(total);
        const auto read_body = log_.read_at(body, valid_end);
        if (!read_body) {
            return std::unexpected(read_body.error());
        }
        if (*read_body < total) {
            break;
        }
        const auto batch = RecordBatch::decode(body);
        if (!batch) {
            break;  // CRC no cuadra / corrupto: aquí empieza la cola torn.
        }
        index_.maybe_append(view->base_offset, static_cast<std::uint32_t>(valid_end), total);
        last_valid = batch->last_offset();
        valid_end += total;
    }

    if (valid_end < size_bytes_) {
        if (const auto truncated = log_.truncate(valid_end); !truncated) {
            return std::unexpected(truncated.error());
        }
        size_bytes_ = valid_end;
    }
    if (const auto flushed = index_.flush(); !flushed) {
        return std::unexpected(flushed.error());
    }
    return last_valid;
}

expected<std::size_t> Segment::position_of(Offset target) const {
    if (target == base_offset_) {
        return std::size_t{0};  // frontera del primer batch: se vacía el segmento entero.
    }
    std::size_t position = 0;
    std::array<std::byte, RecordBatch::kHeaderSize> header{};
    while (position < size_bytes_) {
        const auto read_header = log_.read_at(header, position);
        if (!read_header) {
            return std::unexpected(read_header.error());
        }
        if (*read_header < header.size()) {
            break;  // cabecera incompleta: trata el resto como inexistente.
        }
        const auto view = RecordBatch::peek(header);
        if (!view) {
            break;
        }
        if (view->base_offset == target) {
            return position;
        }
        if (view->base_offset > target) {
            break;  // target cae dentro del batch anterior: no es frontera.
        }
        position += view->encoded_size;
    }
    return make_error(ErrorCode::InvalidArgument, "truncate_to: el offset no es frontera de batch");
}

expected<void> Segment::rebuild_index() {
    if (const auto cleared = index_.reset(); !cleared) {
        return std::unexpected(cleared.error());
    }
    std::size_t scan = 0;
    std::array<std::byte, RecordBatch::kHeaderSize> header{};
    while (scan < size_bytes_) {
        const auto read_header = log_.read_at(header, scan);
        if (!read_header || *read_header < header.size()) {
            break;
        }
        const auto view = RecordBatch::peek(header);
        if (!view) {
            break;
        }
        index_.maybe_append(view->base_offset, static_cast<std::uint32_t>(scan),
                            view->encoded_size);
        scan += view->encoded_size;
    }
    return index_.flush();
}

expected<void> Segment::truncate_to(Offset target) {
    const auto position = position_of(target);
    if (!position) {
        return std::unexpected(position.error());
    }
    if (*position < size_bytes_) {
        if (const auto truncated = log_.truncate(*position); !truncated) {
            return std::unexpected(truncated.error());
        }
        size_bytes_ = *position;
    }
    if (const auto reindexed = rebuild_index(); !reindexed) {
        return std::unexpected(reindexed.error());
    }
    state_ = State::Active;
    return {};
}

expected<void> Segment::sync() {
    if (const auto flushed = index_.flush(); !flushed) {
        return flushed;
    }
    return log_.sync();
}

expected<void> Segment::seal() {
    if (const auto synced = sync(); !synced) {
        return synced;
    }
    state_ = State::Closed;
    return {};
}

}  // namespace nexus
