#include "storage/segment.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <format>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/bytes.hpp"
#include "storage/segment_crypto.hpp"

namespace nexus {
namespace {

// Nombre de fichero del segmento: offset base a 20 dígitos (orden lexicográfico = orden de
// offset) + extensión. 20 dígitos cubren el rango de Offset (int64) no negativo.
std::string segment_filename(Offset base_offset, std::string_view extension) {
    return std::format("{:020d}{}", base_offset, extension);
}

// La cabecera de un bloque cifrado no cabe en menos de la de un batch en claro: leer una cabecera
// cifrada basta para peek de ambos formatos.
static_assert(kEncBlockHeaderSize >= RecordBatch::kHeaderSize);

}  // namespace

Segment::Segment(Offset base_offset, File log, SparseIndex index, std::size_t size_bytes,
                 std::optional<SegmentCipher> cipher, std::size_t data_start)
    : base_offset_(base_offset),
      log_(std::move(log)),
      index_(std::move(index)),
      size_bytes_(size_bytes),
      cipher_(cipher),
      data_start_(data_start) {}

expected<Segment> Segment::create(const std::filesystem::path& dir, Offset base_offset,
                                  std::size_t index_interval_bytes, const EncryptionKey* key) {
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

    std::optional<SegmentCipher> cipher;
    std::size_t data_start = 0;
    std::size_t size_bytes = 0;
    if (key != nullptr) {
        // Segmento cifrado: salt aleatoria por segmento -> DEK; cabecera de cifrado al inicio.
        std::array<std::byte, kEncSaltBytes> salt{};
        if (const auto seeded = random_bytes(salt); !seeded) {
            return std::unexpected(seeded.error());
        }
        auto segment_cipher = SegmentCipher::create(*key, salt);
        if (!segment_cipher) {
            return std::unexpected(segment_cipher.error());
        }
        std::array<std::byte, kEncSegmentHeaderSize> header{};
        encode_segment_header(salt, header);
        if (const auto written = log->write_at(header, 0); !written) {
            return std::unexpected(written.error());
        }
        cipher = *segment_cipher;
        data_start = kEncSegmentHeaderSize;
        size_bytes = kEncSegmentHeaderSize;
    }
    return Segment{base_offset, std::move(*log), std::move(*index), size_bytes, cipher, data_start};
}

expected<Segment> Segment::open(const std::filesystem::path& dir, Offset base_offset,
                                std::size_t index_interval_bytes, const EncryptionKey* key) {
    auto log =
        File::open((dir / segment_filename(base_offset, ".log")).string(), File::Mode::ReadWrite);
    if (!log) {
        return std::unexpected(log.error());
    }
    const auto size = log->size();
    if (!size) {
        return std::unexpected(size.error());
    }
    const auto file_size = static_cast<std::size_t>(*size);

    std::optional<SegmentCipher> cipher;
    std::size_t data_start = 0;
    // Autodetecta cifrado por la cabecera del `.log` (permite logs mixtos claro/cifrado).
    if (file_size >= kEncSegmentHeaderSize) {
        std::array<std::byte, kEncSegmentHeaderSize> header{};
        const auto read_header = log->read_at(header, 0);
        if (!read_header) {
            return std::unexpected(read_header.error());
        }
        if (*read_header == header.size() && is_encrypted_segment_header(header)) {
            if (key == nullptr) {
                return make_error(ErrorCode::Unsupported,
                                  "segmento cifrado, pero no se configuró clave de cifrado");
            }
            auto salt = parse_segment_header(header);
            if (!salt) {
                return std::unexpected(salt.error());
            }
            auto segment_cipher = SegmentCipher::create(*key, *salt);
            if (!segment_cipher) {
                return std::unexpected(segment_cipher.error());
            }
            cipher = *segment_cipher;
            data_start = kEncSegmentHeaderSize;
        }
    }

    auto index = SparseIndex::open((dir / segment_filename(base_offset, ".index")).string(),
                                   base_offset, index_interval_bytes);
    if (!index) {
        return std::unexpected(index.error());
    }
    return Segment{base_offset, std::move(*log), std::move(*index), file_size, cipher, data_start};
}

std::size_t Segment::block_header_size() const noexcept {
    return cipher_ ? kEncBlockHeaderSize : RecordBatch::kHeaderSize;
}

expected<std::optional<Segment::BlockHeader>> Segment::peek_block(std::size_t position) const {
    const std::size_t header_size = block_header_size();
    if (position + header_size > size_bytes_) {
        return std::optional<BlockHeader>{std::nullopt};  // sin cabecera completa: fin/cola torn.
    }
    std::array<std::byte, kEncBlockHeaderSize> buffer{};
    const MutByteSpan header{buffer.data(), header_size};
    const auto read = log_.read_at(header, position);
    if (!read) {
        return std::unexpected(read.error());
    }
    if (*read < header_size) {
        return std::optional<BlockHeader>{std::nullopt};
    }
    if (cipher_) {
        const auto view = SegmentCipher::peek_block(header);
        if (!view) {
            return std::optional<BlockHeader>{std::nullopt};  // cabecera cifrada inconsistente.
        }
        return std::optional<BlockHeader>{BlockHeader{.base_offset = view->base_offset,
                                                      .record_count = view->record_count,
                                                      .on_disk_size = view->on_disk_size()}};
    }
    const auto view = RecordBatch::peek(header);
    if (!view) {
        return std::optional<BlockHeader>{std::nullopt};
    }
    return std::optional<BlockHeader>{BlockHeader{.base_offset = view->base_offset,
                                                  .record_count = view->record_count,
                                                  .on_disk_size = view->encoded_size}};
}

expected<void> Segment::load_block(std::size_t position, std::size_t on_disk_size,
                                   Buffer& out) const {
    std::vector<std::byte> block(on_disk_size);
    const auto read = log_.read_at(block, position);
    if (!read) {
        return std::unexpected(read.error());
    }
    if (*read < on_disk_size) {
        return make_error(ErrorCode::Corrupt, "bloque truncado en disco");
    }
    if (cipher_) {
        return cipher_->open_block(block, out);  // Corrupt si el tag/AAD no cuadran (manipulación).
    }
    out.append(block);  // En claro: el bloque en disco ES el batch.
    return {};
}

expected<Offset> Segment::append(const RecordBatch& batch) {
    if (state_ == State::Closed) {
        return make_error(ErrorCode::InvalidArgument, "append sobre un segmento sellado");
    }
    Buffer encoded;
    batch.encode(encoded);

    const std::size_t position = size_bytes_;
    std::size_t on_disk_size = 0;
    if (cipher_) {
        Buffer framed;
        if (const auto sealed = cipher_->seal_block(encoded.as_span(), batch.header().base_offset,
                                                    batch.header().record_count, framed);
            !sealed) {
            return std::unexpected(sealed.error());
        }
        if (const auto written = log_.write_at(framed.as_span(), position); !written) {
            return std::unexpected(written.error());
        }
        on_disk_size = framed.size();
    } else {
        if (const auto written = log_.write_at(encoded.as_span(), position); !written) {
            return std::unexpected(written.error());
        }
        on_disk_size = encoded.size();
    }
    index_.maybe_append(batch.header().base_offset, static_cast<std::uint32_t>(position),
                        on_disk_size);
    size_bytes_ += on_disk_size;
    return batch.last_offset();
}

expected<FetchResult> Segment::read(Offset offset, std::size_t max_bytes) const {
    FetchResult result;
    result.next_offset = offset;

    std::size_t position = std::max<std::size_t>(index_.floor(offset).file_position, data_start_);
    while (position < size_bytes_) {
        const auto header = peek_block(position);
        if (!header) {
            return std::unexpected(header.error());
        }
        const std::optional<BlockHeader>& block = *header;
        if (!block.has_value()) {
            break;  // cola incompleta (no debería ocurrir en un segmento íntegro).
        }
        const BlockHeader info = *block;
        const std::size_t total = info.on_disk_size;
        if (position + total > size_bytes_) {
            break;  // bloque truncado en disco.
        }
        if (info.last_offset() < offset) {
            position += total;  // bloque enteramente anterior al offset pedido.
            continue;
        }
        if (!result.batches.empty() && result.batches.size() + total > max_bytes) {
            break;  // límite de tamaño (siempre se devuelve al menos un batch).
        }
        // Carga el PLAINTEXT del batch (descifra si el segmento está cifrado). Una manipulación
        // aquí produce un error autenticado, nunca datos corruptos silenciosos.
        if (const auto loaded = load_block(position, total, result.batches); !loaded) {
            return std::unexpected(loaded.error());
        }
        result.next_offset = info.last_offset() + 1;
        position += total;
    }
    return result;
}

expected<Offset> Segment::recover() {
    if (const auto cleared = index_.reset(); !cleared) {
        return std::unexpected(cleared.error());
    }
    std::size_t valid_end = data_start_;
    Offset last_valid = base_offset_ - 1;  // segmento vacío: aún nada válido.

    while (valid_end < size_bytes_) {
        const auto header = peek_block(valid_end);
        if (!header) {
            return std::unexpected(header.error());
        }
        const std::optional<BlockHeader>& block = *header;
        if (!block.has_value()) {
            break;  // cabecera incompleta: cola torn.
        }
        const BlockHeader info = *block;
        const std::size_t total = info.on_disk_size;
        if (valid_end + total > size_bytes_) {
            break;  // bloque truncado al final.
        }
        // Descifra (si procede) y valida el CRC32C del batch.
        Buffer plaintext;
        if (const auto loaded = load_block(valid_end, total, plaintext); !loaded) {
            if (loaded.error().code() == ErrorCode::IoError) {
                return std::unexpected(loaded.error());
            }
            break;  // autenticación/tamaño falla: aquí empieza la cola torn.
        }
        const auto batch = RecordBatch::decode(plaintext.as_span());
        if (!batch) {
            break;  // CRC no cuadra / corrupto: aquí empieza la cola torn.
        }
        index_.maybe_append(info.base_offset, static_cast<std::uint32_t>(valid_end), total);
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
        return data_start_;  // frontera del primer batch: se vacía el segmento (conserva cabecera).
    }
    std::size_t position = data_start_;
    while (position < size_bytes_) {
        const auto header = peek_block(position);
        if (!header) {
            return std::unexpected(header.error());
        }
        const std::optional<BlockHeader>& block = *header;
        if (!block.has_value()) {
            break;  // cabecera incompleta: trata el resto como inexistente.
        }
        const BlockHeader info = *block;
        if (info.base_offset == target) {
            return position;
        }
        if (info.base_offset > target) {
            break;  // target cae dentro del batch anterior: no es frontera.
        }
        position += info.on_disk_size;
    }
    return make_error(ErrorCode::InvalidArgument, "truncate_to: el offset no es frontera de batch");
}

expected<void> Segment::rebuild_index() {
    if (const auto cleared = index_.reset(); !cleared) {
        return std::unexpected(cleared.error());
    }
    std::size_t scan = data_start_;
    while (scan < size_bytes_) {
        const auto header = peek_block(scan);
        if (!header) {
            return std::unexpected(header.error());
        }
        const std::optional<BlockHeader>& block = *header;
        if (!block.has_value()) {
            break;
        }
        const BlockHeader info = *block;
        index_.maybe_append(info.base_offset, static_cast<std::uint32_t>(scan), info.on_disk_size);
        scan += info.on_disk_size;
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
