/// @file   io/block_reader.cpp
/// @brief  Implementación de BlockReader (caché LRU de bloques alineados + readahead).
/// @ingroup io

#include "io/block_reader.hpp"

#include <algorithm>
#include <cstring>
#include <utility>

namespace nexus {
namespace {

/// ¿Es @p value una potencia de dos (y no cero)?
[[nodiscard]] bool is_power_of_two(std::size_t value) noexcept {
    return value != 0 && (value & (value - 1)) == 0;
}

}  // namespace

expected<BlockReader> BlockReader::create(File file, std::size_t block_size,
                                          std::size_t cache_blocks, std::size_t readahead_blocks) {
    const std::size_t bsize = (block_size == 0) ? File::direct_alignment() : block_size;
    if (!is_power_of_two(bsize)) {
        return make_error(ErrorCode::InvalidArgument, "block_size no es potencia de dos");
    }
    const expected<std::uint64_t> size = file.size();
    if (!size) {
        return std::unexpected(size.error());
    }
    return BlockReader{std::move(file), bsize, cache_blocks, readahead_blocks, *size};
}

expected<BlockReader::Block> BlockReader::load_block(std::uint64_t index) {
    expected<AlignedBuffer> buf = AlignedBuffer::allocate(block_size_, File::direct_alignment());
    if (!buf) {
        return std::unexpected(buf.error());
    }
    const std::uint64_t offset = index * block_size_;
    const expected<std::size_t> read = file_.read_at(buf->span(), offset);
    if (!read) {
        return std::unexpected(read.error());
    }
    ++disk_reads_;
    return Block{.data = std::move(*buf), .valid = *read};
}

void BlockReader::insert(std::uint64_t index, Block block) {
    if (cache_blocks_ == 0) {
        return;  // caché desactivada: nada que retener.
    }
    if (lru_.size() >= cache_blocks_) {
        // Desaloja el menos usado recientemente (dorso de la lista).
        const std::uint64_t victim = lru_.back().first;
        index_.erase(victim);
        lru_.pop_back();
    }
    lru_.emplace_front(index, std::move(block));
    index_[index] = lru_.begin();
}

expected<const BlockReader::Block*> BlockReader::fetch_block(std::uint64_t index) {
    const auto it = index_.find(index);
    if (it != index_.end()) {
        ++cache_hits_;
        lru_.splice(lru_.begin(), lru_, it->second);  // promueve a más reciente.
        return &it->second->second;
    }
    ++cache_misses_;
    expected<Block> loaded = load_block(index);
    if (!loaded) {
        return std::unexpected(loaded.error());
    }
    if (cache_blocks_ == 0) {
        // Sin caché no podemos retener el bloque; este camino no se usa cuando hay caché.
        return make_error(ErrorCode::Unsupported, "BlockReader sin caché no soporta fetch_block");
    }
    insert(index, std::move(*loaded));
    return &lru_.begin()->second;
}

void BlockReader::prefetch(std::uint64_t first, std::size_t count) {
    if (cache_blocks_ == 0 || count == 0) {
        return;
    }
    const std::uint64_t last_block = (file_size_ == 0) ? 0 : (file_size_ - 1) / block_size_;
    for (std::size_t i = 0; i < count; ++i) {
        const std::uint64_t idx = first + i;
        if (idx > last_block) {
            break;  // más allá del EOF: nada que precargar.
        }
        if (index_.contains(idx)) {
            continue;  // ya en caché.
        }
        expected<Block> loaded = load_block(idx);
        if (!loaded) {
            return;  // best-effort: un fallo de precarga no es fatal.
        }
        insert(idx, std::move(*loaded));
    }
}

expected<std::size_t> BlockReader::read_at(MutByteSpan dst, std::uint64_t offset) {
    if (dst.empty()) {
        return std::size_t{0};
    }
    // Si la lectura roza el final conocido, re-consulta el tamaño (el fichero pudo crecer).
    if (offset + dst.size() > file_size_) {
        const expected<std::uint64_t> size = file_.size();
        if (!size) {
            return std::unexpected(size.error());
        }
        file_size_ = *size;
    }
    if (offset >= file_size_) {
        return std::size_t{0};  // EOF.
    }

    const std::uint64_t end = std::min<std::uint64_t>(offset + dst.size(), file_size_);
    const std::uint64_t first_block = offset / block_size_;
    const std::uint64_t last_block = (end - 1) / block_size_;

    // Readahead: si este acceso continúa la secuencia, precarga los bloques siguientes.
    if (have_seq_ && first_block == next_sequential_ && readahead_blocks_ > 0) {
        prefetch(last_block + 1, readahead_blocks_);
    }

    std::size_t copied = 0;
    for (std::uint64_t blk = first_block; blk <= last_block; ++blk) {
        // Sin caché, sirve el bloque desde un búfer local sin retenerlo.
        Block local;
        const Block* block = nullptr;
        if (cache_blocks_ == 0) {
            expected<Block> loaded = load_block(blk);
            if (!loaded) {
                return std::unexpected(loaded.error());
            }
            local = std::move(*loaded);
            block = &local;
        } else {
            const expected<const Block*> fetched = fetch_block(blk);
            if (!fetched) {
                return std::unexpected(fetched.error());
            }
            block = *fetched;
        }

        const std::uint64_t block_start = blk * block_size_;
        const std::uint64_t copy_from = std::max<std::uint64_t>(offset, block_start);
        const std::uint64_t block_end = block_start + block->valid;
        const std::uint64_t copy_to = std::min<std::uint64_t>(end, block_end);
        if (copy_to <= copy_from) {
            continue;  // el bloque no aporta bytes válidos al rango pedido.
        }
        const auto n = static_cast<std::size_t>(copy_to - copy_from);
        const auto src_off = static_cast<std::size_t>(copy_from - block_start);
        std::memcpy(dst.data() + copied, block->data.data() + src_off, n);
        copied += n;
    }

    next_sequential_ = last_block + 1;
    have_seq_ = true;
    return copied;
}

}  // namespace nexus
