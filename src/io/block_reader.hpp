/// @file   io/block_reader.hpp
/// @brief  BlockReader: lectura por bloques alineados con caché LRU y readahead (F6b).
/// @ingroup io

#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <unordered_map>
#include <utility>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "io/aligned_buffer.hpp"
#include "io/file.hpp"

namespace nexus {

/// @brief Lector por **bloques alineados** sobre un `File`, con caché LRU y *readahead* propios.
///   Afinidad: REACTOR-LOCAL.
/// @details Pensado para acompañar a la E/S directa (`O_DIRECT`, F6a): al saltarse la *page cache*
///   del SO, el broker gestiona su **propia** caché de bloques y prefetch. Lee siempre en bloques
///   de `block_size` (potencia de dos, ≥ `File::direct_alignment()` para el modo directo) en
///   offsets alineados; sirve cualquier rango `[offset, len)` copiando de los bloques que lo
///   cubren. Un acceso **secuencial** dispara la precarga de los siguientes `readahead_blocks`
///   bloques. La caché evita lecturas a disco repetidas (LRU acotada a `cache_blocks`). No es
///   thread-safe.
class BlockReader {
public:
    /// @brief Crea un lector que toma posesión de @p file.
    /// @param block_size Tamaño de bloque (potencia de dos); 0 ⇒ `File::direct_alignment()`.
    /// @param cache_blocks Bloques máximos en caché (LRU); 0 ⇒ sin caché (cada lectura va a disco).
    /// @param readahead_blocks Bloques a precargar tras un acceso secuencial (0 ⇒ sin readahead).
    [[nodiscard]] static expected<BlockReader> create(File file, std::size_t block_size = 0,
                                                      std::size_t cache_blocks = 32,
                                                      std::size_t readahead_blocks = 4);

    /// @brief Lee en @p dst los bytes a partir de @p offset; devuelve los leídos (corto al EOF).
    [[nodiscard]] expected<std::size_t> read_at(MutByteSpan dst, std::uint64_t offset);

    [[nodiscard]] std::size_t block_size() const noexcept { return block_size_; }

    /// Estadísticas (para tests/observabilidad): aciertos/fallos de caché y lecturas a disco.
    [[nodiscard]] std::uint64_t cache_hits() const noexcept { return cache_hits_; }
    [[nodiscard]] std::uint64_t cache_misses() const noexcept { return cache_misses_; }
    [[nodiscard]] std::uint64_t disk_reads() const noexcept { return disk_reads_; }

private:
    /// Un bloque en caché: sus bytes alineados y cuántos son válidos (el último puede ir corto).
    struct Block {
        AlignedBuffer data;     ///< Búfer alineado de `block_size` bytes.
        std::size_t valid = 0;  ///< Bytes válidos (≤ block_size; < block_size solo al EOF).
    };
    using BlockList = std::list<std::pair<std::uint64_t, Block>>;  // (índice de bloque, bloque)

    BlockReader(File file, std::size_t block_size, std::size_t cache_blocks,
                std::size_t readahead_blocks, std::uint64_t file_size) noexcept
        : file_(std::move(file)),
          block_size_(block_size),
          cache_blocks_(cache_blocks),
          readahead_blocks_(readahead_blocks),
          file_size_(file_size) {}

    /// Devuelve (o carga) el bloque @p index, dejándolo como más reciente en la LRU.
    [[nodiscard]] expected<const Block*> fetch_block(std::uint64_t index);
    /// Carga el bloque @p index desde disco (lectura alineada).
    [[nodiscard]] expected<Block> load_block(std::uint64_t index);
    /// Precarga (best-effort) los bloques [first, first+count) que no estén ya en caché.
    void prefetch(std::uint64_t first, std::size_t count);
    /// Inserta @p block en la caché con clave @p index, desalojando el LRU si hace falta.
    void insert(std::uint64_t index, Block block);

    File file_;
    std::size_t block_size_ = 0;
    std::size_t cache_blocks_ = 0;
    std::size_t readahead_blocks_ = 0;
    std::uint64_t file_size_ = 0;

    BlockList lru_;  ///< Frente = más reciente; dorso = candidato a desalojo.
    std::unordered_map<std::uint64_t, BlockList::iterator> index_;

    std::uint64_t next_sequential_ = 0;  ///< Próximo bloque esperado si el acceso es secuencial.
    bool have_seq_ = false;              ///< ¿Hay una posición secuencial establecida?

    std::uint64_t cache_hits_ = 0;
    std::uint64_t cache_misses_ = 0;
    std::uint64_t disk_reads_ = 0;
};

}  // namespace nexus
