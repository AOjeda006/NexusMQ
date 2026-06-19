// Pruebas de BlockReader: lectura por bloques alineados con caché LRU y readahead (F6b).
#include "io/block_reader.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "io/file.hpp"

namespace {

using nexus::BlockReader;
using nexus::File;

std::string temp_path() {
    return (std::filesystem::temp_directory_path() /
            ("nexus_block_reader_" +
             std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + ".tmp"))
        .string();
}

// Escribe @p size bytes con un patrón determinista (byte i = i & 0xFF) y devuelve la ruta.
std::vector<std::byte> write_pattern(const std::string& path, std::size_t size) {
    std::vector<std::byte> content(size);
    for (std::size_t i = 0; i < size; ++i) {
        content[i] = static_cast<std::byte>(i & 0xFFU);
    }
    auto file = File::open(path, File::Mode::ReadWrite);
    EXPECT_TRUE(file.has_value());
    EXPECT_TRUE(file->write_at(nexus::ByteSpan{content}, 0).has_value());
    EXPECT_TRUE(file->sync().has_value());
    return content;
}

// Lee [offset, offset+len) vía BlockReader y lo devuelve como vector (recortado a lo leído).
std::vector<std::byte> read_range(BlockReader& reader, std::uint64_t offset, std::size_t len) {
    std::vector<std::byte> out(len);
    const auto n = reader.read_at(nexus::MutByteSpan{out}, offset);
    EXPECT_TRUE(n.has_value());
    out.resize(n.value_or(0));
    return out;
}

TEST(BlockReader, LeeRangoQueCruzaVariosBloques) {
    const std::string path = temp_path();
    const std::vector<std::byte> content = write_pattern(path, 4000);

    auto file = File::open(path, File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());
    auto reader = BlockReader::create(std::move(*file), /*block_size=*/512);
    ASSERT_TRUE(reader.has_value());

    // Rango que empieza a mitad de un bloque y termina en otro distinto.
    const std::vector<std::byte> got = read_range(*reader, 500, 1100);
    ASSERT_EQ(got.size(), 1100U);
    const std::vector<std::byte> expected(content.begin() + 500, content.begin() + 1600);
    EXPECT_EQ(got, expected);

    std::filesystem::remove(path);
}

TEST(BlockReader, ReleeMismoBloque_AciertaEnCache) {
    const std::string path = temp_path();
    write_pattern(path, 2048);
    auto file = File::open(path, File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());
    auto reader = BlockReader::create(std::move(*file), 512, /*cache_blocks=*/8,
                                      /*readahead_blocks=*/0);
    ASSERT_TRUE(reader.has_value());

    (void)read_range(*reader, 0, 100);  // bloque 0: miss → disco
    const std::uint64_t disk_after_first = reader->disk_reads();
    EXPECT_EQ(reader->cache_misses(), 1U);

    (void)read_range(*reader, 100, 100);  // mismo bloque 0: hit, sin disco
    EXPECT_EQ(reader->disk_reads(), disk_after_first);
    EXPECT_EQ(reader->cache_hits(), 1U);

    std::filesystem::remove(path);
}

TEST(BlockReader, ReadaheadPrecargaBloquesSiguientes) {
    const std::string path = temp_path();
    write_pattern(path, 4096);  // 8 bloques de 512
    auto file = File::open(path, File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());
    auto reader = BlockReader::create(std::move(*file), 512, /*cache_blocks=*/16,
                                      /*readahead_blocks=*/3);
    ASSERT_TRUE(reader.has_value());

    (void)read_range(*reader, 0, 512);    // bloque 0 (miss). No hay secuencia previa: sin prefetch.
    (void)read_range(*reader, 512, 512);  // bloque 1: continúa la secuencia → precarga 2,3,4.
    // Leer el bloque 2 ahora debe ser un acierto de caché (lo precargó el readahead).
    const std::uint64_t hits_before = reader->cache_hits();
    (void)read_range(*reader, 1024, 512);  // bloque 2
    EXPECT_EQ(reader->cache_hits(), hits_before + 1) << "el bloque 2 venía precargado";

    std::filesystem::remove(path);
}

TEST(BlockReader, DesalojaLruCuandoSeLlena) {
    const std::string path = temp_path();
    write_pattern(path, 4096);  // 8 bloques de 512
    auto file = File::open(path, File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());
    auto reader = BlockReader::create(std::move(*file), 512, /*cache_blocks=*/2,
                                      /*readahead_blocks=*/0);
    ASSERT_TRUE(reader.has_value());

    (void)read_range(*reader, 0, 100);     // bloque 0 (miss)
    (void)read_range(*reader, 512, 100);   // bloque 1 (miss); caché = {1,0}
    (void)read_range(*reader, 1024, 100);  // bloque 2 (miss); desaloja 0 → caché = {2,1}
    const std::uint64_t disk_before = reader->disk_reads();
    (void)read_range(*reader, 0, 100);  // bloque 0 fue desalojado → miss → disco
    EXPECT_EQ(reader->disk_reads(), disk_before + 1);

    std::filesystem::remove(path);
}

TEST(BlockReader, UltimoBloqueParcial_LeeHastaElEof) {
    const std::string path = temp_path();
    const std::vector<std::byte> content = write_pattern(path, 700);  // 1 bloque + 188 bytes
    auto file = File::open(path, File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());
    auto reader = BlockReader::create(std::move(*file), 512);
    ASSERT_TRUE(reader.has_value());

    const std::vector<std::byte> got = read_range(*reader, 600, 512);  // pide 512, solo hay 100
    ASSERT_EQ(got.size(), 100U);
    const std::vector<std::byte> expected(content.begin() + 600, content.end());
    EXPECT_EQ(got, expected);

    std::filesystem::remove(path);
}

TEST(BlockReader, LeerMasAllaDelEof_DevuelveCero) {
    const std::string path = temp_path();
    write_pattern(path, 256);
    auto file = File::open(path, File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());
    auto reader = BlockReader::create(std::move(*file), 512);
    ASSERT_TRUE(reader.has_value());

    const std::vector<std::byte> got = read_range(*reader, 1000, 128);
    EXPECT_TRUE(got.empty());

    std::filesystem::remove(path);
}

TEST(BlockReader, BlockSizeNoPotenciaDeDos_EsInvalidArgument) {
    const std::string path = temp_path();
    write_pattern(path, 256);
    auto file = File::open(path, File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());
    auto reader = BlockReader::create(std::move(*file), /*block_size=*/300);
    ASSERT_FALSE(reader.has_value());
    EXPECT_EQ(reader.error().code(), nexus::ErrorCode::InvalidArgument);
    std::filesystem::remove(path);
}

}  // namespace
