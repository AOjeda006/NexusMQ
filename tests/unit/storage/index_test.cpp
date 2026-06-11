#include "storage/index.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "io/file.hpp"

namespace {

std::string temp_path(const char* tag) {
    return (std::filesystem::temp_directory_path() /
            ("nexus_index_" + std::string{tag} + "_" +
             std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + ".index"))
        .string();
}

// Siembra entradas con intervalo 10 y batches de 10 bytes (base 0). Produce anclas en
// {5,10}, {10,20}, {15,30} de forma determinista (ver maybe_append).
nexus::SparseIndex seeded_index(const std::string& path) {
    auto idx = nexus::SparseIndex::open(path, /*base_offset=*/0, /*index_interval_bytes=*/10);
    EXPECT_TRUE(idx.has_value());
    idx->maybe_append(0, 0, 10);    // bytes_since_ 0 -> no ancla; queda 10
    idx->maybe_append(5, 10, 10);   // 10 >= 10 -> ancla {5,10}
    idx->maybe_append(10, 20, 10);  // ancla {10,20}
    idx->maybe_append(15, 30, 10);  // ancla {15,30}
    return std::move(*idx);
}

TEST(SparseIndex, Floor_IndiceVacio_DevuelveInicio) {
    const std::string path = temp_path("vacio");
    auto idx = nexus::SparseIndex::open(path, 0, 4096);
    ASSERT_TRUE(idx.has_value());
    EXPECT_TRUE(idx->empty());
    const nexus::IndexEntry e = idx->floor(42);
    EXPECT_EQ(e.relative_offset, 0U);
    EXPECT_EQ(e.file_position, 0U);
    std::filesystem::remove(path);
}

TEST(SparseIndex, MaybeAppend_SiembraCadaIntervalo) {
    const std::string path = temp_path("siembra");
    const auto idx = seeded_index(path);
    EXPECT_EQ(idx.size(), 3U);  // {5,10},{10,20},{15,30}
    std::filesystem::remove(path);
}

TEST(SparseIndex, Floor_AntesDePrimeraAncla_DevuelveInicio) {
    const std::string path = temp_path("antes");
    const auto idx = seeded_index(path);
    // offsets 0..4 caen antes de la primera ancla (rel 5) -> inicio del segmento.
    EXPECT_EQ(idx.floor(0).file_position, 0U);
    EXPECT_EQ(idx.floor(4).file_position, 0U);
    std::filesystem::remove(path);
}

TEST(SparseIndex, Floor_BusquedaBinaria_DevuelveMayorAnclaMenorOIgual) {
    const std::string path = temp_path("floor");
    const auto idx = seeded_index(path);
    EXPECT_EQ(idx.floor(5).file_position, 10U);     // ancla exacta {5,10}
    EXPECT_EQ(idx.floor(9).file_position, 10U);     // entre {5} y {10} -> {5,10}
    EXPECT_EQ(idx.floor(10).file_position, 20U);    // ancla exacta {10,20}
    EXPECT_EQ(idx.floor(14).file_position, 20U);    // entre {10} y {15} -> {10,20}
    EXPECT_EQ(idx.floor(1000).file_position, 30U);  // más allá de la última -> {15,30}
    std::filesystem::remove(path);
}

TEST(SparseIndex, FlushYReopen_PreservaEntradas) {
    const std::string path = temp_path("reopen");
    {
        auto idx = seeded_index(path);
        ASSERT_TRUE(idx.flush().has_value());
    }
    auto reloaded = nexus::SparseIndex::open(path, 0, 10);
    ASSERT_TRUE(reloaded.has_value());
    EXPECT_EQ(reloaded->size(), 3U);
    EXPECT_EQ(reloaded->floor(9).file_position, 10U);
    EXPECT_EQ(reloaded->floor(1000).file_position, 30U);
    std::filesystem::remove(path);
}

TEST(SparseIndex, Open_NoEstrictamenteCreciente_DevuelveCorrupt) {
    const std::string path = temp_path("corrupto");
    {
        auto file = nexus::File::open(path, nexus::File::Mode::ReadWrite);
        ASSERT_TRUE(file.has_value());
        // Dos entradas con relative_offset descendente (5, luego 3): viola el invariante.
        std::array<std::byte, 2 * nexus::SparseIndex::kEntrySize> raw{};
        const nexus::MutByteSpan span{raw};
        nexus::store_le<std::uint32_t>(5, span.subspan(0));
        nexus::store_le<std::uint32_t>(0, span.subspan(4));
        nexus::store_le<std::uint32_t>(3, span.subspan(8));
        nexus::store_le<std::uint32_t>(8, span.subspan(12));
        ASSERT_TRUE(file->write_at(raw, 0).has_value());
        ASSERT_TRUE(file->sync().has_value());
    }
    const auto idx = nexus::SparseIndex::open(path, 0, 10);
    ASSERT_FALSE(idx.has_value());
    EXPECT_EQ(idx.error().code(), nexus::ErrorCode::Corrupt);
    std::filesystem::remove(path);
}

}  // namespace
