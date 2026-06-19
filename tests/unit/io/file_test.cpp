#include "io/file.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <filesystem>
#include <string>

#include "common/error.hpp"
#include "io/aligned_buffer.hpp"

namespace {

std::string temp_path() {
    return (std::filesystem::temp_directory_path() /
            ("nexus_file_test_" +
             std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + ".tmp"))
        .string();
}

TEST(File, WriteSyncRead_RoundTrip) {
    const std::string path = temp_path();
    auto file = nexus::File::open(path, nexus::File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());

    const std::array<std::byte, 4> data{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE},
                                        std::byte{0xEF}};
    ASSERT_TRUE(file->write_at(data, 0).has_value());
    ASSERT_TRUE(file->sync().has_value());

    const auto size = file->size();
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(*size, 4U);

    std::array<std::byte, 4> buf{};
    const auto read = file->read_at(buf, 0);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, 4U);
    EXPECT_EQ(buf, data);

    std::filesystem::remove(path);
}

TEST(File, Truncate_ReduceElTamano) {
    const std::string path = temp_path();
    auto file = nexus::File::open(path, nexus::File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());
    const std::array<std::byte, 8> data{};
    ASSERT_TRUE(file->write_at(data, 0).has_value());
    ASSERT_TRUE(file->truncate(3).has_value());
    const auto size = file->size();
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(*size, 3U);
    std::filesystem::remove(path);
}

TEST(File, ReadMasAllaDelFinal_DevuelveCeroBytes) {
    const std::string path = temp_path();
    auto file = nexus::File::open(path, nexus::File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());
    std::array<std::byte, 8> buf{};
    const auto read = file->read_at(buf, 0);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, 0U);  // fichero vacío -> EOF inmediato
    std::filesystem::remove(path);
}

TEST(File, Open_Inexistente_EnSoloLectura_DevuelveIoError) {
    const auto file = nexus::File::open("/no/existe/jamas.tmp", nexus::File::Mode::ReadOnly);
    ASSERT_FALSE(file.has_value());
    EXPECT_EQ(file.error().code(), nexus::ErrorCode::IoError);
}

TEST(File, ReadWrite_NoEsDirecto) {
    const std::string path = temp_path();
    auto file = nexus::File::open(path, nexus::File::Mode::ReadWrite);
    ASSERT_TRUE(file.has_value());
    EXPECT_FALSE(file->is_direct());
    std::filesystem::remove(path);
}

// E/S directa: abre con O_DIRECT (o recae a búfer si el FS no lo admite) y hace un round-trip
// con offset/longitud/búfer alineados. Funciona en ambos modos (4096 alineado vale también
// para E/S con búfer); is_direct() solo indica si O_DIRECT quedó activo.
TEST(File, ReadWriteDirect_RoundTripAlineado) {
    const std::string path = temp_path();
    auto file = nexus::File::open(path, nexus::File::Mode::ReadWriteDirect);
    ASSERT_TRUE(file.has_value());

    const std::size_t block = nexus::File::direct_alignment();
    auto write_buf = nexus::AlignedBuffer::allocate(block);
    ASSERT_TRUE(write_buf.has_value());
    const nexus::MutByteSpan wspan = write_buf->span();
    for (std::size_t i = 0; i < wspan.size(); ++i) {
        wspan[i] = static_cast<std::byte>(i & 0xFFU);
    }
    ASSERT_TRUE(file->write_at(write_buf->span(), 0).has_value());
    ASSERT_TRUE(file->sync().has_value());

    auto read_buf = nexus::AlignedBuffer::allocate(block);
    ASSERT_TRUE(read_buf.has_value());
    const auto read = file->read_at(read_buf->span(), 0);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, block);
    EXPECT_EQ(read_buf->span()[0], std::byte{0x00});
    EXPECT_EQ(read_buf->span()[255], std::byte{0xFF});

    std::filesystem::remove(path);
}

}  // namespace
