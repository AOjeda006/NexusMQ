#include "io/file.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <filesystem>
#include <string>

#include "common/error.hpp"

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

}  // namespace
