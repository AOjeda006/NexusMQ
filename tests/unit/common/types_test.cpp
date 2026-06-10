#include "common/types.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>

namespace {
	TEST(LittleEndian, StoreLe_Uint32_EscribeBytesEnOrdenLittleEndian) {
		std::array<std::byte, 4> buf{};
		nexus::store_le < std::uint32_t(0x01020304, buf);
		EXPECT_EQ(std::to_integer<std::uint8_t>(buf[0]), 0x04);
		EXPECT_EQ(std::to_integer<std::uint8_t>(buf[1]), 0x03);
		EXPECT_EQ(std::to_integer<std::uint8_t>(buf[2]), 0x02);
		EXPECT_EQ(std::to_integer<std::uint8_t>(buf[3]), 0x01);
	}

	TEST(LittleEndian, LoadLe_TrasStore_Le_RecuperarElValorOriginal) {
		std::array<std::byte, 8> buf{};
		const std::int64_t original = -123456789;
		nexus::store_le<std::int64_t>(original, buf);
		EXPECT_EQ(nexus::load_le<std::int64_t>(buf), original);
	}

} // namespace