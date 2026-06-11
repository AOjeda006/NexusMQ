#include "common/error.hpp"

#include <gtest/gtest.h>

namespace {

nexus::expected<int> parse(bool ok) {
    if (!ok) {
        return nexus::make_error(nexus::ErrorCode::InvalidArgument, "entrada vacía");
    }
    return 42;
}

TEST(Error, Expected_ConValor_TieneValue) {
    const nexus::expected<int> r = parse(true);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 42);
}

TEST(Error, Expected_ConError_LlevaCodigoYMensaje) {
    const nexus::expected<int> r = parse(false);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), nexus::ErrorCode::InvalidArgument);
    EXPECT_EQ(r.error().message(), "entrada vacía");
}

TEST(Error, WithContext_AnteponeContextoSinMutarElOriginal) {
    const nexus::Error e{nexus::ErrorCode::Corrupt, "crc"};
    const nexus::Error wrapped = e.with_context("segment 3");
    EXPECT_EQ(wrapped.message(), "segment 3: crc");
    EXPECT_EQ(e.message(), "crc");
}

}  // namespace
