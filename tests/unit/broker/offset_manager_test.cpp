// Pruebas de OffsetManager: almacén en memoria de offsets confirmados por (grupo, topic,
// partición).
#include "broker/offset_manager.hpp"

#include <gtest/gtest.h>

#include "common/error.hpp"

namespace {

TEST(OffsetManager, Fetch_SinCommitPrevio_DevuelveNotFound) {
    const nexus::OffsetManager offsets;
    const nexus::expected<nexus::Offset> result = offsets.fetch("g", "t", 0);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::NotFound);
}

TEST(OffsetManager, CommitLuegoFetch_DevuelveElOffset) {
    nexus::OffsetManager offsets;
    offsets.commit("g", "t", 0, 42);
    const nexus::expected<nexus::Offset> result = offsets.fetch("g", "t", 0);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 42);
    EXPECT_EQ(offsets.size(), 1U);
}

TEST(OffsetManager, Commit_SobrescribeElAnterior) {
    nexus::OffsetManager offsets;
    offsets.commit("g", "t", 0, 10);
    offsets.commit("g", "t", 0, 99);
    const nexus::expected<nexus::Offset> result = offsets.fetch("g", "t", 0);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 99);
    EXPECT_EQ(offsets.size(), 1U);  // misma clave: no crece.
}

TEST(OffsetManager, ClavesDistintas_SonIndependientes) {
    nexus::OffsetManager offsets;
    offsets.commit("g1", "t", 0, 1);
    offsets.commit("g2", "t", 0, 2);
    offsets.commit("g1", "t", 1, 3);
    offsets.commit("g1", "otro", 0, 4);

    EXPECT_EQ(offsets.fetch("g1", "t", 0).value(), 1);
    EXPECT_EQ(offsets.fetch("g2", "t", 0).value(), 2);
    EXPECT_EQ(offsets.fetch("g1", "t", 1).value(), 3);
    EXPECT_EQ(offsets.fetch("g1", "otro", 0).value(), 4);
    EXPECT_EQ(offsets.size(), 4U);
}

}  // namespace
