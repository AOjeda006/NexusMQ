// Pruebas de OffsetManager: almacén en memoria de offsets confirmados por (grupo, topic,
// partición).
#include "broker/offset_manager.hpp"

#include <gtest/gtest.h>

#include <vector>

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

TEST(OffsetManager, ListForGroup_DevuelveTodosOrdenados) {
    nexus::OffsetManager offsets;
    offsets.commit("g1", "topic-b", 1, 30);
    offsets.commit("g1", "topic-a", 0, 10);
    offsets.commit("g1", "topic-a", 1, 20);
    offsets.commit("g2", "topic-a", 0, 99);  // otro grupo: no debe salir.

    const std::vector<nexus::GroupOffsetEntry> entries = offsets.list_for_group("g1");
    ASSERT_EQ(entries.size(), 3U);
    // Orden determinista por (topic, partición).
    EXPECT_EQ(entries[0].topic, "topic-a");
    EXPECT_EQ(entries[0].partition, 0);
    EXPECT_EQ(entries[0].offset, 10);
    EXPECT_EQ(entries[1].topic, "topic-a");
    EXPECT_EQ(entries[1].partition, 1);
    EXPECT_EQ(entries[1].offset, 20);
    EXPECT_EQ(entries[2].topic, "topic-b");
    EXPECT_EQ(entries[2].partition, 1);
    EXPECT_EQ(entries[2].offset, 30);
}

TEST(OffsetManager, ListForGroup_GrupoSinCommits_DevuelveVacio) {
    nexus::OffsetManager offsets;
    offsets.commit("g1", "t", 0, 1);
    EXPECT_TRUE(offsets.list_for_group("g2").empty());
}

}  // namespace
