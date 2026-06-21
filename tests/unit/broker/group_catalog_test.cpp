// GroupCatalog: un GroupShard (coordinador de grupos + offsets) por núcleo (sharding ADR-0026).
// Verifica el recuento de núcleos, el clamp, la independencia de shards y los punteros del cableado.
#include "broker/group_catalog.hpp"

#include <gtest/gtest.h>

#include <vector>

namespace {

TEST(GroupCatalog, CoreCount_ReflejaElNumeroDeNucleos) {
    const nexus::GroupCatalog cat{3};
    EXPECT_EQ(cat.core_count(), 3);
}

TEST(GroupCatalog, NumCoresInvalido_SeTrataComoUno) {
    EXPECT_EQ(nexus::GroupCatalog{0}.core_count(), 1);
    EXPECT_EQ(nexus::GroupCatalog{-4}.core_count(), 1);
}

TEST(GroupCatalog, ShardsIndependientes_PorNucleo) {
    nexus::GroupCatalog cat{2};
    cat.offsets(0).commit("g", "t", 0, 5);
    EXPECT_EQ(cat.offsets(0).size(), 1U);
    EXPECT_EQ(cat.offsets(1).size(), 0U);  // el shard del núcleo 1 queda intacto.
}

TEST(GroupCatalog, AllGroupsYAllOffsets_DevuelvenUnPunteroPorNucleo) {
    nexus::GroupCatalog cat{2};
    const std::vector<nexus::GroupCoordinator*> groups = cat.all_groups();
    const std::vector<nexus::OffsetManager*> offsets = cat.all_offsets();
    ASSERT_EQ(groups.size(), 2U);
    ASSERT_EQ(offsets.size(), 2U);
    EXPECT_EQ(groups[0], &cat.groups(0));
    EXPECT_EQ(groups[1], &cat.groups(1));
    EXPECT_EQ(offsets[0], &cat.offsets(0));
    EXPECT_EQ(offsets[1], &cat.offsets(1));
}

}  // namespace
