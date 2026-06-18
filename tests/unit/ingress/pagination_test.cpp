// Paginación (§7.6): parseo defensivo de page/size desde la query string, con defaults y límites.
#include "ingress/pagination.hpp"

#include <gtest/gtest.h>

#include "common/error.hpp"

namespace {

TEST(Pagination, Defaults_SinParametros) {
    const auto page = nexus::parse_pagination("");
    ASSERT_TRUE(page.has_value());
    EXPECT_EQ(page->number, 1U);
    EXPECT_EQ(page->size, 20U);
    EXPECT_EQ(page->offset(), 0U);
}

TEST(Pagination, ParseaPageYSize) {
    const auto page = nexus::parse_pagination("page=3&size=10");
    ASSERT_TRUE(page.has_value());
    EXPECT_EQ(page->number, 3U);
    EXPECT_EQ(page->size, 10U);
    EXPECT_EQ(page->offset(), 20U);  // (3-1)*10.
}

TEST(Pagination, IgnoraOtrosParametros) {
    const auto page = nexus::parse_pagination("foo=bar&size=5&baz=1");
    ASSERT_TRUE(page.has_value());
    EXPECT_EQ(page->number, 1U);
    EXPECT_EQ(page->size, 5U);
}

TEST(Pagination, PageCero_Rechaza) {
    const auto page = nexus::parse_pagination("page=0");
    ASSERT_FALSE(page.has_value());
    EXPECT_EQ(page.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(Pagination, SizeFueraDeRango_Rechaza) {
    nexus::PaginationLimits limits;
    limits.max_size = 50;
    EXPECT_FALSE(nexus::parse_pagination("size=51", limits).has_value());
    EXPECT_FALSE(nexus::parse_pagination("size=0", limits).has_value());
}

TEST(Pagination, NoNumerico_Rechaza) {
    EXPECT_FALSE(nexus::parse_pagination("page=abc").has_value());
    EXPECT_FALSE(nexus::parse_pagination("size=1x").has_value());
}

}  // namespace
