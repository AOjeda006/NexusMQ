// FNV-1a de 64 bits: hash determinista y estable entre plataformas. Verifica los vectores de
// referencia conocidos (para no desviarse del estándar), que es `constexpr` y que discrimina.
#include "common/fnv1a.hpp"

#include <gtest/gtest.h>

#include <cstdint>

namespace {

TEST(Fnv1a64, CadenaVacia_DevuelveOffsetBasis) {
    EXPECT_EQ(nexus::fnv1a_64(""), 14695981039346656037ULL);  // offset basis FNV-1a 64.
}

TEST(Fnv1a64, VectoresConocidos_CoincidenConLaReferencia) {
    // Vectores canónicos de FNV-1a 64 (Landon Curt Noll): garantizan compatibilidad con el
    // estándar.
    EXPECT_EQ(nexus::fnv1a_64("a"), 0xaf63dc4c8601ec8cULL);
    EXPECT_EQ(nexus::fnv1a_64("foobar"), 0x85944171f73967e8ULL);
}

TEST(Fnv1a64, EsConstexpr_SeEvaluaEnCompilacion) {
    static_assert(nexus::fnv1a_64("a") == 0xaf63dc4c8601ec8cULL);
    SUCCEED();
}

TEST(Fnv1a64, EntradasDistintas_ProducenHashesDistintos) {
    EXPECT_NE(nexus::fnv1a_64("group-1"), nexus::fnv1a_64("group-2"));
}

}  // namespace
