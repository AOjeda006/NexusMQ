#include "common/version.hpp"

#include <gtest/gtest.h>

// Patrón de nombre: Metodo_Escenario_ResultadoEsperado.
TEST(Version, SinArgumentos_DevuelveCadenaNoVacia) {
  EXPECT_FALSE(nexus::version().empty());
}

TEST(Version, SinArgumentos_CoincideConLaVersionDeCMake) {
  EXPECT_EQ(nexus::version(), "0.1.0");
}
