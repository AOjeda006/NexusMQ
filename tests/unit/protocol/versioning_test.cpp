#include "protocol/versioning.hpp"

#include <gtest/gtest.h>

#include "protocol/frame.hpp"

namespace {

TEST(Negotiate, SolapaDevuelveElMaximoComun) {
    const nexus::ApiVersionRange server{.key = nexus::ApiKey::Produce, .min = 2, .max = 5};
    EXPECT_EQ(nexus::negotiate(7, server), 5);  // limitado por el servidor
    EXPECT_EQ(nexus::negotiate(4, server), 4);  // limitado por el cliente
    EXPECT_EQ(nexus::negotiate(2, server), 2);  // justo el mínimo del servidor
}

TEST(Negotiate, SinSolapeDevuelveCero) {
    const nexus::ApiVersionRange server{.key = nexus::ApiKey::Produce, .min = 3, .max = 5};
    EXPECT_EQ(nexus::negotiate(2, server), 0);  // el cliente no llega al mínimo
}

}  // namespace
