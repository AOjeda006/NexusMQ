// Pruebas de PeerDirectory: directorio inmutable NodeId -> dirección inter-nodo.
#include "cluster/peer_directory.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"

namespace {

nexus::PeerDirectory make_directory() {
    std::unordered_map<nexus::NodeId, nexus::PeerAddress> peers;
    peers.emplace(2, nexus::PeerAddress{.host = "10.0.0.2", .port = 9300});
    peers.emplace(3, nexus::PeerAddress{.host = "10.0.0.3", .port = 9301});
    return nexus::PeerDirectory{std::move(peers)};
}

TEST(PeerDirectory, Find_NodoPresente_DevuelveSuDireccion) {
    const nexus::PeerDirectory directory = make_directory();

    const nexus::PeerAddress* address = directory.find(2);

    ASSERT_NE(address, nullptr);
    EXPECT_EQ(address->host, "10.0.0.2");
    EXPECT_EQ(address->port, 9300);
}

TEST(PeerDirectory, Find_NodoAusente_DevuelveNullptr) {
    const nexus::PeerDirectory directory = make_directory();

    EXPECT_EQ(directory.find(99), nullptr);
}

TEST(PeerDirectory, Contains_DistingueePresenteDeAusente) {
    const nexus::PeerDirectory directory = make_directory();

    EXPECT_TRUE(directory.contains(3));
    EXPECT_FALSE(directory.contains(1));
}

TEST(PeerDirectory, NodeIds_DevuelveLosNodosOrdenadosAscendente) {
    const nexus::PeerDirectory directory = make_directory();

    EXPECT_EQ(directory.node_ids(), (std::vector<nexus::NodeId>{2, 3}));
}

TEST(PeerDirectory, PorDefecto_EstaVacio) {
    const nexus::PeerDirectory directory;

    EXPECT_TRUE(directory.empty());
    EXPECT_EQ(directory.size(), 0U);
    EXPECT_TRUE(directory.node_ids().empty());
}

TEST(PeerDirectory, Size_CuentaLosPeersRegistrados) {
    const nexus::PeerDirectory directory = make_directory();

    EXPECT_FALSE(directory.empty());
    EXPECT_EQ(directory.size(), 2U);
}

}  // namespace
