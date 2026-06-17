// LoadBalancer (§6.4): tres estrategias deterministas. Round-robin cicla en orden;
// least-connections elige el de menos conexiones activas (desempata por menor id);
// consistent-hashing mapea cada clave de forma estable, mantiene la clave al quitar un nodo que no
// es su dueño y la reubica si se quita su dueño, y reparte muchas claves entre varios nodos.
#include "ingress/load_balancer.hpp"

#include <gtest/gtest.h>

#include <set>
#include <string>

namespace {

TEST(LoadBalancer, Pick_ConjuntoVacio_DevuelveNullopt) {
    nexus::LoadBalancer lb(nexus::BalanceStrategy::RoundRobin);
    EXPECT_FALSE(lb.pick().has_value());
}

TEST(LoadBalancer, RoundRobin_CiclaEnOrdenEstable) {
    nexus::LoadBalancer lb(nexus::BalanceStrategy::RoundRobin);
    lb.add_node(3);
    lb.add_node(1);
    lb.add_node(2);  // se ordenan: 1, 2, 3.
    EXPECT_EQ(lb.pick(), 1);
    EXPECT_EQ(lb.pick(), 2);
    EXPECT_EQ(lb.pick(), 3);
    EXPECT_EQ(lb.pick(), 1);  // cicla.
}

TEST(LoadBalancer, LeastConnections_EligeElDeMenosActivas) {
    nexus::LoadBalancer lb(nexus::BalanceStrategy::LeastConnections);
    lb.add_node(1);
    lb.add_node(2);
    lb.add_node(3);
    lb.on_acquire(1);
    lb.on_acquire(1);
    lb.on_acquire(2);  // activas: 1→2, 2→1, 3→0.
    EXPECT_EQ(lb.pick(), 3);
    lb.on_acquire(3);
    lb.on_acquire(3);  // 3→2; mínimo ahora es 2 (1 activa).
    EXPECT_EQ(lb.pick(), 2);
    EXPECT_EQ(lb.active(1), 2U);
}

TEST(LoadBalancer, LeastConnections_Release_BajaElRecuento) {
    nexus::LoadBalancer lb(nexus::BalanceStrategy::LeastConnections);
    lb.add_node(1);
    lb.on_acquire(1);
    lb.on_release(1);
    lb.on_release(1);  // no baja de cero.
    EXPECT_EQ(lb.active(1), 0U);
}

TEST(LoadBalancer, ConsistentHashing_MismaClaveMismoNodo) {
    nexus::LoadBalancer lb(nexus::BalanceStrategy::ConsistentHashing);
    for (nexus::NodeId id : {1, 2, 3, 4, 5}) {
        lb.add_node(id);
    }
    const auto first = lb.pick("topic-a:7");
    ASSERT_TRUE(first.has_value());
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(lb.pick("topic-a:7"), first);  // estable.
    }
}

TEST(LoadBalancer, ConsistentHashing_QuitarNoDuenyo_MantieneLaClave) {
    nexus::LoadBalancer lb(nexus::BalanceStrategy::ConsistentHashing);
    for (nexus::NodeId id : {1, 2, 3, 4, 5}) {
        lb.add_node(id);
    }
    const nexus::NodeId owner = *lb.pick("clave-estable");
    // Quita un nodo que NO es el dueño: la clave debe seguir mapeando al mismo nodo.
    const nexus::NodeId other = (owner == 1) ? 2 : 1;
    lb.remove_node(other);
    EXPECT_EQ(lb.pick("clave-estable"), owner);
}

TEST(LoadBalancer, ConsistentHashing_QuitarDuenyo_ReubicaLaClave) {
    nexus::LoadBalancer lb(nexus::BalanceStrategy::ConsistentHashing);
    for (nexus::NodeId id : {1, 2, 3, 4, 5}) {
        lb.add_node(id);
    }
    const nexus::NodeId owner = *lb.pick("otra-clave");
    lb.remove_node(owner);
    const auto reassigned = lb.pick("otra-clave");
    ASSERT_TRUE(reassigned.has_value());
    EXPECT_NE(*reassigned, owner);  // ya no existe: la sirve otro nodo.
}

TEST(LoadBalancer, ConsistentHashing_ReparteClavesEntreVariosNodos) {
    nexus::LoadBalancer lb(nexus::BalanceStrategy::ConsistentHashing);
    for (nexus::NodeId id : {1, 2, 3}) {
        lb.add_node(id);
    }
    std::set<nexus::NodeId> seen;
    for (int i = 0; i < 200; ++i) {
        seen.insert(*lb.pick("key-" + std::to_string(i)));
    }
    EXPECT_GT(seen.size(), 1U) << "el reparto debe tocar más de un nodo";
}

}  // namespace
