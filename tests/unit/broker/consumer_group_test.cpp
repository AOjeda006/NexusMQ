// Pruebas de ConsumerGroup: FSM de membresía (rebalanceo generacional eager) con reloj inyectado.
// Cubre alta/lider, reparto del lider en sync, rebalanceo al entrar/salir miembros, heartbeat
// (Ok/RebalanceInProgress), expiración de sesión por tick y validación de generación.
#include "broker/consumer_group.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <initializer_list>
#include <vector>

#include "common/error.hpp"

namespace {

using namespace std::chrono_literals;

std::vector<std::byte> bytes(std::initializer_list<int> values) {
    std::vector<std::byte> out;
    out.reserve(values.size());
    for (const int value : values) {
        out.push_back(static_cast<std::byte>(value));
    }
    return out;
}

// Lleva el grupo a Stable con dos miembros (grp-1 seguidor, grp-2 líder de la gen 2). Deja a ambos
// con `last_seen == now`. Devuelve nada; los ids son deterministas ("<group>-<n>").
void make_stable_pair(nexus::ConsumerGroup& group, nexus::MonoTime now) {
    ASSERT_TRUE(group.join(now, "", {}, 30s).has_value());                  // grp-1: líder gen 1.
    ASSERT_TRUE(group.sync(now, "grp-1", 1, {{"grp-1", {}}}).has_value());  // -> Stable gen 1.
    ASSERT_TRUE(group.join(now, "", {}, 30s).has_value());       // grp-2: abre rebalanceo.
    ASSERT_TRUE(group.join(now, "grp-1", {}, 30s).has_value());  // grp-1 reingresa -> gen 2.
    ASSERT_EQ(group.generation(), 2);
    ASSERT_EQ(group.leader_id(), "grp-2");
    ASSERT_TRUE(group.sync(now, "grp-2", 2, {{"grp-1", bytes({0xA})}, {"grp-2", bytes({0xB})}})
                    .has_value());
    ASSERT_EQ(group.state(), nexus::GroupState::Stable);
}

TEST(ConsumerGroup, Join_PrimerMiembroIdVacio_GeneraIdYCompletaComoLider) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};

    const nexus::expected<nexus::JoinResult> result = group.join(t0, "", bytes({1}), 30s);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->member_id, "grp-1");
    EXPECT_EQ(result->generation, 1);
    EXPECT_TRUE(result->is_leader);
    EXPECT_EQ(result->leader_id, "grp-1");
    ASSERT_EQ(result->members.size(), 1U);  // el líder recibe la lista a repartir.
    EXPECT_EQ(result->members.front().member_id, "grp-1");
    EXPECT_EQ(group.state(), nexus::GroupState::CompletingRebalance);
    EXPECT_EQ(group.member_count(), 1U);
}

TEST(ConsumerGroup, Sync_LiderReparte_GrupoQuedaStableYDevuelveAsignacion) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());

    const nexus::expected<nexus::SyncResult> sync =
        group.sync(t0, "grp-1", 1, {{"grp-1", bytes({1, 2, 3})}});
    ASSERT_TRUE(sync.has_value());
    EXPECT_TRUE(sync->assigned);
    EXPECT_EQ(sync->assignment, bytes({1, 2, 3}));
    EXPECT_EQ(group.state(), nexus::GroupState::Stable);
}

TEST(ConsumerGroup, Sync_GeneracionObsoleta_DevuelveInvalidArgument) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());

    const nexus::expected<nexus::SyncResult> sync = group.sync(t0, "grp-1", 99, {});
    ASSERT_FALSE(sync.has_value());
    EXPECT_EQ(sync.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(ConsumerGroup, Sync_MiembroDesconocido_DevuelveNotFound) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());

    const nexus::expected<nexus::SyncResult> sync = group.sync(t0, "fantasma", 1, {});
    ASSERT_FALSE(sync.has_value());
    EXPECT_EQ(sync.error().code(), nexus::ErrorCode::NotFound);
}

TEST(ConsumerGroup, Join_SegundoMiembro_AbreRebalanceYNoCompletaHastaQueTodosReingresan) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());
    ASSERT_TRUE(group.sync(t0, "grp-1", 1, {{"grp-1", {}}}).has_value());  // Stable gen 1.

    const nexus::expected<nexus::JoinResult> second = group.join(t0, "", {}, 30s);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->member_id, "grp-2");
    EXPECT_EQ(group.state(), nexus::GroupState::PreparingRebalance);
    EXPECT_EQ(group.generation(), 1);  // no sube hasta que todos reingresen.
    EXPECT_TRUE(second->is_leader);    // grp-2 es el primero en reincorporarse a la ronda.
    EXPECT_EQ(group.member_count(), 2U);

    const nexus::expected<nexus::HeartbeatStatus> beat = group.heartbeat(t0, "grp-1", 1);
    ASSERT_TRUE(beat.has_value());
    EXPECT_EQ(*beat, nexus::HeartbeatStatus::RebalanceInProgress);

    const nexus::expected<nexus::JoinResult> rejoin = group.join(t0, "grp-1", {}, 30s);
    ASSERT_TRUE(rejoin.has_value());
    EXPECT_EQ(group.state(), nexus::GroupState::CompletingRebalance);
    EXPECT_EQ(group.generation(), 2);
    EXPECT_FALSE(rejoin->is_leader);
    EXPECT_EQ(rejoin->leader_id, "grp-2");
}

TEST(ConsumerGroup, Sync_SeguidorAntesDelLider_NoAsignado) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());                  // grp-1.
    ASSERT_TRUE(group.sync(t0, "grp-1", 1, {{"grp-1", {}}}).has_value());  // Stable gen 1.
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());       // grp-2 líder de la ronda.
    ASSERT_TRUE(group.join(t0, "grp-1", {}, 30s).has_value());  // -> CompletingRebalance.
    ASSERT_EQ(group.state(), nexus::GroupState::CompletingRebalance);

    // grp-1 (seguidor) sincroniza antes de que el líder reparta: aún sin asignación.
    const nexus::expected<nexus::SyncResult> follower = group.sync(t0, "grp-1", 2, {});
    ASSERT_TRUE(follower.has_value());
    EXPECT_FALSE(follower->assigned);
    EXPECT_EQ(group.state(), nexus::GroupState::CompletingRebalance);
}

TEST(ConsumerGroup, Sync_SeguidorTrasElLider_RecibeSuAsignacion) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    make_stable_pair(group, t0);  // gen 2, líder grp-2; asignaciones grp-1=0xA, grp-2=0xB.

    const nexus::expected<nexus::SyncResult> follower = group.sync(t0, "grp-1", 2, {});
    ASSERT_TRUE(follower.has_value());
    EXPECT_TRUE(follower->assigned);
    EXPECT_EQ(follower->assignment, bytes({0xA}));
}

TEST(ConsumerGroup, Heartbeat_GrupoEstable_DevuelveOk) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());
    ASSERT_TRUE(group.sync(t0, "grp-1", 1, {{"grp-1", {}}}).has_value());

    const nexus::expected<nexus::HeartbeatStatus> beat = group.heartbeat(t0, "grp-1", 1);
    ASSERT_TRUE(beat.has_value());
    EXPECT_EQ(*beat, nexus::HeartbeatStatus::Ok);
}

TEST(ConsumerGroup, Heartbeat_MiembroDesconocido_DevuelveNotFound) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());

    const nexus::expected<nexus::HeartbeatStatus> beat = group.heartbeat(t0, "fantasma", 1);
    ASSERT_FALSE(beat.has_value());
    EXPECT_EQ(beat.error().code(), nexus::ErrorCode::NotFound);
}

TEST(ConsumerGroup, Heartbeat_GeneracionObsoletaEstable_DevuelveInvalidArgument) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());
    ASSERT_TRUE(group.sync(t0, "grp-1", 1, {{"grp-1", {}}}).has_value());

    const nexus::expected<nexus::HeartbeatStatus> beat = group.heartbeat(t0, "grp-1", 99);
    ASSERT_FALSE(beat.has_value());
    EXPECT_EQ(beat.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(ConsumerGroup, Tick_SesionExpirada_ExpulsaMiembroYAbreRebalance) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    make_stable_pair(group, t0);  // ambos con last_seen == t0, session_timeout 30s.

    // grp-1 sigue latiendo; grp-2 no.
    ASSERT_TRUE(group.heartbeat(t0 + 20s, "grp-1", 2).has_value());
    group.tick(t0 + 31s);  // grp-2 lleva 31s sin latido (>30s): expira.

    EXPECT_EQ(group.member_count(), 1U);
    EXPECT_TRUE(group.contains("grp-1"));
    EXPECT_FALSE(group.contains("grp-2"));
    EXPECT_EQ(group.state(), nexus::GroupState::PreparingRebalance);
}

TEST(ConsumerGroup, Tick_ExpiranTodos_GrupoVuelveAEmpty) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    make_stable_pair(group, t0);

    group.tick(t0 + 31s);  // ninguno ha latido desde t0: ambos expiran.

    EXPECT_EQ(group.member_count(), 0U);
    EXPECT_EQ(group.state(), nexus::GroupState::Empty);
}

TEST(ConsumerGroup, Leave_MiembroDeVarios_AbreRebalance) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    make_stable_pair(group, t0);

    ASSERT_TRUE(group.leave("grp-1").has_value());
    EXPECT_EQ(group.member_count(), 1U);
    EXPECT_TRUE(group.contains("grp-2"));
    EXPECT_EQ(group.state(), nexus::GroupState::PreparingRebalance);
}

TEST(ConsumerGroup, Leave_UltimoMiembro_GrupoVuelveAEmpty) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());
    ASSERT_TRUE(group.sync(t0, "grp-1", 1, {{"grp-1", {}}}).has_value());

    ASSERT_TRUE(group.leave("grp-1").has_value());
    EXPECT_EQ(group.member_count(), 0U);
    EXPECT_EQ(group.state(), nexus::GroupState::Empty);
}

TEST(ConsumerGroup, Leave_MiembroDesconocido_DevuelveNotFound) {
    nexus::ConsumerGroup group{"grp"};
    const nexus::MonoTime t0{};
    ASSERT_TRUE(group.join(t0, "", {}, 30s).has_value());

    const nexus::expected<void> result = group.leave("fantasma");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::NotFound);
}

}  // namespace
