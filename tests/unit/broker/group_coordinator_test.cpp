// Pruebas de GroupCoordinator: crea el grupo en el primer join, exige grupo existente en
// sync/heartbeat/leave (NotFound si no), y expira sesiones de todos los grupos con tick.
#include "broker/group_coordinator.hpp"

#include <gtest/gtest.h>

#include <chrono>

#include "broker/consumer_group.hpp"
#include "common/error.hpp"
#include "common/types.hpp"

namespace {

using namespace std::chrono_literals;

TEST(GroupCoordinator, Join_GrupoNuevo_LoCreaYDevuelveMiembro) {
    nexus::GroupCoordinator coordinator;
    const nexus::MonoTime t0{};

    const nexus::expected<nexus::JoinResult> result = coordinator.join(t0, "g", "", {}, 30s);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->member_id, "g-1");
    EXPECT_EQ(coordinator.group_count(), 1U);
    EXPECT_NE(coordinator.find("g"), nullptr);
}

TEST(GroupCoordinator, Sync_GrupoDesconocido_DevuelveNotFound) {
    nexus::GroupCoordinator coordinator;
    const nexus::MonoTime t0{};

    const nexus::expected<nexus::SyncResult> result = coordinator.sync(t0, "fantasma", "m", 1, {});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::NotFound);
}

TEST(GroupCoordinator, Leave_GrupoDesconocido_DevuelveNotFound) {
    nexus::GroupCoordinator coordinator;

    const nexus::expected<void> result = coordinator.leave("fantasma", "m");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::NotFound);
}

TEST(GroupCoordinator, Tick_ExpiraSesionesEnVariosGrupos) {
    nexus::GroupCoordinator coordinator;
    const nexus::MonoTime t0{};
    ASSERT_TRUE(coordinator.join(t0, "a", "", {}, 30s).has_value());
    ASSERT_TRUE(coordinator.join(t0, "b", "", {}, 30s).has_value());
    ASSERT_EQ(coordinator.group_count(), 2U);

    coordinator.tick(t0 + 31s);  // ninguno ha latido: ambos grupos quedan vacíos.

    ASSERT_NE(coordinator.find("a"), nullptr);
    ASSERT_NE(coordinator.find("b"), nullptr);
    EXPECT_EQ(coordinator.find("a")->member_count(), 0U);
    EXPECT_EQ(coordinator.find("a")->state(), nexus::GroupState::Empty);
    EXPECT_EQ(coordinator.find("b")->member_count(), 0U);
}

}  // namespace
