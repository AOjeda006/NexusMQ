// Pruebas de ConnectionState: identidad, versiones negociadas, principal, créditos y vuelo.
#include "ingress/connection_state.hpp"

#include <gtest/gtest.h>

#include "protocol/frame.hpp"

namespace {

nexus::ConnectionState make_state() {
    return nexus::ConnectionState{/*conn_id=*/42, /*credits=*/10};
}

TEST(ConnectionState, IdentidadYCreditosIniciales) {
    nexus::ConnectionState state = make_state();
    EXPECT_EQ(state.conn_id(), 42U);
    EXPECT_EQ(state.credits().available(), 10);
}

TEST(ConnectionState, VersionNegociada_AusentePorDefecto) {
    nexus::ConnectionState state = make_state();
    EXPECT_FALSE(state.negotiated_version(nexus::ApiKey::Produce).has_value());
}

TEST(ConnectionState, VersionNegociada_SeFijaYLee) {
    nexus::ConnectionState state = make_state();
    state.set_negotiated_version(nexus::ApiKey::Produce, 3);
    const auto version = state.negotiated_version(nexus::ApiKey::Produce);
    ASSERT_TRUE(version.has_value());
    EXPECT_EQ(*version, 3);
    EXPECT_FALSE(state.negotiated_version(nexus::ApiKey::Fetch).has_value());
}

TEST(ConnectionState, Principal_AnonimoPorDefecto_YSeFija) {
    nexus::ConnectionState state = make_state();
    EXPECT_FALSE(state.principal().has_value());
    state.set_principal("nexus-client");
    ASSERT_TRUE(state.principal().has_value());
    EXPECT_EQ(*state.principal(), "nexus-client");
}

TEST(ConnectionState, Inflight_AltaYBaja) {
    nexus::ConnectionState state = make_state();
    EXPECT_EQ(state.inflight_count(), 0U);

    const nexus::InflightRequest request{.api_key = nexus::ApiKey::Fetch, .started = {}};
    EXPECT_TRUE(state.begin_request(7, request));
    EXPECT_TRUE(state.has_inflight(7));
    EXPECT_EQ(state.inflight_count(), 1U);

    // Duplicar el mismo correlation_id se rechaza.
    EXPECT_FALSE(state.begin_request(7, request));

    const auto completed = state.complete_request(7);
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ(completed->api_key, nexus::ApiKey::Fetch);
    EXPECT_FALSE(state.has_inflight(7));
    EXPECT_EQ(state.inflight_count(), 0U);

    // Completar uno inexistente devuelve nullopt.
    EXPECT_FALSE(state.complete_request(7).has_value());
}

TEST(ConnectionState, Creditos_ConcederAumentaDisponibles) {
    nexus::ConnectionState state = make_state();
    state.credits().grant(5);
    EXPECT_EQ(state.credits().available(), 15);
}

}  // namespace
