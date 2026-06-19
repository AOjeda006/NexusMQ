/// @file   ingress/connection_state.hpp
/// @brief  ConnectionState: estado por conexión del ingress (versiones, principal, créditos,
/// vuelo).
/// @ingroup ingress

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "broker/credit_window.hpp"  // CreditWindow: primitivo header-only (backpressure)
#include "common/types.hpp"
#include "protocol/frame.hpp"

namespace nexus {

/// @brief Petición en vuelo en una conexión: su `ApiKey` y cuándo empezó. Afinidad: INMUTABLE.
/// @details Permite correlacionar la respuesta, medir latencia y aplicar timeouts por petición.
struct InflightRequest {
    ApiKey api_key = ApiKey::ApiVersions;
    MonoTime started{};
};

/// @brief Estado mutable de una conexión cliente del ingress. Afinidad: REACTOR-LOCAL.
/// @details Reúne lo que el reactor necesita recordar mientras la conexión vive: el identificador
///   de conexión, las **versiones negociadas** por `ApiKey` (handshake `ApiVersions`), el
///   **principal autenticado** (de TLS/mTLS o JWT, si lo hay), la **ventana de créditos** que frena
///   al emisor cuando satura (backpressure, §7.11) y el mapa de **peticiones en vuelo** por
///   `correlation_id`. Es **no copiable y no movible**: vive anclado a su reactor y la
///   `CreditWindow` guarda un handle de corrutina suspendida (moverla colgaría una referencia).
/// @invariant `inflight_count()` = entradas añadidas con `begin_request` y no retiradas con
///   `complete_request`.
class ConnectionState {
public:
    /// @param conn_id Identificador único de la conexión (lo asigna el aceptador).
    /// @param initial_credits Créditos iniciales de la ventana de backpressure.
    ConnectionState(std::uint64_t conn_id, std::int32_t initial_credits) noexcept
        : conn_id_(conn_id), credits_(initial_credits) {}

    ConnectionState(const ConnectionState&) = delete;
    ConnectionState& operator=(const ConnectionState&) = delete;
    ConnectionState(ConnectionState&&) = delete;
    ConnectionState& operator=(ConnectionState&&) = delete;
    ~ConnectionState() = default;

    [[nodiscard]] std::uint64_t conn_id() const noexcept { return conn_id_; }

    /// @brief Fija la versión negociada para @p key (resultado del handshake `ApiVersions`).
    void set_negotiated_version(ApiKey key, std::uint16_t version) { negotiated_[key] = version; }
    /// @brief Versión negociada para @p key, o `nullopt` si no se negoció.
    [[nodiscard]] std::optional<std::uint16_t> negotiated_version(ApiKey key) const {
        const auto it = negotiated_.find(key);
        if (it == negotiated_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    /// @brief Fija el principal autenticado de la conexión (CN del cert mTLS o `sub` del JWT).
    void set_principal(std::string principal) { principal_ = std::move(principal); }
    /// @brief Principal autenticado, o `nullopt` si la conexión es anónima.
    [[nodiscard]] const std::optional<std::string>& principal() const noexcept {
        return principal_;
    }

    /// @brief Ventana de créditos de la conexión (backpressure del emisor).
    [[nodiscard]] CreditWindow& credits() noexcept { return credits_; }

    /// @brief Registra una petición en vuelo bajo @p correlation_id. @return false si ya existía.
    bool begin_request(std::uint32_t correlation_id, const InflightRequest& request) {
        return inflight_.emplace(correlation_id, request).second;
    }
    /// @brief Retira la petición en vuelo de @p correlation_id. @return sus datos, o `nullopt`.
    std::optional<InflightRequest> complete_request(std::uint32_t correlation_id) {
        const auto it = inflight_.find(correlation_id);
        if (it == inflight_.end()) {
            return std::nullopt;
        }
        const InflightRequest request = it->second;
        inflight_.erase(it);
        return request;
    }
    /// @brief ¿Hay una petición en vuelo con @p correlation_id?
    [[nodiscard]] bool has_inflight(std::uint32_t correlation_id) const {
        return inflight_.contains(correlation_id);
    }
    /// @brief Número de peticiones en vuelo.
    [[nodiscard]] std::size_t inflight_count() const noexcept { return inflight_.size(); }

private:
    std::uint64_t conn_id_;
    std::optional<std::string> principal_;
    CreditWindow credits_;
    std::unordered_map<ApiKey, std::uint16_t> negotiated_;
    std::unordered_map<std::uint32_t, InflightRequest> inflight_;
};

}  // namespace nexus
