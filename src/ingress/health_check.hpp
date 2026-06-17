/// @file   ingress/health_check.hpp
/// @brief  HealthChecker: salud de nodos por sondeo activo + errores pasivos (§6.4/§7.5, ADR-0006).
/// @ingroup ingress

#pragma once

#include <cstddef>
#include <unordered_map>

#include "common/types.hpp"

namespace nexus {

/// @brief Umbrales de histéresis del chequeo de salud. Afinidad: INMUTABLE.
struct HealthCheckConfig {
    /// Fallos consecutivos para marcar un nodo **caído** (estando sano).
    std::size_t failure_threshold = 3;
    /// Éxitos consecutivos para volver a marcarlo **sano** (estando caído).
    std::size_t success_threshold = 2;
};

/// @brief Rastrea la salud de los nodos del cluster para enrutado/balanceo. Afinidad:
/// REACTOR-LOCAL.
/// @details Combina dos fuentes (§6.4): un **sondeo activo** (ping periódico que el reactor dispara
/// y
///   cuyo resultado entrega aquí) y la **observación pasiva** de los resultados reales de las
///   peticiones. Ambas llaman a `observe(node, ok)`. Aplica **histéresis** para no oscilar: hacen
///   falta `failure_threshold` fallos consecutivos para caer y `success_threshold` éxitos
///   consecutivos para recuperarse (un solo resultado del signo opuesto reinicia el contador
///   contrario). Alimenta `/readyz` y los *health checks* del ingress (§7.5).
/// @note FSM **sin E/S ni reloj**: el sondeo activo lo dispara el portador (reactor); aquí solo se
///   contabilizan resultados, para pruebas deterministas. No es thread-safe: vive en su reactor.
/// @invariant Un nodo nunca observado se considera **sano** (optimista: recibe tráfico hasta que se
///   demuestre lo contrario).
class HealthChecker {
public:
    explicit HealthChecker(HealthCheckConfig config = {}) : config_(config) {}

    /// @brief Registra un resultado para @p node (`ok` del sondeo activo o de una petición real).
    void observe(NodeId node, bool ok) {
        NodeHealth& health = nodes_[node];
        if (ok) {
            ++health.successes;
            health.failures = 0;
            if (!health.healthy && health.successes >= config_.success_threshold) {
                health.healthy = true;
            }
        } else {
            ++health.failures;
            health.successes = 0;
            if (health.healthy && health.failures >= config_.failure_threshold) {
                health.healthy = false;
            }
        }
    }

    /// @brief ¿Está @p node sano ahora? Un nodo nunca observado se considera sano.
    [[nodiscard]] bool healthy(NodeId node) const {
        const auto it = nodes_.find(node);
        return it == nodes_.end() || it->second.healthy;
    }

    /// @brief Fallos consecutivos contabilizados para @p node (0 si nunca visto).
    [[nodiscard]] std::size_t consecutive_failures(NodeId node) const {
        const auto it = nodes_.find(node);
        return it == nodes_.end() ? 0 : it->second.failures;
    }

private:
    struct NodeHealth {
        bool healthy = true;
        std::size_t failures = 0;
        std::size_t successes = 0;
    };

    HealthCheckConfig config_;
    std::unordered_map<NodeId, NodeHealth> nodes_;
};

}  // namespace nexus
