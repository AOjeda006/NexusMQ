/// @file   ingress/health_monitor.hpp
/// @brief  HealthMonitor: endpoints `/healthz` (liveness) y `/readyz` (readiness) (§7.6).
/// @ingroup ingress

#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "ingress/http.hpp"

namespace nexus {

/// @brief Resultado de un *probe* de readiness. Afinidad: INMUTABLE (valor).
struct HealthCheckResult {
    /// Nombre del check (`disk`, `raft`, `replicationLag`…).
    std::string name;
    /// ¿pasa el check?
    bool healthy = true;
    /// Explicación (vacía si todo va bien).
    std::string detail;
};

/// @brief Monitor de salud del nodo: sirve `/healthz` (liveness) y `/readyz` (readiness).
/// @details Afinidad: THREAD-SAFE (un mutex protege los *probes* y los flags). La **liveness**
///   indica que el proceso está vivo (200) salvo que esté **drenando** al apagar (503), señal para
///   que el balanceador deje de enviar tráfico. La **readiness** indica que el nodo puede aceptar
///   trabajo: pasa solo si el arranque ha terminado (`set_started`) y todos los *probes*
///   registrados (disco, Raft, *lag*…) están sanos. Los *probes* se **inyectan** desde el cableado
///   del server (I14), de modo que el monitor no depende del broker.
/// @invariant Los *probes* se registran antes de empezar a servir.
class HealthMonitor {
public:
    /// *Probe* de readiness: función que evalúa una condición y devuelve su resultado.
    using Probe = std::function<HealthCheckResult()>;

    /// Registra un *probe* de readiness bajo @p name (típicamente en el arranque).
    void register_readiness(std::string name, Probe probe);

    /// Marca el arranque como completado (`/readyz` no da 200 hasta entonces).
    void set_started(bool started) noexcept;

    /// Marca el nodo como vivo o **drenando** (al apagar). `false` ⇒ `/healthz` devuelve 503.
    void set_live(bool live) noexcept;

    /// @brief Respuesta de `/healthz`: 200 si vivo, 503 si drenando. Cuerpo JSON `{status}`.
    [[nodiscard]] HttpResponse liveness() const;

    /// @brief Respuesta de `/readyz`: 200 si listo, 503 si no. Cuerpo `{status, checks:[...]}`.
    [[nodiscard]] HttpResponse readiness() const;

private:
    mutable std::mutex mutex_;
    std::vector<std::pair<std::string, Probe>> probes_;
    bool started_ = false;
    bool live_ = true;
};

/// @brief *Probe* de espacio en disco: sano si el espacio libre en @p path ≥ @p min_free_bytes.
/// @details Útil para `/readyz`: evita aceptar escrituras sin sitio (cf. `ErrorCode::OutOfSpace`).
[[nodiscard]] HealthMonitor::Probe disk_space_probe(std::filesystem::path path,
                                                    std::uintmax_t min_free_bytes);

}  // namespace nexus
