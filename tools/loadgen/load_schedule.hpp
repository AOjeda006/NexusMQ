/// @file   tools/loadgen/load_schedule.hpp
/// @brief  OpenLoopSchedule: instantes previstos de un generador de carga open-loop.
/// @ingroup loadgen

#pragma once

#include <chrono>
#include <cstdint>

#include "common/types.hpp"

namespace nexus::loadgen {

/// @brief Calendario de envío a **tasa fija** de un generador de carga open-loop. Afinidad:
///   INMUTABLE.
/// @details Da el **instante previsto** de la petición número `index` desde un `epoch`, separando
///   peticiones por un intervalo constante `1/tasa`. Es la pieza que evita la *coordinated
///   omission* (Gregg/Tene): la latencia de cada petición se mide contra su instante **previsto**,
///   no contra el de envío real; si el sistema se satura y el generador se retrasa, ese retraso
///   queda dentro de la latencia medida (refleja la cola que sufriría un cliente real a tasa fija).
///   Con `tasa <= 0` el calendario **no es ritmado** (`is_paced() == false`): todas las peticiones
///   se envían cuanto antes (carga closed-loop a máximo throughput), útil para medir el techo.
/// @invariant `intended_at` es monótona creciente en `index` cuando `is_paced()`.
class OpenLoopSchedule {
public:
    /// @brief Construye el calendario a @p rate_per_second peticiones/segundo desde @p epoch.
    /// @param rate_per_second Tasa objetivo; `<= 0` desactiva el ritmo (envío a máxima velocidad).
    /// @param epoch Instante de la petición 0 (origen del calendario).
    OpenLoopSchedule(double rate_per_second, MonoTime epoch) noexcept
        : interval_ns_(rate_per_second > 0.0 ? kNanosPerSecond / rate_per_second : 0.0),
          epoch_(epoch) {}

    /// @brief ¿El calendario impone un ritmo (tasa fija)? `false` si la tasa era `<= 0`.
    [[nodiscard]] bool is_paced() const noexcept { return interval_ns_ > 0.0; }

    /// @brief Instante previsto de la petición @p index. `epoch` si no es ritmado.
    [[nodiscard]] MonoTime intended_at(std::uint64_t index) const noexcept {
        if (!is_paced()) {
            return epoch_;
        }
        const double offset_ns = interval_ns_ * static_cast<double>(index);
        return epoch_ + std::chrono::nanoseconds{static_cast<std::int64_t>(offset_ns)};
    }

    /// @brief Intervalo entre instantes previstos, en nanosegundos (`0` si no es ritmado).
    [[nodiscard]] double interval_ns() const noexcept { return interval_ns_; }

private:
    static constexpr double kNanosPerSecond = 1'000'000'000.0;

    double interval_ns_;  ///< Separación entre peticiones (ns); `0` = sin ritmo.
    MonoTime epoch_;      ///< Instante de la petición 0.
};

}  // namespace nexus::loadgen
