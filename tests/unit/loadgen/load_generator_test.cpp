// Test del motor de carga con un runner falso: a tasa 0 (sin ritmo) el bucle no duerme, así el
// recuento de peticiones, el descarte de calentamiento y el conteo de errores son deterministas.
// La corrección de coordinated omission (instantes previstos) se prueba aparte en OpenLoopSchedule.
#include "load_generator.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>

#include "common/error.hpp"
#include "loadgen_config.hpp"
#include "request_runner.hpp"

namespace {

using nexus::ErrorCode;
using nexus::expected;
using nexus::make_error;
using nexus::loadgen::LoadGenConfig;
using nexus::loadgen::LoadGenerator;
using nexus::loadgen::RequestRunner;
using nexus::loadgen::RunnerFactory;

// Runner falso: cuenta invocaciones y falla 1 de cada `fail_every` (0 = nunca falla). El contador
// es compartido entre conexiones (atómico) solo para verificar el total disparado por el motor.
class FakeRunner : public RequestRunner {
public:
    FakeRunner(std::atomic<std::uint64_t>& calls, std::uint64_t fail_every) noexcept
        : calls_(calls), fail_every_(fail_every) {}

    expected<void> run_once() override {
        const std::uint64_t n = ++calls_;
        if (fail_every_ != 0 && (n % fail_every_) == 0) {
            return make_error(ErrorCode::IoError, "fallo inyectado");
        }
        return {};
    }

private:
    std::atomic<std::uint64_t>& calls_;  // NOLINT(*-avoid-const-or-ref-data-members)
    std::uint64_t fail_every_;
};

RunnerFactory make_factory(std::atomic<std::uint64_t>& calls, std::uint64_t fail_every = 0) {
    return [&calls, fail_every](int /*worker_id*/) -> expected<std::unique_ptr<RequestRunner>> {
        return std::make_unique<FakeRunner>(calls, fail_every);
    };
}

LoadGenConfig unpaced_config(std::size_t op_count, std::size_t warmup, int connections) {
    LoadGenConfig cfg;
    cfg.op_count = op_count;
    cfg.warmup_ops = warmup;
    cfg.connections = connections;
    cfg.target_rate = 0.0;  // sin ritmo: el bucle no duerme (test rápido y determinista)
    return cfg;
}

TEST(LoadGenerator, SinErrores_RegistraSoloLasMedidas) {
    std::atomic<std::uint64_t> calls{0};
    LoadGenerator generator{unpaced_config(100, 20, 1), make_factory(calls)};

    const expected<nexus::loadgen::LoadGenReport> report = generator.run();
    ASSERT_TRUE(report.has_value());
    EXPECT_EQ(report->recorded, 100U);  // solo las medidas entran al histograma
    EXPECT_EQ(report->errors, 0U);
    EXPECT_EQ(calls.load(), 120U);  // calentamiento + medidas sí se disparan
    EXPECT_GT(report->p50_ns, 0U);  // hubo latencia registrada
}

TEST(LoadGenerator, ConErrores_LosCuentaYNoLosRegistra) {
    std::atomic<std::uint64_t> calls{0};
    // Sin calentamiento y 1 conexión: falla 1 de cada 10 → 10 errores de 100 medidas.
    LoadGenerator generator{unpaced_config(100, 0, 1), make_factory(calls, 10)};

    const expected<nexus::loadgen::LoadGenReport> report = generator.run();
    ASSERT_TRUE(report.has_value());
    EXPECT_EQ(report->errors, 10U);
    EXPECT_EQ(report->recorded, 90U);  // las fallidas no entran al histograma
    EXPECT_EQ(calls.load(), 100U);
}

TEST(LoadGenerator, VariasConexiones_RepartenTodoElTrabajo) {
    std::atomic<std::uint64_t> calls{0};
    LoadGenerator generator{unpaced_config(1'000, 0, 4), make_factory(calls)};

    const expected<nexus::loadgen::LoadGenReport> report = generator.run();
    ASSERT_TRUE(report.has_value());
    EXPECT_EQ(report->recorded, 1'000U);  // suma de los 4 histogramas por conexión
    EXPECT_EQ(calls.load(), 1'000U);      // cada petición se dispara una sola vez
}

TEST(LoadGenerator, FabricaFalla_AbortaLaCampana) {
    LoadGenConfig cfg = unpaced_config(10, 0, 2);
    RunnerFactory factory = [](int /*worker_id*/) -> expected<std::unique_ptr<RequestRunner>> {
        return make_error(ErrorCode::IoError, "conexión rechazada");
    };
    LoadGenerator generator{cfg, std::move(factory)};

    const expected<nexus::loadgen::LoadGenReport> report = generator.run();
    ASSERT_FALSE(report.has_value());
    EXPECT_EQ(report.error().code(), ErrorCode::IoError);
}

TEST(LoadGenerator, ConfigInvalida_Rechazada) {
    std::atomic<std::uint64_t> calls{0};
    LoadGenerator zero_conns{unpaced_config(10, 0, 0), make_factory(calls)};
    EXPECT_FALSE(zero_conns.run().has_value());

    LoadGenerator zero_ops{unpaced_config(0, 0, 1), make_factory(calls)};
    EXPECT_FALSE(zero_ops.run().has_value());
}

}  // namespace
