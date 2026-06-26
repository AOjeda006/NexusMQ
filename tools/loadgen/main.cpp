/// @file   tools/loadgen/main.cpp
/// @brief  nexus-loadgen: generador de carga open-loop sobre la red contra un `nexusd` real.
/// @ingroup loadgen
///
/// Mide la latencia end-to-end de produce contra el broker a una **tasa fija** (open-loop), sin
/// *coordinated omission* (la latencia se mide contra el instante previsto de cada petición, no el
/// de envío). Reporta p50/p99/p999/max (metodología de `fundamentos/rendimiento/`). La campaña a
/// escala y la publicación de cifras es L2; aquí queda la herramienta lista y probada.

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <format>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "client/client.hpp"
#include "client/endpoint.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "load_generator.hpp"
#include "loadgen_config.hpp"
#include "loadgen_report.hpp"
#include "produce_runner.hpp"
#include "request_runner.hpp"

namespace {

// <print> es de GCC 14; el CI usa GCC 13. Se usa std::format + std::cout (disponible en ambos).
template <class... Args>
void print_line(std::format_string<Args...> fmt, Args&&... args) {
    std::cout << std::format(fmt, std::forward<Args>(args)...) << '\n';
}

/// Parámetros de línea de comandos del generador (broker destino + carga).
struct CliOptions {
    std::string host = "127.0.0.1";
    std::uint16_t port = 9092;
    std::string topic = "loadgen";
    nexus::PartitionId partition = 0;
    nexus::loadgen::LoadGenConfig config;
};

/// Convierte @p text a entero sin signo; deja @p out intacto si no parsea.
template <class T>
void parse_uint(std::string_view text, T& out) {
    T value{};
    if (std::from_chars(text.data(), text.data() + text.size(), value).ec == std::errc{}) {
        out = value;
    }
}

// from_chars de coma flotante está deleted en libc++ 18 → se usa strtod sobre una copia con \0.
void parse_double(std::string_view text, double& out) {
    const std::string copy{text};
    char* end = nullptr;
    const double value = std::strtod(copy.c_str(), &end);
    if (end != copy.c_str()) {
        out = value;
    }
}

/// Parsea `--clave valor` en @p opts (tolerante: ignora lo desconocido, conserva los defaults).
CliOptions parse_args(int argc, char** argv) {
    CliOptions opts;
    for (int i = 1; i + 1 < argc; i += 2) {
        const std::string_view key{argv[i]};
        const std::string_view value{argv[i + 1]};
        if (key == "--host") {
            opts.host = std::string{value};
        } else if (key == "--port") {
            parse_uint(value, opts.port);
        } else if (key == "--topic") {
            opts.topic = std::string{value};
        } else if (key == "--partition") {
            parse_uint(value, opts.partition);
        } else if (key == "--payload") {
            parse_uint(value, opts.config.payload_size);
        } else if (key == "--rate") {
            parse_double(value, opts.config.target_rate);
        } else if (key == "--count") {
            parse_uint(value, opts.config.op_count);
        } else if (key == "--warmup") {
            parse_uint(value, opts.config.warmup_ops);
        } else if (key == "--connections") {
            parse_uint(value, opts.config.connections);
        }
    }
    return opts;
}

/// Asegura que el topic exista (best-effort): un fallo «ya existe» no es un error de la campaña.
void ensure_topic(const CliOptions& opts) {
    nexus::expected<nexus::Client> client =
        nexus::Client::connect(nexus::Endpoint{.host = opts.host, .port = opts.port});
    if (!client) {
        return;
    }
    static_cast<void>(client->create_topic(opts.topic, 1));
    client->close();
}

}  // namespace

int main(int argc, char** argv) {
    const CliOptions opts = parse_args(argc, argv);
    const nexus::Endpoint endpoint{.host = opts.host, .port = opts.port};

    print_line(
        "NexusMQ — generador de carga open-loop ({} ops, {} conexiones, payload {} B, "
        "rate {:.0f} req/s)",
        opts.config.op_count, opts.config.connections, opts.config.payload_size,
        opts.config.target_rate);

    ensure_topic(opts);

    nexus::loadgen::RunnerFactory factory =
        [endpoint, topic = opts.topic, partition = opts.partition,
         payload = opts.config.payload_size](
            int /*worker_id*/) -> nexus::expected<std::unique_ptr<nexus::loadgen::RequestRunner>> {
        nexus::expected<std::unique_ptr<nexus::loadgen::ProduceRunner>> runner =
            nexus::loadgen::ProduceRunner::create(endpoint, topic, partition, payload);
        if (!runner) {
            return std::unexpected<nexus::Error>(runner.error());
        }
        return std::unique_ptr<nexus::loadgen::RequestRunner>{std::move(*runner)};
    };

    nexus::loadgen::LoadGenerator generator{opts.config, std::move(factory)};
    nexus::expected<nexus::loadgen::LoadGenReport> report = generator.run();
    if (!report) {
        std::cerr << std::format("loadgen falló: {}\n", report.error().message());
        return EXIT_FAILURE;
    }

    print_line("{}", nexus::loadgen::summary_line(*report));
    return EXIT_SUCCESS;
}
