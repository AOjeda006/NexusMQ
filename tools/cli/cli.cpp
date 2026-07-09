/// @file   cli/cli.cpp
/// @brief  Implementación del parseo de opciones globales y el despacho de subcomandos.
/// @ingroup cli

#include "cli/cli.hpp"

#include <charconv>
#include <string>
#include <utility>

#include "cli/admin_commands.hpp"
#include "cli/group_commands.hpp"
#include "cli/http_admin_client.hpp"
#include "cli/partition_commands.hpp"
#include "cli/topic_commands.hpp"

namespace nexus::cli {

namespace {

/// Parsea un puerto (`1..65535`) de @p text.
expected<std::uint16_t> parse_port(std::string_view text) {
    unsigned long value = 0;
    const auto* end = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(text.data(), end, value);
    if (ec != std::errc{} || ptr != end || value == 0 || value > 65535) {
        return make_error(ErrorCode::InvalidArgument, "puerto inválido: " + std::string{text});
    }
    return static_cast<std::uint16_t>(value);
}

/// Imprime el uso del CLI en @p out.
void print_usage(std::ostream& out) {
    out << "uso: nexus-cli [--host H] [--port P] [--token T] <comando> [args…]\n"
        << "comandos:\n"
        << "  topic list                      lista los topics\n"
        << "  topic create <name> [-p N] [-r R]  crea un topic\n"
        << "  topic describe <name>           describe un topic\n"
        << "  topic delete <name>             borra un topic\n"
        << "  group list                      lista los grupos de consumidores\n"
        << "  group describe <id>             describe un grupo\n"
        << "  partitions <topic>              lista las particiones de un topic\n"
        << "  metrics                         vuelca /metrics (Prometheus)\n"
        << "  diagnostics                     resumen de liveness/readiness\n";
}

}  // namespace

expected<ParsedArgs> parse_global_options(std::span<const std::string_view> args) {
    ParsedArgs parsed;
    std::size_t i = 0;
    while (i < args.size()) {
        const std::string_view arg = args[i];
        if (arg == "--help" || arg == "-h") {
            break;  // ayuda: la despacha `run_cli` (help/--help/-h) e imprime el uso con éxito.
        }
        if (!arg.starts_with("--")) {
            break;  // primer no-flag: empieza el comando.
        }
        // Soporta `--flag=valor`.
        std::string_view name = arg;
        std::string_view inline_value;
        bool has_inline = false;
        if (const std::size_t eq = arg.find('='); eq != std::string_view::npos) {
            name = arg.substr(0, eq);
            inline_value = arg.substr(eq + 1);
            has_inline = true;
        }
        auto next_value = [&](std::string_view& out_value) -> bool {
            if (has_inline) {
                out_value = inline_value;
                return true;
            }
            if (i + 1 < args.size()) {
                out_value = args[++i];
                return true;
            }
            return false;
        };

        std::string_view value;
        if (name == "--host") {
            if (!next_value(value)) {
                return make_error(ErrorCode::InvalidArgument, "--host requiere un valor");
            }
            parsed.options.host = std::string{value};
        } else if (name == "--port") {
            if (!next_value(value)) {
                return make_error(ErrorCode::InvalidArgument, "--port requiere un valor");
            }
            const expected<std::uint16_t> port = parse_port(value);
            if (!port) {
                return std::unexpected{port.error()};
            }
            parsed.options.port = *port;
        } else if (name == "--token") {
            if (!next_value(value)) {
                return make_error(ErrorCode::InvalidArgument, "--token requiere un valor");
            }
            parsed.options.token = std::string{value};
        } else {
            return make_error(ErrorCode::InvalidArgument,
                              "opción desconocida: " + std::string{arg});
        }
        ++i;
    }
    for (; i < args.size(); ++i) {
        parsed.rest.push_back(args[i]);
    }
    return parsed;
}

int run_cli(std::span<const std::string_view> args, std::ostream& out, std::ostream& err) {
    const expected<ParsedArgs> parsed = parse_global_options(args);
    if (!parsed) {
        err << parsed.error().message() << '\n';
        return 1;
    }
    if (parsed->rest.empty()) {
        print_usage(out);
        return 1;
    }
    const std::string_view command = parsed->rest[0];
    const std::span<const std::string_view> rest{parsed->rest.data() + 1, parsed->rest.size() - 1};

    if (command == "help" || command == "--help" || command == "-h") {
        print_usage(out);
        return 0;
    }
    if (command == "topic" || command == "group" || command == "partitions" ||
        command == "metrics" || command == "diagnostics") {
        HttpAdminClient client{HttpAdminClient::Options{.host = parsed->options.host,
                                                        .port = parsed->options.port,
                                                        .bearer_token = parsed->options.token}};
        if (command == "topic") {
            return run_topic(client, rest, out, err);
        }
        if (command == "group") {
            return run_group(client, rest, out, err);
        }
        if (command == "partitions") {
            return run_partitions(client, rest, out, err);
        }
        if (command == "metrics") {
            return run_metrics(client, out, err);
        }
        return run_diagnostics(client, out, err);
    }
    err << "comando desconocido: " << command << '\n';
    print_usage(err);
    return 1;
}

}  // namespace nexus::cli
