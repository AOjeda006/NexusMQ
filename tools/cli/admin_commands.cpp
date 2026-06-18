/// @file   cli/admin_commands.cpp
/// @brief  Implementación de los subcomandos de operación (`metrics`, `diagnostics`).
/// @ingroup cli

#include "cli/admin_commands.hpp"

#include "cli/render.hpp"

namespace nexus::cli {

int run_metrics(AdminClient& client, std::ostream& out, std::ostream& err) {
    const expected<ClientResponse> response = client.get("/metrics");
    if (!response) {
        err << response.error().message() << '\n';
        return 1;
    }
    if (response->status != 200) {
        return fail(err, *response);
    }
    out << response->body;
    if (!response->body.empty() && response->body.back() != '\n') {
        out << '\n';
    }
    return 0;
}

int run_diagnostics(AdminClient& client, std::ostream& out, std::ostream& err) {
    const expected<ClientResponse> liveness = client.get("/healthz");
    if (!liveness) {
        err << liveness.error().message() << '\n';
        return 1;
    }
    const expected<ClientResponse> readiness = client.get("/readyz");
    if (!readiness) {
        err << readiness.error().message() << '\n';
        return 1;
    }
    const bool live = liveness->status == 200;
    const bool ready = readiness->status == 200;
    out << "liveness:  " << (live ? "ok" : "drenando") << " (HTTP " << liveness->status << ")\n";
    out << "readiness: " << (ready ? "listo" : "no listo") << " (HTTP " << readiness->status
        << ")\n";
    return (live && ready) ? 0 : 1;
}

}  // namespace nexus::cli
