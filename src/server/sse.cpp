/// @file   server/sse.cpp
/// @brief  Implementación del formato de eventos SSE.
/// @ingroup server

#include "server/sse.hpp"

#include <cstddef>

namespace nexus {

std::string format_sse_event(std::string_view event, std::string_view data) {
    std::string out;
    out.reserve(event.size() + data.size() + 16);
    if (!event.empty()) {
        out += "event: ";
        out += event;
        out += '\n';
    }
    // Cada línea del payload lleva su propio prefijo `data: ` (SSE / W3C EventSource).
    std::size_t pos = 0;
    while (true) {
        const std::size_t newline = data.find('\n', pos);
        const std::string_view line = (newline == std::string_view::npos)
                                          ? data.substr(pos)
                                          : data.substr(pos, newline - pos);
        out += "data: ";
        out += line;
        out += '\n';
        if (newline == std::string_view::npos) {
            break;
        }
        pos = newline + 1;
    }
    out += '\n';  // línea en blanco: fin del evento.
    return out;
}

}  // namespace nexus
