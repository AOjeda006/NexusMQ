/// @file   cli/render.hpp
/// @brief  Utilidades de render compartidas por los subcomandos (lectura JSON + errores).
/// @ingroup cli

#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>

#include "cli/admin_client.hpp"
#include "ingress/json_value.hpp"

namespace nexus::cli {

/// Lee un entero @p key de un objeto JSON (0 si falta o no es número).
inline std::int64_t json_int(const JsonValue& object, std::string_view key) {
    const JsonValue* value = object.find(key);
    return (value != nullptr && value->is_number()) ? value->as_int64() : 0;
}

/// Lee una cadena @p key de un objeto JSON (vacía si falta o no es cadena).
inline std::string json_str(const JsonValue& object, std::string_view key) {
    const JsonValue* value = object.find(key);
    return (value != nullptr && value->is_string()) ? value->as_string() : std::string{};
}

/// Imprime el cuerpo de error (problem+json o texto) en @p err y devuelve 1.
inline int fail(std::ostream& err, const ClientResponse& response) {
    err << "error (HTTP " << response.status << ")";
    if (!response.body.empty()) {
        err << ": " << response.body;
    }
    err << '\n';
    return 1;
}

}  // namespace nexus::cli
