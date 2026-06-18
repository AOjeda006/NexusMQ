/// @file   ingress/pagination.cpp
/// @brief  Implementación del parseo de paginación (`page`/`size`).
/// @ingroup ingress

#include "ingress/pagination.hpp"

#include <charconv>
#include <optional>
#include <string_view>

namespace nexus {

namespace {

/// Busca el valor del parámetro @p key en una *query string* `k=v&k2=v2`.
std::optional<std::string_view> query_param(std::string_view query, std::string_view key) {
    std::size_t pos = 0;
    while (pos <= query.size()) {
        const std::size_t amp = query.find('&', pos);
        const std::string_view pair =
            query.substr(pos, amp == std::string_view::npos ? amp : amp - pos);
        const std::size_t eq = pair.find('=');
        if (eq != std::string_view::npos && pair.substr(0, eq) == key) {
            return pair.substr(eq + 1);
        }
        if (amp == std::string_view::npos) {
            break;
        }
        pos = amp + 1;
    }
    return std::nullopt;
}

/// Parsea un entero decimal completo (todo el texto), o `nullopt` si no lo es.
std::optional<std::size_t> parse_size_t(std::string_view text) {
    std::size_t value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || ptr != text.data() + text.size()) {
        return std::nullopt;
    }
    return value;
}

}  // namespace

expected<Page> parse_pagination(std::string_view query, const PaginationLimits& limits) {
    Page page;
    page.size = limits.default_size;

    if (const auto raw = query_param(query, "page")) {
        const auto number = parse_size_t(*raw);
        if (!number || *number < 1) {
            return make_error(ErrorCode::InvalidArgument,
                              "parámetro 'page' inválido (debe ser >= 1)");
        }
        page.number = *number;
    }

    if (const auto raw = query_param(query, "size")) {
        const auto size = parse_size_t(*raw);
        if (!size || *size < 1 || *size > limits.max_size) {
            return make_error(ErrorCode::InvalidArgument, "parámetro 'size' fuera de rango");
        }
        page.size = *size;
    }

    return page;
}

}  // namespace nexus
