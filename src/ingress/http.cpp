/// @file   ingress/http.cpp
/// @brief  Implementación del parser y serializador HTTP/1.1 (§7.6).
/// @ingroup ingress

#include "ingress/http.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <string>

namespace nexus {

namespace {

/// Igualdad de cadenas sin distinguir mayúsculas (ASCII).
bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        const auto ca = static_cast<unsigned char>(a[i]);
        const auto cb = static_cast<unsigned char>(b[i]);
        if (std::tolower(ca) != std::tolower(cb)) {
            return false;
        }
    }
    return true;
}

/// Recorta espacios y tabuladores al inicio y final.
std::string_view trim(std::string_view s) {
    const auto is_space = [](char c) { return c == ' ' || c == '\t'; };
    while (!s.empty() && is_space(s.front())) {
        s.remove_prefix(1);
    }
    while (!s.empty() && is_space(s.back())) {
        s.remove_suffix(1);
    }
    return s;
}

/// Parsea la línea de petición (`METHOD target HTTP/x.y`) en @p req.
expected<void> parse_request_line(std::string_view line, HttpRequest& req);
/// Parsea el bloque de cabeceras (líneas `Name: value`) en @p headers; capta `Content-Length`.
expected<void> parse_headers(std::string_view block, const HttpParseLimits& limits,
                             std::vector<std::pair<std::string, std::string>>& headers,
                             std::optional<std::size_t>& content_length);

HttpMethod parse_method(std::string_view token) {
    if (token == "GET") {
        return HttpMethod::Get;
    }
    if (token == "HEAD") {
        return HttpMethod::Head;
    }
    if (token == "POST") {
        return HttpMethod::Post;
    }
    if (token == "PUT") {
        return HttpMethod::Put;
    }
    if (token == "DELETE") {
        return HttpMethod::Delete;
    }
    if (token == "PATCH") {
        return HttpMethod::Patch;
    }
    if (token == "OPTIONS") {
        return HttpMethod::Options;
    }
    return HttpMethod::Unknown;
}

expected<void> parse_request_line(std::string_view line, HttpRequest& req) {
    const std::size_t sp1 = line.find(' ');
    const std::size_t sp2 = sp1 == std::string_view::npos ? sp1 : line.find(' ', sp1 + 1);
    if (sp1 == std::string_view::npos || sp2 == std::string_view::npos) {
        return make_error(ErrorCode::InvalidArgument, "línea de petición HTTP malformada");
    }
    const std::string_view method = line.substr(0, sp1);
    const std::string_view target = line.substr(sp1 + 1, sp2 - sp1 - 1);
    const std::string_view version = line.substr(sp2 + 1);
    if (method.empty() || target.empty() || !version.starts_with("HTTP/")) {
        return make_error(ErrorCode::InvalidArgument, "línea de petición HTTP inválida");
    }
    req.method = parse_method(method);
    req.method_text = std::string{method};
    req.target = std::string{target};
    req.version = std::string{version};
    return {};
}

expected<void> parse_headers(std::string_view block, const HttpParseLimits& limits,
                             std::vector<std::pair<std::string, std::string>>& headers,
                             std::optional<std::size_t>& content_length) {
    std::size_t pos = 0;
    while (pos < block.size()) {
        const std::size_t eol = block.find("\r\n", pos);
        const std::string_view header_line =
            block.substr(pos, eol == std::string_view::npos ? eol : eol - pos);
        pos = eol == std::string_view::npos ? block.size() : eol + 2;

        const std::size_t colon = header_line.find(':');
        if (colon == std::string_view::npos) {
            return make_error(ErrorCode::InvalidArgument, "cabecera HTTP sin ':'");
        }
        const std::string_view name = trim(header_line.substr(0, colon));
        const std::string_view value = trim(header_line.substr(colon + 1));
        if (name.empty()) {
            return make_error(ErrorCode::InvalidArgument, "cabecera HTTP con nombre vacío");
        }
        if (headers.size() >= limits.max_headers) {
            return make_error(ErrorCode::InvalidArgument, "demasiadas cabeceras HTTP");
        }
        if (iequals(name, "content-length")) {
            std::size_t len = 0;
            const auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), len);
            if (ec != std::errc{} || ptr != value.data() + value.size()) {
                return make_error(ErrorCode::InvalidArgument, "Content-Length inválido");
            }
            content_length = len;
        }
        headers.emplace_back(std::string{name}, std::string{value});
    }
    return {};
}

}  // namespace

std::string_view HttpRequest::path() const noexcept {
    const std::string_view t{target};
    const std::size_t q = t.find('?');
    return std::string_view{t.data(), q == std::string_view::npos ? t.size() : q};
}

std::string_view HttpRequest::query() const noexcept {
    const std::string_view t{target};
    const std::size_t q = t.find('?');
    if (q == std::string_view::npos) {
        return {};
    }
    return std::string_view{t.data() + q + 1, t.size() - q - 1};
}

std::optional<std::string_view> HttpRequest::header(std::string_view name) const {
    for (const auto& [key, value] : headers) {
        if (iequals(key, name)) {
            return std::string_view{value};
        }
    }
    return std::nullopt;
}

void HttpResponse::set_header(std::string name, std::string value) {
    for (auto& [key, existing] : headers) {
        if (iequals(key, name)) {
            existing = std::move(value);
            return;
        }
    }
    headers.emplace_back(std::move(name), std::move(value));
}

std::string HttpResponse::serialize() const {
    std::string out = "HTTP/1.1 ";
    out += std::to_string(status);
    out += ' ';
    out += reason;
    out += "\r\n";
    for (const auto& [key, value] : headers) {
        if (iequals(key, "content-length")) {
            continue;  // se recalcula siempre acorde al cuerpo.
        }
        out += key;
        out += ": ";
        out += value;
        out += "\r\n";
    }
    out += "Content-Length: ";
    out += std::to_string(body.size());
    out += "\r\n\r\n";
    out += body;
    return out;
}

std::string_view http_reason(int status) noexcept {
    switch (status) {
        case 200:
            return "OK";
        case 201:
            return "Created";
        case 202:
            return "Accepted";
        case 204:
            return "No Content";
        case 400:
            return "Bad Request";
        case 401:
            return "Unauthorized";
        case 403:
            return "Forbidden";
        case 404:
            return "Not Found";
        case 405:
            return "Method Not Allowed";
        case 409:
            return "Conflict";
        case 422:
            return "Unprocessable Entity";
        case 429:
            return "Too Many Requests";
        case 500:
            return "Internal Server Error";
        case 501:
            return "Not Implemented";
        case 503:
            return "Service Unavailable";
        case 507:
            return "Insufficient Storage";
        default:
            return "";
    }
}

expected<HttpRequest> parse_request(std::string_view raw, const HttpParseLimits& limits) {
    const std::size_t line_end = raw.find("\r\n");
    if (line_end == std::string_view::npos) {
        return make_error(ErrorCode::InvalidArgument, "petición HTTP sin línea de petición");
    }
    if (line_end > limits.max_request_line) {
        return make_error(ErrorCode::InvalidArgument, "línea de petición HTTP demasiado larga");
    }

    HttpRequest req;
    if (auto parsed = parse_request_line(raw.substr(0, line_end), req); !parsed) {
        return std::unexpected(parsed.error());
    }

    const std::size_t headers_end = raw.find("\r\n\r\n", line_end);
    if (headers_end == std::string_view::npos) {
        return make_error(ErrorCode::InvalidArgument, "petición HTTP sin fin de cabeceras");
    }
    // Bloque de cabeceras (puede estar vacío): entre el fin de la línea de petición y la línea en
    // blanco. Si no hay cabeceras, ambos coinciden y el bloque es vacío.
    const std::size_t block_start = line_end + 2;
    const std::string_view header_block = headers_end > block_start
                                              ? raw.substr(block_start, headers_end - block_start)
                                              : std::string_view{};
    if (header_block.size() > limits.max_header_bytes) {
        return make_error(ErrorCode::InvalidArgument, "cabeceras HTTP demasiado grandes");
    }

    std::optional<std::size_t> content_length;
    if (auto parsed = parse_headers(header_block, limits, req.headers, content_length); !parsed) {
        return std::unexpected(parsed.error());
    }

    const std::string_view body = raw.substr(headers_end + 4);
    if (content_length) {
        if (*content_length > limits.max_body) {
            return make_error(ErrorCode::InvalidArgument, "cuerpo HTTP excede el máximo");
        }
        if (body.size() < *content_length) {
            return make_error(ErrorCode::InvalidArgument, "cuerpo HTTP incompleto");
        }
        req.body = std::string{body.substr(0, *content_length)};
    } else {
        if (body.size() > limits.max_body) {
            return make_error(ErrorCode::InvalidArgument, "cuerpo HTTP excede el máximo");
        }
        req.body = std::string{body};
    }
    return req;
}

}  // namespace nexus
