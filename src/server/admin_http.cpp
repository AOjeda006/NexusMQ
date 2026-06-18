/// @file   server/admin_http.cpp
/// @brief  Implementación del servicio HTTP/1.1 del puerto de operación sobre un Socket.
/// @ingroup server

#include "server/admin_http.hpp"

#include <array>
#include <charconv>
#include <cstddef>
#include <string>
#include <string_view>

#include "common/bytes.hpp"
#include "server/admin_router.hpp"

namespace nexus {

namespace {

/// Tamaño del búfer de recepción por iteración.
constexpr std::size_t kChunkSize = 4096;

/// ¿Coinciden @p a y @p b sin distinguir mayúsculas (ASCII)?
bool iequals(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        const char ca = (a[i] >= 'A' && a[i] <= 'Z') ? static_cast<char>(a[i] - 'A' + 'a') : a[i];
        const char cb = (b[i] >= 'A' && b[i] <= 'Z') ? static_cast<char>(b[i] - 'A' + 'a') : b[i];
        if (ca != cb) {
            return false;
        }
    }
    return true;
}

/// Recorta espacios y tabuladores al principio y al final de @p text.
std::string_view trim(std::string_view text) noexcept {
    while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
        text.remove_prefix(1);
    }
    while (!text.empty() && (text.back() == ' ' || text.back() == '\t')) {
        text.remove_suffix(1);
    }
    return text;
}

/// Extrae el `Content-Length` (case-insensitive) de las cabeceras; `0` si ausente o inválido.
std::size_t parse_content_length(std::string_view headers) {
    std::size_t pos = 0;
    while (pos < headers.size()) {
        const std::size_t eol = headers.find("\r\n", pos);
        const std::size_t line_len =
            (eol == std::string_view::npos) ? headers.size() - pos : eol - pos;
        const std::string_view line = headers.substr(pos, line_len);
        if (const std::size_t colon = line.find(':'); colon != std::string_view::npos) {
            if (iequals(trim(line.substr(0, colon)), "content-length")) {
                const std::string value{trim(line.substr(colon + 1))};
                std::size_t length = 0;
                if (std::from_chars(value.data(), value.data() + value.size(), length).ec ==
                    std::errc{}) {
                    return length;
                }
                return 0;
            }
        }
        if (eol == std::string_view::npos) {
            break;
        }
        pos = eol + 2;
    }
    return 0;
}

/// Respuesta `400 Bad Request` de texto plano para mensajes malformados.
HttpResponse bad_request() {
    HttpResponse response;
    response.status = 400;
    response.reason = std::string{http_reason(400)};
    response.set_header("Content-Type", "text/plain; charset=utf-8");
    response.body = "bad request\n";
    return response;
}

}  // namespace

task<expected<HttpRequest>> read_http_request(Proactor& proactor, const Socket& sock,
                                              HttpParseLimits limits) {
    std::string buf;
    std::array<std::byte, kChunkSize> chunk{};
    const std::size_t header_cap = limits.max_request_line + limits.max_header_bytes;

    std::size_t header_end = std::string::npos;
    while (true) {
        header_end = buf.find("\r\n\r\n");
        if (header_end != std::string::npos) {
            break;
        }
        if (buf.size() > header_cap) {
            co_return make_error(ErrorCode::InvalidArgument, "cabeceras HTTP demasiado grandes");
        }
        const expected<std::size_t> got =
            co_await sock.async_recv(proactor, MutByteSpan{chunk.data(), chunk.size()});
        if (!got) {
            co_return std::unexpected{got.error()};
        }
        if (*got == 0) {
            if (buf.empty()) {
                co_return make_error(ErrorCode::Shutdown, "el par cerró sin enviar datos");
            }
            co_return make_error(ErrorCode::InvalidArgument, "petición HTTP truncada");
        }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        buf.append(reinterpret_cast<const char*>(chunk.data()), *got);
    }

    const std::size_t body_start = header_end + 4;
    const std::size_t content_length =
        parse_content_length(std::string_view{buf}.substr(0, header_end));
    if (content_length > limits.max_body) {
        co_return make_error(ErrorCode::InvalidArgument, "cuerpo HTTP excede el límite");
    }
    const std::size_t total = body_start + content_length;
    while (buf.size() < total) {
        const expected<std::size_t> got =
            co_await sock.async_recv(proactor, MutByteSpan{chunk.data(), chunk.size()});
        if (!got) {
            co_return std::unexpected{got.error()};
        }
        if (*got == 0) {
            co_return make_error(ErrorCode::InvalidArgument, "cuerpo HTTP truncado");
        }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        buf.append(reinterpret_cast<const char*>(chunk.data()), *got);
    }
    co_return parse_request(std::string_view{buf}.substr(0, total), limits);
}

task<void> serve_admin_connection(Proactor& proactor, Socket sock, const AdminRouter& router,
                                  HttpParseLimits limits) {
    const expected<HttpRequest> request = co_await read_http_request(proactor, sock, limits);
    HttpResponse response;
    if (request) {
        response = router.handle(*request);
    } else if (request.error().code() == ErrorCode::Shutdown) {
        co_return;  // cierre limpio sin datos: nada que responder.
    } else {
        response = bad_request();
    }
    response.set_header("Connection", "close");

    const std::string raw = response.serialize();
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    ByteSpan remaining{reinterpret_cast<const std::byte*>(raw.data()), raw.size()};
    while (!remaining.empty()) {
        const expected<std::size_t> sent = co_await sock.async_send(proactor, remaining);
        if (!sent || *sent == 0) {
            break;  // error o par cerrado: abandonamos el envío.
        }
        remaining = remaining.subspan(*sent);
    }
    co_return;
}

}  // namespace nexus
