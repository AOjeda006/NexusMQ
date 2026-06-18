/// @file   cli/http_admin_client.cpp
/// @brief  Implementación del cliente HTTP/1.1 bloqueante del REST admin.
/// @ingroup cli

#include "cli/http_admin_client.hpp"

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cctype>
#include <cstddef>
#include <string>
#include <utility>

namespace nexus::cli {

namespace {

/// Extrae el estado y el cuerpo de una respuesta HTTP/1.1 cruda.
ClientResponse parse_response(const std::string& raw) {
    ClientResponse response;
    if (const std::size_t space = raw.find(' '); space != std::string::npos) {
        int status = 0;
        std::size_t i = space + 1;
        while (i < raw.size() && (std::isdigit(static_cast<unsigned char>(raw[i])) != 0)) {
            status = (status * 10) + (raw[i] - '0');
            ++i;
        }
        response.status = status;
    }
    if (const std::size_t body = raw.find("\r\n\r\n"); body != std::string::npos) {
        response.body = raw.substr(body + 4);
    }
    return response;
}

}  // namespace

HttpAdminClient::HttpAdminClient(Options options) : options_(std::move(options)) {}

expected<ClientResponse> HttpAdminClient::get(std::string_view path) {
    return request("GET", path, {});
}

expected<ClientResponse> HttpAdminClient::post(std::string_view path, std::string_view body) {
    return request("POST", path, body);
}

expected<ClientResponse> HttpAdminClient::del(std::string_view path) {
    return request("DELETE", path, {});
}

expected<ClientResponse> HttpAdminClient::request(std::string_view method, std::string_view path,
                                                  std::string_view body) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* resolved = nullptr;
    const std::string port_str = std::to_string(options_.port);
    if (::getaddrinfo(options_.host.c_str(), port_str.c_str(), &hints, &resolved) != 0) {
        return make_error(ErrorCode::IoError, "no se pudo resolver el host: " + options_.host);
    }

    int fd = -1;
    for (addrinfo* ai = resolved; ai != nullptr; ai = ai->ai_next) {
        fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
            break;
        }
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(resolved);
    if (fd < 0) {
        return make_error(ErrorCode::IoError,
                          "no se pudo conectar a " + options_.host + ":" + port_str);
    }

    std::string req;
    req += method;
    req += ' ';
    req += path;
    req += " HTTP/1.1\r\nHost: ";
    req += options_.host;
    req += "\r\nConnection: close\r\n";
    if (!options_.bearer_token.empty()) {
        req += "Authorization: Bearer ";
        req += options_.bearer_token;
        req += "\r\n";
    }
    if (!body.empty()) {
        req += "Content-Type: application/json\r\nContent-Length: ";
        req += std::to_string(body.size());
        req += "\r\n";
    }
    req += "\r\n";
    req += body;

    const char* ptr = req.data();
    std::size_t left = req.size();
    while (left > 0) {
        const ssize_t sent = ::send(fd, ptr, left, MSG_NOSIGNAL);
        if (sent <= 0) {
            ::close(fd);
            return make_error(ErrorCode::IoError, "fallo al enviar la petición");
        }
        ptr += sent;
        left -= static_cast<std::size_t>(sent);
    }

    std::string raw;
    std::array<char, 4096> chunk{};
    while (true) {
        const ssize_t got = ::recv(fd, chunk.data(), chunk.size(), 0);
        if (got < 0) {
            ::close(fd);
            return make_error(ErrorCode::IoError, "fallo al leer la respuesta");
        }
        if (got == 0) {
            break;
        }
        raw.append(chunk.data(), static_cast<std::size_t>(got));
    }
    ::close(fd);
    return parse_response(raw);
}

}  // namespace nexus::cli
