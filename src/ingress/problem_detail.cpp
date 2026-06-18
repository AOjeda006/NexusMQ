/// @file   ingress/problem_detail.cpp
/// @brief  Implementación de ProblemDetail (RFC 7807) y mapeo ErrorCode→HTTP.
/// @ingroup ingress

#include "ingress/problem_detail.hpp"

#include <string>

#include "ingress/json.hpp"

namespace nexus {

int http_status_for(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::InvalidArgument:
        case ErrorCode::OutOfRange:
            return 400;
        case ErrorCode::NotFound:
            return 404;
        case ErrorCode::Unsupported:
            return 501;
        case ErrorCode::OutOfSpace:
            return 507;
        case ErrorCode::Shutdown:
            return 503;
        case ErrorCode::Corrupt:
        case ErrorCode::IoError:
            return 500;
    }
    return 500;
}

ProblemDetail problem_from_error(const Error& error, std::string instance) {
    const int status = http_status_for(error.code());
    return ProblemDetail{.type = "about:blank",
                         .title = std::string{http_reason(status)},
                         .status = status,
                         .detail = std::string{error.message()},
                         .instance = std::move(instance)};
}

std::string ProblemDetail::to_json() const {
    JsonWriter json;
    json.begin_object();
    json.field("type", type);
    json.field("title", title);
    json.field("status", status);
    if (!detail.empty()) {
        json.field("detail", detail);
    }
    if (!instance.empty()) {
        json.field("instance", instance);
    }
    json.end_object();
    return json.take();
}

HttpResponse ProblemDetail::to_response() const {
    HttpResponse resp;
    resp.status = status;
    resp.reason = std::string{http_reason(status)};
    resp.set_header("Content-Type", "application/problem+json");
    resp.body = to_json();
    return resp;
}

}  // namespace nexus
